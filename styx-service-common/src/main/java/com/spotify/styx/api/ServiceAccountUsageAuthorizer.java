/*-
 * -\-\-
 * Spotify Styx API Service
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */

package com.spotify.styx.api;

import static com.github.rholder.retry.WaitStrategies.exponentialWait;
import static com.github.rholder.retry.WaitStrategies.randomWait;
import static com.google.api.services.directory.DirectoryScopes.ADMIN_DIRECTORY_GROUP_MEMBER_READONLY;
import static com.spotify.apollo.Status.BAD_REQUEST;
import static com.spotify.apollo.Status.FORBIDDEN;
import static com.spotify.styx.util.ConfigUtil.get;
import static java.lang.Boolean.TRUE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.StopStrategy;
import com.github.rholder.retry.WaitStrategies;
import com.github.rholder.retry.WaitStrategy;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.GetIamPolicyRequest;
import com.google.api.services.cloudresourcemanager.model.GetPolicyOptions;
import com.google.api.services.directory.Directory;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.IamScopes;
import com.google.api.services.iam.v1.model.ServiceAccount;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closer;
import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.styx.model.WorkflowId;
import com.typesafe.config.Config;
import io.norberg.automatter.AutoMatter;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that a user is authorized to use a service account in a workflow by verifying that the user has
 * a specific role either on the service account itself or in the project of the service account.
 * <p> This requires the following permissions:<ul>
 * <li> GCP: {@code resourcemanager.projects.getIamPolicy}
 * <li> GCP: {@code iam.serviceAccounts.getIamPolicy}
 * <li> GSuite: {@code https://www.googleapis.com/auth/admin.directory.group.member.readonly}
 * </ul></p>
 */
public interface ServiceAccountUsageAuthorizer {

  Logger LOG = LoggerFactory.getLogger(ServiceAccountUsageAuthorizer.class);

  String AUTHORIZATION_SERVICE_ACCOUNT_USER_ROLE_CONFIG = "styx.authorization.service-account-user-role";
  String AUTHORIZATION_REQUIRE_ALL_CONFIG = "styx.authorization.require.all";
  String AUTHORIZATION_REQUIRE_WORKFLOWS = "styx.authorization.require.workflows";
  String AUTHORIZATION_GSUITE_USER_CONFIG = "styx.authorization.gsuite-user";
  String AUTHORIZATION_MESSAGE_CONFIG = "styx.authorization.message";
  String AUTHORIZATION_ADMINISTRATORS_CONFIG = "styx.authorization.administrators";
  String AUTHORIZATION_BLACKLIST_CONFIG = "styx.authorization.blacklist";

  /**
   * Authorize service account usage by a principal in a workflow.
   * @throws ResponseException if not authorized.
   */
  void authorizeServiceAccountUsage(WorkflowId workflowId, String serviceAccount, GoogleIdToken idToken);

  /**
   * Check if a principal is authorized to use a service account.
   */
  ServiceAccountUsageAuthorizationResult checkServiceAccountUsageAuthorization(String serviceAccount, String principal);

  class Impl implements ServiceAccountUsageAuthorizer {

    private static final Logger log = LoggerFactory.getLogger(ServiceAccountUsageAuthorizer.class);

    private static final Pattern SERVICE_ACCOUNT_PATTERN =
        Pattern.compile("^.+\\.gserviceaccount\\.com$");

    private static final Pattern USER_CREATED_SERVICE_ACCOUNT_PATTERN =
        Pattern.compile("^.+@(.+)\\.iam\\.gserviceaccount\\.com$");

    private static final WaitStrategy DEFAULT_WAIT_STRATEGY =
        WaitStrategies.join(exponentialWait(), randomWait(1, SECONDS));

    private static final StopStrategy DEFAULT_RETRY_STOP_STRATEGY = StopStrategies.stopAfterDelay(10, SECONDS);

    private static final String CACHE_HIT = "hit";
    private static final String CACHE_MISS = "miss";

    private final Iam iam;
    private final CloudResourceManager crm;
    private final Directory directory;
    private final String serviceAccountUserRole;
    private final AuthorizationPolicy authorizationPolicy;
    private final WaitStrategy waitStrategy;
    private final StopStrategy retryStopStrategy;
    private final String message;
    private final List<String> administrators;
    private final List<String> blacklist;

    /**
     * (principalEmail, serviceAccount) -> ServiceAccountUsageAuthorizationResult
     */
    private final Cache<Tuple2<String, String>, ServiceAccountUsageAuthorizationResult> cache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(60, SECONDS)
            .maximumSize(10_000)
            .build();

    Impl(Iam iam, CloudResourceManager crm, Directory directory, String serviceAccountUserRole,
         AuthorizationPolicy authorizationPolicy, WaitStrategy waitStrategy, StopStrategy retryStopStrategy,
         String message, List<String> administrators, List<String> blacklist) {
      this.iam = Objects.requireNonNull(iam, "iam");
      this.crm = Objects.requireNonNull(crm, "crm");
      this.directory = Objects.requireNonNull(directory, "directory");
      this.serviceAccountUserRole = Objects.requireNonNull(serviceAccountUserRole, "serviceAccountUserRole");
      this.authorizationPolicy = Objects.requireNonNull(authorizationPolicy, "authorizationPolicy");
      this.waitStrategy = Objects.requireNonNull(waitStrategy, "waitStrategy");
      this.retryStopStrategy = Objects.requireNonNull(retryStopStrategy, "retryStopStrategy");
      this.message = Objects.requireNonNull(message, "message");
      this.administrators = Objects.requireNonNull(administrators, "administrators");
      this.blacklist = Objects.requireNonNull(blacklist, "blacklist");
    }

    @Override
    public void authorizeServiceAccountUsage(WorkflowId workflowId, String serviceAccount, GoogleIdToken idToken) {
      final String principalEmail = idToken.getPayload().getEmail();

      final boolean enforce = authorizationPolicy.shouldEnforceAuthorization(workflowId, serviceAccount, idToken);

      // Cached authorization check
      final AtomicBoolean cached = new AtomicBoolean(true);
      final ServiceAccountUsageAuthorizationResult result;
      try {
        result = cache.get(Tuple.of(principalEmail, serviceAccount), () -> {
          cached.set(false);
          try {
            return checkServiceAccountUsageAuthorization(serviceAccount, principalEmail);
          } catch (ResponseException e) {
            return ServiceAccountUsageAuthorizationResult.ofErrorResponse(e.getResponse());
          }
        });
      } catch (Exception e) {
        log.warn("Authorization failure for service account {} used by {} (enforce: {})",
            serviceAccount, principalEmail, enforce, e);
        if (enforce) {
          Throwables.throwIfUnchecked(e);
          throw new RuntimeException(e);
        } else {
          return;
        }
      }

      // Propagate response exception
      result.errorResponse().ifPresent(e -> { throw new ResponseException(e); });

      // Grant access?
      if (result.authorized()) {
        logAuthorization(workflowId, serviceAccount, enforce, result.message().orElse(""), cached.get());
        return;
      }

      // Deny access?
      logDenial(workflowId, serviceAccount, enforce, principalEmail, cached.get());
      if (enforce) {
        throw denialResponseException(result.message().orElse(""));
      }
    }

    @Override
    public ServiceAccountUsageAuthorizationResult checkServiceAccountUsageAuthorization(String serviceAccount,
                                                                                        String principalEmail) {
      final Supplier<String> projectIdSupplier = Suppliers.memoize(() -> serviceAccountProjectId(serviceAccount));

      return checkIsPrincipalBlacklisted(principalEmail)
          .or(() -> checkRoleOrIsPrincipalAdmin(serviceAccount, principalEmail, projectIdSupplier))
          .orElseGet(() -> deny(serviceAccount, principalEmail, projectIdSupplier));
    }

    private Optional<ServiceAccountUsageAuthorizationResult> checkIsPrincipalBlacklisted(String principalEmail) {
      return memberStatus(principalEmail, blacklist)
          .map(status -> ServiceAccountUsageAuthorizationResult.builder()
              .authorized(false)
              .blacklisted(true)
              .message(blacklistedDenialMessage(principalEmail))
              .build());
    }

    private Optional<ServiceAccountUsageAuthorizationResult> checkRoleOrIsPrincipalAdmin(String serviceAccount,
                                                                                         String principalEmail,
                                                                                         Supplier<String> projectIdSupplier) {

      Optional<ServiceAccountUsageAuthorizationResult> result = Optional.empty();
      RuntimeException checkRoleException = null;

      try {
        result = checkRole(serviceAccount, principalEmail, projectIdSupplier);
      } catch (RuntimeException e) {
        checkRoleException = e;
      }

      if (result.isEmpty()) {
        result = checkIsPrincipalAdmin(principalEmail);
      }

      if (result.isEmpty() && checkRoleException != null) {
        throw checkRoleException;
      }
      return result;
    }

    private Optional<ServiceAccountUsageAuthorizationResult> checkIsPrincipalAdmin(
        String principalEmail) {
      return memberStatus(principalEmail, administrators)
          .map(status -> ServiceAccountUsageAuthorizationResult.builder()
              .authorized(true)
              .message(String.format("Principal %s is an admin %s", principalEmail, status))
              .build());
    }

    private Optional<ServiceAccountUsageAuthorizationResult> checkRole(String serviceAccount,
                                                                       String principalEmail,
                                                                       Supplier<String> projectIdSupplier) {

      return firstPresent(

          // Check if the principal has been granted the service account user role in the project of the SA
          () -> projectPolicyAccess(projectIdSupplier.get(), principalEmail)
              .map(type -> String.format("Principal %s has role %s in project %s %s",
                  principalEmail, serviceAccountUserRole, projectIdSupplier.get(), type)),

          // Check if the principal has been granted the service account user role on the SA itself
          () -> serviceAccountPolicyAccess(serviceAccount, principalEmail)
              .map(type -> String.format("Principal %s has role %s on service account %s %s",
                  principalEmail, serviceAccountUserRole, serviceAccount, type)))
          .map(accessMessage ->
              ServiceAccountUsageAuthorizationResult.builder()
                  .serviceAccountProjectId(projectIdSupplier.get())
                  .authorized(true)
                  .message(accessMessage)
                  .build()
          );
    }

    private ServiceAccountUsageAuthorizationResult deny(String serviceAccount,
                                                        String principalEmail,
                                                        Supplier<String> projectIdSupplier) {
      return ServiceAccountUsageAuthorizationResult.builder()
          .serviceAccountProjectId(projectIdSupplier.get())
          .authorized(false)
          .message(denialMessage(serviceAccount, principalEmail, projectIdSupplier.get()))
          .build();
    }

    private ResponseException denialResponseException(String message) {
      return new ResponseException(Response.forStatus(FORBIDDEN.withReasonPhrase(message)));
    }

    private String blacklistedDenialMessage(String principalEmail) {
      return "The principal " + principalEmail + " is blacklisted. Please use another one. " + message;
    }

    private String denialMessage(String serviceAccount, String principalEmail, String projectId) {
      return "The principal " + principalEmail + " must have the role " + serviceAccountUserRole
             + " in the project " + projectId + " or on the "
             + "service account " + serviceAccount
             + ", either through a group membership or directly. " + message;
    }

    private void logDenial(WorkflowId workflowId, String serviceAccount, boolean enforce, String principalEmail,
        boolean cached) {
      log.info("[AUDIT] Principal {} denied use of service account {} in workflow {} (Enforcing: {}, Cache: {})",
          principalEmail, serviceAccount, workflowId.toKey(), enforce, cached ? CACHE_HIT : CACHE_MISS);
    }

    private void logAuthorization(WorkflowId workflowId, String serviceAccount, boolean enforce, String accessMessage,
        boolean cached) {
      log.info("[AUDIT] {}, authorizing use of service account {} in workflow {} (Enforcing: {}, Cache: {})",
          accessMessage, serviceAccount, workflowId.toKey(), enforce, cached ? CACHE_HIT : CACHE_MISS);
    }

    private String serviceAccountProjectId(String serviceAccount) {
      final Matcher matcher = USER_CREATED_SERVICE_ACCOUNT_PATTERN.matcher(serviceAccount);
      if (matcher.matches()) {
        return matcher.group(1);
      }
      return lookupServiceAccountProjectId(serviceAccount);
    }

    private String lookupServiceAccountProjectId(String email) {
      try {
        final ServiceAccount serviceAccount = retry(() ->
            iam.projects().serviceAccounts().get("projects/-/serviceAccounts/" + email).execute());
        return serviceAccount.getProjectId();
      } catch (ExecutionException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof GoogleJsonResponseException
            && ((GoogleJsonResponseException) cause).getStatusCode() == 404) {
          log.debug("Service account {} doesn't exist", email, e);
          throw new ResponseException(Response.forStatus(
              BAD_REQUEST.withReasonPhrase("Service account does not exist: " + email)));
        }
        throw new RuntimeException(e);
      } catch (RetryException e) {
        throw new RuntimeException(e);
      }
    }

    private Optional<String> projectPolicyAccess(String projectId, String principalEmail) {
      final com.google.api.services.cloudresourcemanager.model.Policy policy = getProjectPolicy(projectId)
          .orElseThrow(() -> new ResponseException(Response.forStatus(
              BAD_REQUEST.withReasonPhrase("Project does not exist: " + projectId))));

      final List<String> members = emptyListIfNull(policy.getBindings()).stream()
          .filter(binding -> serviceAccountUserRole.equals(binding.getRole()))
          .flatMap(binding -> emptyListIfNull(binding.getMembers()).stream())
          .collect(toList());

      return memberStatus(principalEmail, members);
    }

    private Optional<String> serviceAccountPolicyAccess(String serviceAccount, String principalEmail) {
      final com.google.api.services.iam.v1.model.Policy policy = getServiceAccountPolicy(serviceAccount)
          .orElseThrow(() -> new ResponseException(Response.forStatus(
              BAD_REQUEST.withReasonPhrase("Service account does not exist: " + serviceAccount))));

      final List<String> members = emptyListIfNull(policy.getBindings()).stream()
          .filter(binding -> serviceAccountUserRole.equals(binding.getRole()))
          .flatMap(binding -> emptyListIfNull(binding.getMembers()).stream())
          .collect(toList());
      return memberStatus(principalEmail, members);
    }

    private Optional<String> memberStatus(String principalEmail, List<String> members) {

      // Direct member?
      if (members.contains(memberEntry(principalEmail))) {
        return Optional.of("directly");
      }

      // Group member?
      final String groupPrefix = "group:";
      return members.stream()
          .filter(s -> s.startsWith(groupPrefix))
          .map(s -> s.substring(groupPrefix.length()))
          .filter(group -> isMemberOfGroup(principalEmail, group))
          .findFirst()
          .map(group -> "via group " + group);
    }

    private boolean isMemberOfGroup(String principalEmail, String group) {
      try {
        final Boolean isMember = retry(() -> directory.members().hasMember(group, principalEmail)
            .execute()
            .getIsMember());
        return TRUE.equals(isMember);
      } catch (ExecutionException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof GoogleJsonResponseException) {
          var statusCode = ((GoogleJsonResponseException) cause).getStatusCode();
          // hasMember API returns 404 if the group does not exist, while returning 400 if the principal
          // email does not exist or does not have the same domain as the group if it is enforced
          if (statusCode == 400) {
            log.info("Principal {} does not exist or belongs to different domain than group {}",
                principalEmail, group);
            return false;
          } else if (statusCode == 404) {
            log.info("Group {} does not exist", group);
            return false;
          }
        }
        throw new RuntimeException(e);
      } catch (RetryException e) {
        throw new RuntimeException(e);
      }
    }

    private Optional<com.google.api.services.cloudresourcemanager.model.Policy> getProjectPolicy(String projectId) {
      try {
        return retry(() -> Optional.of(crm.projects()
            .getIamPolicy(projectId,
                new GetIamPolicyRequest().setOptions(new GetPolicyOptions().setRequestedPolicyVersion(3)))
            .execute()));
      } catch (ExecutionException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof GoogleJsonResponseException
            && ((GoogleJsonResponseException) cause).getStatusCode() == 404) {
          log.info("Project {} does not exist", projectId, cause);
          return Optional.empty();
        }
        throw new RuntimeException(e);
      } catch (RetryException e) {
        throw new RuntimeException(e);
      }
    }

    private Optional<com.google.api.services.iam.v1.model.Policy> getServiceAccountPolicy(String serviceAccount) {
      try {
        return retry(() -> Optional.of(iam.projects().serviceAccounts()
            .getIamPolicy("projects/-/serviceAccounts/" + serviceAccount)
            .execute()));
      } catch (ExecutionException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof GoogleJsonResponseException
            && ((GoogleJsonResponseException) cause).getStatusCode() == 404) {
          log.info("Service account {} does not exist", serviceAccount, cause);
          return Optional.empty();
        }
        throw new RuntimeException(e);
      } catch (RetryException e) {
        throw new RuntimeException(e);
      }
    }

    private <T> T retry(Callable<T> f) throws ExecutionException, RetryException {
      final Retryer<T> retryer = RetryerBuilder.<T>newBuilder()
          .retryIfException(Impl::isRetryableException)
          .withWaitStrategy(waitStrategy)
          .withStopStrategy(retryStopStrategy)
          .withRetryListener(Impl::onRequestAttempt)
          .build();

      return retryer.call(f);
    }

    private static <T> void onRequestAttempt(Attempt<T> attempt) {
      if (attempt.hasException() && isRetryableException(attempt.getExceptionCause())) {
        log.warn("Failed request attempt {}", attempt.getAttemptNumber(), attempt.getExceptionCause());
      }
    }

    private static boolean isRetryableException(Throwable t) {
      if (t instanceof IOException) {
        if (t instanceof GoogleJsonResponseException) {
          return ((GoogleJsonResponseException) t).getStatusCode() / 100 == 5;
        }
        return true;
      }
      return false;
    }

    private static String memberEntry(String email) {
      final String type = SERVICE_ACCOUNT_PATTERN.matcher(email).matches() ? "serviceAccount" : "user";
      return type + ":" + email;
    }

    @SafeVarargs
    private static <T> Optional<T> firstPresent(Supplier<Optional<T>>... optionals) {
      return Stream.of(optionals)
          .map(Supplier::get)
          .filter(Optional::isPresent)
          .findFirst()
          .orElse(Optional.empty());
    }

    private static <T> List<T> emptyListIfNull(List<T> list) {
      return Objects.requireNonNullElse(list, List.of());
    }
  }

  class Nop implements ServiceAccountUsageAuthorizer {

    static final Nop INSTANCE = new Nop();

    @Override
    public void authorizeServiceAccountUsage(WorkflowId workflowId, String serviceAccount, GoogleIdToken idToken) {
      // nop
    }

    @Override
    public ServiceAccountUsageAuthorizationResult checkServiceAccountUsageAuthorization(
        String serviceAccount, String principal) {
      return ServiceAccountUsageAuthorizationResult.builder().authorized(true).build();
    }
  }

  static ServiceAccountUsageAuthorizer create(Environment environment, String serviceName,
                                              GoogleCredentials credential) {
    final Config config = environment.config();
    return get(config, config::getString, AUTHORIZATION_SERVICE_ACCOUNT_USER_ROLE_CONFIG)
        .map(role -> {
          final AuthorizationPolicy authorizationPolicy = AuthorizationPolicy.fromConfig(config);
          final String gsuiteUserEmail = config.getString(AUTHORIZATION_GSUITE_USER_CONFIG);
          final String message = get(config, config::getString, AUTHORIZATION_MESSAGE_CONFIG).orElse("");
          final List<String> administrators = get(config, config::getStringList, AUTHORIZATION_ADMINISTRATORS_CONFIG)
              .orElse(Collections.emptyList());
          final List<String> blacklist = get(config, config::getStringList, AUTHORIZATION_BLACKLIST_CONFIG)
              .orElse(Collections.emptyList());
          return ServiceAccountUsageAuthorizer.create(environment.closer(),
              role, authorizationPolicy, credential, gsuiteUserEmail, serviceName, message, administrators, blacklist);
        })
        .orElseGet(() -> {
          LOG.warn("{} not configured, fallback to nop ServiceAccountUsageAuthorizer",
              AUTHORIZATION_SERVICE_ACCOUNT_USER_ROLE_CONFIG);
          return ServiceAccountUsageAuthorizer.nop();
        });
  }

  static ServiceAccountUsageAuthorizer create(Environment environment, String serviceName) {
    return create(environment, serviceName, defaultCredentials());
  }

  static ServiceAccountUsageAuthorizer create(Closer closer,
                                              String serviceAccountUserRole,
                                              AuthorizationPolicy authorizationPolicy,
                                              GoogleCredentials credentials,
                                              String gsuiteUserEmail,
                                              String serviceName,
                                              String message,
                                              List<String> administrators,
                                              List<String> blacklist) {

    final HttpTransport httpTransport;
    try {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException(e);
    }

    final JsonFactory jsonFactory = Utils.getDefaultJsonFactory();

    final CloudResourceManager crm = new CloudResourceManager.Builder(
        httpTransport, jsonFactory, new HttpCredentialsAdapter(credentials.createScoped(IamScopes.all())))
        .setApplicationName(serviceName)
        .build();

    final Iam iam = new Iam.Builder(
        httpTransport, jsonFactory, new HttpCredentialsAdapter(credentials.createScoped(IamScopes.all())))
        .setApplicationName(serviceName)
        .build();

    final IamCredentialsClient iamCredentialsClient;
    try {
      iamCredentialsClient = IamCredentialsClient.create();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    closer.register(iamCredentialsClient::close);

    final GoogleCredential directoryCredential = new ManagedServiceAccountKeyCredential.Builder(iamCredentialsClient)
        .setServiceAccountId(ServiceAccounts.serviceAccountEmail(credentials))
        .setServiceAccountUser(gsuiteUserEmail)
        .setServiceAccountScopes(Set.of(ADMIN_DIRECTORY_GROUP_MEMBER_READONLY))
        .build();

    final Directory directory = new Directory.Builder(httpTransport, jsonFactory, directoryCredential)
        .setApplicationName(serviceName)
        .build();

    return new Impl(iam, crm, directory, serviceAccountUserRole, authorizationPolicy,
        Impl.DEFAULT_WAIT_STRATEGY, Impl.DEFAULT_RETRY_STOP_STRATEGY, message, administrators, blacklist);
  }

  static GoogleCredentials defaultCredentials() {
    return Try.of(GoogleCredentials::getApplicationDefault).get();
  }

  static ServiceAccountUsageAuthorizer nop() {
    return Nop.INSTANCE;
  }

  /**
   * A policy that decides whether to enforce an authorization decision.
   */
  interface AuthorizationPolicy {

    /**
     * Returns true if authorization should be enforced, false otherwise.
     */
    boolean shouldEnforceAuthorization(WorkflowId workflowId, String serviceAccount, GoogleIdToken idToken);

    static AuthorizationPolicy fromConfig(Config config) {
      final AuthorizationPolicy authorizationPolicy;
      if (get(config, config::getBoolean, AUTHORIZATION_REQUIRE_ALL_CONFIG).orElse(false)) {
        authorizationPolicy = new AllAuthorizationPolicy();
      } else {
        final List<WorkflowId> ids = get(config, config::getStringList, AUTHORIZATION_REQUIRE_WORKFLOWS)
            .orElse(Collections.emptyList())
            .stream()
            .map(WorkflowId::parseKey)
            .collect(Collectors.toList());
        authorizationPolicy = new WhitelistAuthorizationPolicy(ids);
      }
      return authorizationPolicy;
    }
  }

  /**
   * A policy that always enforces authorization.
   */
  class AllAuthorizationPolicy implements AuthorizationPolicy {

    @Override
    public boolean shouldEnforceAuthorization(WorkflowId workflowId, String serviceAccount, GoogleIdToken idToken) {
      return true;
    }
  }

  /**
   * A policy that only enforces authorization for a set of workflows.
   */
  class WhitelistAuthorizationPolicy implements AuthorizationPolicy {

    private final Set<WorkflowId> whitelist;

    WhitelistAuthorizationPolicy(Iterable<WorkflowId> whitelist) {
      this.whitelist = ImmutableSet.copyOf(whitelist);
    }

    @Override
    public boolean shouldEnforceAuthorization(WorkflowId workflowId, String serviceAccount, GoogleIdToken idToken) {
      return whitelist.contains(workflowId);
    }
  }

  @AutoMatter
  interface ServiceAccountUsageAuthorizationResult {

    /**
     * A response describing any error encountered during the authorization check.
     */
    Optional<Response<?>> errorResponse();

    /**
     * Successfully authorized?
     */
    boolean authorized();

    /**
     * Blacklisted?
     */
    boolean blacklisted();

    /**
     * A message describing the authorization or denial reason.
     */
    Optional<String> message();

    /**
     * The project ID of the service account, if successfully resolved.
     */
    Optional<String> serviceAccountProjectId();

    static ServiceAccountUsageAuthorizationResultBuilder builder() {
      return new ServiceAccountUsageAuthorizationResultBuilder();
    }

    static ServiceAccountUsageAuthorizationResult ofErrorResponse(Response<?> response) {
      return builder().errorResponse(response).build();
    }
  }

  interface Factory extends BiFunction<Environment, String, ServiceAccountUsageAuthorizer> {

    Factory DEFAULT = ServiceAccountUsageAuthorizer::create;
  }
}
