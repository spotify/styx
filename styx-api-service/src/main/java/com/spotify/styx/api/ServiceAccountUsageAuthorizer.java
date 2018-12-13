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
import static com.google.api.services.admin.directory.DirectoryScopes.ADMIN_DIRECTORY_GROUP_MEMBER_READONLY;
import static com.spotify.apollo.Status.BAD_REQUEST;
import static com.spotify.apollo.Status.FORBIDDEN;
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
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.admin.directory.Directory;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.GetIamPolicyRequest;
import com.google.api.services.iam.v1.Iam;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.spotify.apollo.Response;
import com.spotify.styx.model.WorkflowId;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.control.Either;
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
@FunctionalInterface
public interface ServiceAccountUsageAuthorizer {

  ServiceAccountUsageAuthorizer NOP = (x, y, z) -> {
    // nop
  };

  void authorizeServiceAccountUsage(WorkflowId workflowId, String serviceAccount,
      GoogleIdToken idToken);

  class Impl implements ServiceAccountUsageAuthorizer {

    private static final Logger log = LoggerFactory.getLogger(ServiceAccountUsageAuthorizer.class);

    private static final Pattern SERVICE_ACCOUNT_PATTERN =
        Pattern.compile("^.+\\.gserviceaccount\\.com$");

    private static final Pattern USER_CREATED_SERVICE_ACCOUNT_PATTERN =
        Pattern.compile("^.+@(.+)\\.iam\\.gserviceaccount\\.com$");

    private static final StopStrategy DEFAULT_RETRY_STOP_STRATEGY = StopStrategies.stopAfterDelay(10, SECONDS);

    private static final String CACHE_HIT = "hit";
    private static final String CACHE_MISS = "miss";

    private final Iam iam;
    private final CloudResourceManager crm;
    private final Directory directory;
    private final String serviceAccountUserRole;
    private final AuthorizationPolicy authorizationPolicy;
    private final StopStrategy retryStopStrategy;

    /**
     * (principalEmail, serviceAccount) -> Either[ResponseException, [Optional[accessMessage]]
     */
    private final Cache<Tuple2<String, String>, Either<Response<?>, Optional<String>>> cache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(60, SECONDS)
            .maximumSize(10_000)
            .build();

    Impl(Iam iam, CloudResourceManager crm, Directory directory, String serviceAccountUserRole,
        AuthorizationPolicy authorizationPolicy, StopStrategy retryStopStrategy) {
      this.iam = Objects.requireNonNull(iam, "iam");
      this.crm = Objects.requireNonNull(crm, "crm");
      this.directory = Objects.requireNonNull(directory, "directory");
      this.serviceAccountUserRole = Objects.requireNonNull(serviceAccountUserRole, "serviceAccountUserRole");
      this.authorizationPolicy = Objects.requireNonNull(authorizationPolicy, "authorizationPolicy");
      this.retryStopStrategy = Objects.requireNonNull(retryStopStrategy, "retryStopStrategy");
    }

    @Override
    public void authorizeServiceAccountUsage(WorkflowId workflowId, String serviceAccount, GoogleIdToken idToken) {
      final String principalEmail = idToken.getPayload().getEmail();
      final String projectId = serviceAccountProjectId(workflowId, serviceAccount);

      final AtomicBoolean cached = new AtomicBoolean(true);

      // Cached access check
      final Either<Response<?>, Optional<String>> result = get(cache, Tuple.of(principalEmail, serviceAccount), () -> {
        cached.set(false);

        try {
          return Either.right(firstPresent(
              // Check if the principal has been granted the service account user role in the project of the SA
              () -> projectPolicyAccess(projectId, principalEmail)
                  .map(type -> String.format("Principal %s has role %s on service account %s %s",
                      principalEmail, serviceAccountUserRole, serviceAccount, type)),

              // Check if the principal has been granted the service account user role on the SA itself
              () -> serviceAccountPolicyAccess(serviceAccount, principalEmail)
                  .map(type -> String.format("Principal %s has role %s on service account %s %s",
                      principalEmail, serviceAccountUserRole, serviceAccount, type))));
        } catch (ResponseException e) {
          return Either.left(e.getResponse());
        }
      });

      // Propagate response exception
      if (result.isLeft()) {
        throw new ResponseException(result.left().get());
      }

      final boolean enforce = authorizationPolicy.shouldEnforceAuthorization(workflowId, serviceAccount, idToken);
      final Optional<String> accessMessage = result.right().get();

      // Grant access?
      if (accessMessage.isPresent()) {
        logAuthorization(workflowId, serviceAccount, enforce, accessMessage.get(), cached.get());
        return;
      }

      // Deny access?
      logDenial(workflowId, serviceAccount, enforce, principalEmail, cached.get());
      if (enforce) {
        throw denialResponseException(serviceAccount, principalEmail, projectId);
      }
    }

    private static <K, V> V get(Cache<K, V> cache, K key, Callable<V> loader) {
      try {
        return cache.get(key, loader);
      } catch (ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    private ResponseException denialResponseException(String serviceAccount, String principalEmail, String projectId) {
      return new ResponseException(Response.forStatus(
          FORBIDDEN.withReasonPhrase("The user " + principalEmail + " must have the role " + serviceAccountUserRole
              + " on the project " + projectId + " or the service account " + serviceAccount +
              ", either through a group membership (recommended) or directly")));
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

    private String serviceAccountProjectId(WorkflowId workflowId, String serviceAccount) {
      final Matcher matcher = USER_CREATED_SERVICE_ACCOUNT_PATTERN.matcher(serviceAccount);
      if (!matcher.matches()) {
        log.info("Attempted to use non user created service account {} in workflow {}",
            serviceAccount, workflowId.toKey());
        throw new ResponseException(Response.forStatus(
            BAD_REQUEST.withReasonPhrase("Not a user created service account: " + serviceAccount)));
      }
      return matcher.group(1);
    }

    private Optional<String> projectPolicyAccess(String projectId, String principalEmail) {
      final com.google.api.services.cloudresourcemanager.model.Policy policy = getProjectPolicy(projectId)
          .orElseThrow(() -> new ResponseException(Response.forStatus(
              BAD_REQUEST.withReasonPhrase("Project does not exist: " + projectId))));

      final List<String> members = policy.getBindings().stream()
          .filter(binding -> serviceAccountUserRole.equals(binding.getRole()))
          .flatMap(binding -> binding.getMembers().stream())
          .collect(toList());

      return memberStatus(principalEmail, members);
    }

    private Optional<String> serviceAccountPolicyAccess(String serviceAccount, String principalEmail) {
      final com.google.api.services.iam.v1.model.Policy policy = getServiceAccountPolicy(serviceAccount)
          .orElseThrow(() -> new ResponseException(Response.forStatus(
              BAD_REQUEST.withReasonPhrase("Service account does not exist: " + serviceAccount))));

      final List<String> members = policy.getBindings().stream()
          .filter(binding -> serviceAccountUserRole.equals(binding.getRole()))
          .flatMap(binding -> binding.getMembers().stream())
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
        if (cause instanceof GoogleJsonResponseException
            && ((GoogleJsonResponseException) cause).getStatusCode() == 404) {
          return false;
        }
        throw new RuntimeException(e);
      } catch (RetryException e) {
        throw new RuntimeException(e);
      }
    }

    private Optional<com.google.api.services.cloudresourcemanager.model.Policy> getProjectPolicy(String projectId) {
      try {
        return retry(() -> Optional.of(crm.projects()
            .getIamPolicy(projectId, new GetIamPolicyRequest())
            .execute()));
      } catch (ExecutionException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof GoogleJsonResponseException
            && ((GoogleJsonResponseException) cause).getStatusCode() == 404) {
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
          .withWaitStrategy(WaitStrategies.join(exponentialWait(), randomWait(1, SECONDS)))
          .withStopStrategy(retryStopStrategy)
          .withRetryListener(Impl::onRequestAttempt)
          .build();

      return retryer.call(f);
    }

    private static <T> void onRequestAttempt(Attempt<T> attempt) {
      if (attempt.hasException()) {
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
  }

  static ServiceAccountUsageAuthorizer create(String serviceAccountUserRole,
                                              AuthorizationPolicy authorizationPolicy,
                                              GoogleCredential credential,
                                              String gsuiteUserEmail,
                                              String serviceName) {

    final HttpTransport httpTransport;
    try {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException(e);
    }

    final JsonFactory jsonFactory = Utils.getDefaultJsonFactory();

    final CloudResourceManager crm =
        new CloudResourceManager.Builder(httpTransport, jsonFactory, credential)
            .setApplicationName(serviceName)
            .build();

    final Iam iam = new Iam.Builder(
        httpTransport, jsonFactory, credential)
        .setApplicationName(serviceName)
        .build();

    if (credential.getServiceAccountId() == null) {
      throw new AssertionError("Credential must be a service account");
    }

    final GoogleCredential directoryCredential = new GoogleCredential.Builder()
        .setTransport(httpTransport)
        .setJsonFactory(jsonFactory)
        .setServiceAccountId(credential.getServiceAccountId())
        .setServiceAccountScopes(ImmutableSet.of(ADMIN_DIRECTORY_GROUP_MEMBER_READONLY))
        .setServiceAccountUser(gsuiteUserEmail)
        .setServiceAccountPrivateKey(credential.getServiceAccountPrivateKey())
        .build();

    final Directory directory = new Directory.Builder(httpTransport, jsonFactory, directoryCredential)
        .setApplicationName(serviceName)
        .build();

    return new Impl(iam, crm, directory, serviceAccountUserRole, authorizationPolicy, Impl.DEFAULT_RETRY_STOP_STRATEGY);
  }

  /**
   * A policy that decides whether to enforce an authorization decision.
   */
  interface AuthorizationPolicy {

    /**
     * Returns true if authorization should be enforced, false otherwise.
     */
    boolean shouldEnforceAuthorization(WorkflowId workflowId, String serviceAccount, GoogleIdToken idToken);
  }

  /**
   * A policy that does not enforce any authorization.
   */
  class NoAuthorizationPolicy implements AuthorizationPolicy {

    @Override
    public boolean shouldEnforceAuthorization(WorkflowId workflowId, String serviceAccount, GoogleIdToken idToken) {
      return false;
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

    public WhitelistAuthorizationPolicy(Iterable<WorkflowId> whitelist) {
      this.whitelist = ImmutableSet.copyOf(whitelist);
    }

    @Override
    public boolean shouldEnforceAuthorization(WorkflowId workflowId, String serviceAccount, GoogleIdToken idToken) {
      return whitelist.contains(workflowId);
    }
  }
}
