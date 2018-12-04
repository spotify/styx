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

import static com.spotify.apollo.Status.BAD_REQUEST;
import static com.spotify.apollo.Status.FORBIDDEN;
import static java.lang.Boolean.TRUE;
import static java.util.stream.Collectors.toList;

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
import com.google.common.collect.ImmutableSet;
import com.spotify.apollo.Response;
import com.spotify.styx.model.WorkflowId;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Verifies that a user is authorized to use a service account in a workflow by verifying that the user has
 * a specific role either on the service account itself or in the project of the service account.
 * <p> This requires the following permissions:<ul>
 * <li> {@code resourcemanager.projects.getIamPolicy}
 * <li> {@code iam.serviceAccounts.getIamPolicy}
 * </ul></p>
 */
@FunctionalInterface
public interface ServiceAccountUsageAuthorizer {

  void authorizeServiceAccountUsage(WorkflowId workflowId, String serviceAccount,
      GoogleIdToken idToken);

  class Impl implements ServiceAccountUsageAuthorizer {

    private static final Logger log = LoggerFactory.getLogger(ServiceAccountUsageAuthorizer.class);

    private static final Pattern SERVICE_ACCOUNT_PATTERN =
        Pattern.compile("^.+\\.gserviceaccount\\.com$");

    private static final Pattern USER_CREATED_SERVICE_ACCOUNT_PATTERN =
        Pattern.compile("^.+@(.+)\\.iam\\.gserviceaccount\\.com$");

    private final Iam iam;
    private final CloudResourceManager crm;
    private final Directory directory;
    private final String serviceAccountUserRole;
    private final AuthorizationPolicy authorizationPolicy;

    Impl(Iam iam, CloudResourceManager crm, Directory directory, String serviceAccountUserRole,
        AuthorizationPolicy authorizationPolicy) {
      this.iam = Objects.requireNonNull(iam, "iam");
      this.crm = Objects.requireNonNull(crm, "crm");
      this.directory = Objects.requireNonNull(directory, "directory");
      this.serviceAccountUserRole = Objects.requireNonNull(serviceAccountUserRole, "serviceAccountUserRole");
      this.authorizationPolicy = Objects.requireNonNull(authorizationPolicy, "authorizationPolicy");
    }

    @Override
    public void authorizeServiceAccountUsage(WorkflowId workflowId, String serviceAccount, GoogleIdToken idToken) {

      final boolean enforce = authorizationPolicy.shouldEnforceAuthorization(workflowId, serviceAccount, idToken);

      final String principalEmail = idToken.getPayload().getEmail();
      final String projectId = serviceAccountProjectId(serviceAccount);

      // Check if the principal has been granted the service account user role in the project of the SA
      final Optional<String> projectPolicyAccess = projectPolicyAccess(projectId, principalEmail);
      if (projectPolicyAccess.isPresent()) {
        final String accessType = projectPolicyAccess.get();
        log.info("[AUDIT] Principal {} has role {} in project {} {}, "
                + "authorizing use of service account {} in workflow {} (Enforcing: {})",
            principalEmail, serviceAccountUserRole, projectId, accessType, serviceAccount, workflowId.toKey(), enforce);
        return;
      }

      // Check if the principal has been granted the service account user role on the SA itself
      final Optional<String> serviceAccountPolicyAccess = serviceAccountPolicyAccess(serviceAccount, principalEmail);
      if (serviceAccountPolicyAccess.isPresent()) {
        final String accessType = serviceAccountPolicyAccess.get();
        log.info("[AUDIT] Principal {} has role {} on service account {} {}, "
                + "authorizing use of service account {} in workflow {} (Enforcing: {})",
            principalEmail, serviceAccountUserRole, serviceAccount, accessType, serviceAccount, workflowId.toKey(),
            enforce);
        return;
      }

      log.info("[AUDIT] Principal {} denied use of service account {} in workflow {} (Enforcing: {})",
          principalEmail, serviceAccount, workflowId.toKey(), enforce);
      if (enforce) {
        throw new ResponseException(Response.forStatus(
            FORBIDDEN.withReasonPhrase("The user " + principalEmail + " must have the role " + serviceAccountUserRole
                + " on the project " + projectId + " or the service account " + serviceAccount +
                ", either through a group membership (recommended) or directly")));
      }
    }

    private String serviceAccountProjectId(String serviceAccount) {
      final Matcher matcher = USER_CREATED_SERVICE_ACCOUNT_PATTERN.matcher(serviceAccount);
      if (!matcher.matches()) {
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
        return TRUE.equals(directory.members().hasMember(group, principalEmail)
            .execute()
            .getIsMember());
      } catch (GoogleJsonResponseException e) {
        if (e.getStatusCode() == 404) {
          return false;
        }
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private Optional<com.google.api.services.cloudresourcemanager.model.Policy> getProjectPolicy(String projectId) {
      try {
        return Optional.of(crm.projects()
            .getIamPolicy(projectId, new GetIamPolicyRequest())
            .execute());
      } catch (GoogleJsonResponseException e) {
        if (e.getStatusCode() == 404) {
          return Optional.empty();
        }
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private Optional<com.google.api.services.iam.v1.model.Policy> getServiceAccountPolicy(String serviceAccount) {
      try {
        return Optional.of(iam.projects().serviceAccounts()
            .getIamPolicy("projects/-/serviceAccounts/" + serviceAccount)
            .execute());
      } catch (GoogleJsonResponseException e) {
        if (e.getStatusCode() == 404) {
          return Optional.empty();
        }
        throw new RuntimeException(e);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    private static String memberEntry(String email) {
      final String type = SERVICE_ACCOUNT_PATTERN.matcher(email).matches() ? "serviceAccount" : "user";
      return type + ":" + email;
    }
  }

  class Nop implements ServiceAccountUsageAuthorizer {

    static final Nop INSTANCE = new Nop();

    @Override
    public void authorizeServiceAccountUsage(WorkflowId workflowId, String serviceAccount, GoogleIdToken idToken) {
    }
  }

  static ServiceAccountUsageAuthorizer create(String serviceAccountUserRole, AuthorizationPolicy authorizationPolicy,
      GoogleCredential credential) {

    final HttpTransport httpTransport;
    try {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException(e);
    }

    final JsonFactory jsonFactory = Utils.getDefaultJsonFactory();

    final String applicationName = "styx-api";

    final CloudResourceManager crm =
        new CloudResourceManager.Builder(httpTransport, jsonFactory, credential)
            .setApplicationName(applicationName)
            .build();

    final Iam iam = new Iam.Builder(
        httpTransport, jsonFactory, credential)
        .setApplicationName(applicationName)
        .build();

    final Directory directory = new Directory.Builder(httpTransport, jsonFactory, credential)
        .setApplicationName(applicationName)
        .build();

    return new Impl(iam, crm, directory, serviceAccountUserRole, authorizationPolicy);
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
