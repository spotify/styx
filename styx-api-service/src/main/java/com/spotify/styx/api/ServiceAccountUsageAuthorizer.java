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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.GetIamPolicyRequest;
import com.google.api.services.cloudresourcemanager.model.Policy;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.IamScopes;
import com.spotify.apollo.Response;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Objects;
import java.util.Optional;
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

  void authorizeServiceAccountUsage(String serviceAccount, GoogleIdToken idToken);

  class Impl implements ServiceAccountUsageAuthorizer {

    private static final Logger log = LoggerFactory.getLogger(ServiceAccountUsageAuthorizer.class);

    private static final Pattern SERVICE_ACCOUNT_PATTERN =
        Pattern.compile("^.+\\.gserviceaccount\\.com$");

    private static final Pattern USER_CREATED_SERVICE_ACCOUNT_PATTERN =
        Pattern.compile("^.+@(.+)\\.iam\\.gserviceaccount\\.com$");

    private final Iam iam;
    private final CloudResourceManager crm;
    private final String serviceAccountUserRole;

    Impl(Iam iam, CloudResourceManager crm, final String serviceAccountUserRole) {
      this.iam = Objects.requireNonNull(iam, "iam");
      this.crm = Objects.requireNonNull(crm, "crm");
      this.serviceAccountUserRole = Objects.requireNonNull(serviceAccountUserRole, "serviceAccountUserRole");
    }

    @Override
    public void authorizeServiceAccountUsage(String serviceAccount, GoogleIdToken idToken) {

      final String principalEmail = idToken.getPayload().getEmail();
      final String projectId = serviceAccountProjectId(serviceAccount);

      // Check if the principal has been granted the service account user role in the project of the SA
      if (projectPolicyAccessGranted(projectId, principalEmail)) {
        log.info("[AUDIT] Principal {} has role {} in project {}, authorizing use of service account {}",
            principalEmail, serviceAccountUserRole, projectId, serviceAccount);
        return;
      }

      // Check if the principal has been granted the service account user role on the SA itself
      if (serviceAccountPolicyAccessGranted(serviceAccount, principalEmail)) {
        log.info("[AUDIT] Principal {} has role {} on service account {}, authorizing use of service account {}",
            principalEmail, serviceAccountUserRole, serviceAccount, serviceAccount);
        return;
      }

      log.info("[AUDIT] Principal {} denied use of service account {}", principalEmail, serviceAccount);
      throw new ResponseException(Response.forStatus(
          FORBIDDEN.withReasonPhrase("Missing role " + serviceAccountUserRole
              + " on either the project " + projectId + " or the service account " + serviceAccount)));
    }

    private String serviceAccountProjectId(String serviceAccount) {
      final Matcher matcher = USER_CREATED_SERVICE_ACCOUNT_PATTERN.matcher(serviceAccount);
      if (!matcher.matches()) {
        throw new ResponseException(Response.forStatus(
            BAD_REQUEST.withReasonPhrase("Not a user created service account: " + serviceAccount)));
      }
      return matcher.group(1);
    }

    private boolean projectPolicyAccessGranted(String projectId, String principalEmail) {
      final com.google.api.services.cloudresourcemanager.model.Policy policy = getProjectPolicy(projectId)
          .orElseThrow(() -> new ResponseException(Response.forStatus(
              BAD_REQUEST.withReasonPhrase("Project does not exist: " + projectId))));

      return policy.getBindings().stream()
          .filter(binding -> serviceAccountUserRole.equals(binding.getRole()))
          .flatMap(binding -> binding.getMembers().stream())
          .anyMatch(memberEntry(principalEmail)::equals);
    }

    private boolean serviceAccountPolicyAccessGranted(String serviceAccount, String principalEmail) {
      final com.google.api.services.iam.v1.model.Policy policy = getServiceAccountPolicy(serviceAccount)
          .orElseThrow(() -> new ResponseException(Response.forStatus(
              BAD_REQUEST.withReasonPhrase("Service account does not exist: " + serviceAccount))));

      return policy.getBindings().stream()
          .filter(binding -> serviceAccountUserRole.equals(binding.getRole()))
          .flatMap(binding -> binding.getMembers().stream())
          .anyMatch(memberEntry(principalEmail)::equals);
    }

    private Optional<Policy> getProjectPolicy(String projectId) {
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

  static ServiceAccountUsageAuthorizer create(String serviceAccountUserRole) {
    return create(serviceAccountUserRole, defaultCredential());
  }

  static ServiceAccountUsageAuthorizer create(String serviceAccountUserRole, GoogleCredential credential) {

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

    return new Impl(iam, crm, serviceAccountUserRole);
  }

  static ServiceAccountUsageAuthorizer nop() {
    return (sa, id) -> { };
  }

  static GoogleCredential defaultCredential() {
    final GoogleCredential credential;
    try {
      credential = GoogleCredential.getApplicationDefault()
          .createScoped(IamScopes.all());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return credential;
  }


}
