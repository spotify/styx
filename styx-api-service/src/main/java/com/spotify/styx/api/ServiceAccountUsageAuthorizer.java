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

public class ServiceAccountUsageAuthorizer {

  private static final Logger log = LoggerFactory.getLogger(ServiceAccountUsageAuthorizer.class);

  // TODO: make configurable
  static final String SERVICE_ACCOUNT_USER_ROLE = "organizations/3141592/roles/StyxWorkflowServiceAccountUser";

  private static final Pattern SERVICE_ACCOUNT_PATTERN =
      Pattern.compile("^.+\\.gserviceaccount\\.com$");

  private static final Pattern USER_CREATED_SERVICE_ACCOUNT_PATTERN =
      Pattern.compile("^.+@(.+)\\.iam\\.gserviceaccount\\.com$");

  private final Iam iam;
  private final CloudResourceManager crm;

  ServiceAccountUsageAuthorizer(Iam iam, CloudResourceManager crm) {
    this.iam = Objects.requireNonNull(iam, "iam");
    this.crm = Objects.requireNonNull(crm, "crm");
  }

  <T> Optional<Response<T>> authorizeServiceAccountUsage(String serviceAccount, GoogleIdToken idToken) {

    final String principalEmail = idToken.getPayload().getEmail();
    final String principalType = SERVICE_ACCOUNT_PATTERN.matcher(principalEmail).matches() ? "serviceAccount" : "user";
    final String principalMemberEntry = principalType + ":" + principalEmail;

    final Matcher matcher = USER_CREATED_SERVICE_ACCOUNT_PATTERN.matcher(serviceAccount);
    if (!matcher.matches()) {
      return Optional.of(Response.forStatus(
          BAD_REQUEST.withReasonPhrase("Not a user created service account: " + serviceAccount)));
    }

    final String projectId = matcher.group(1);
    final com.google.api.services.cloudresourcemanager.model.Policy projectPolicy;
    try {
      projectPolicy = crm.projects()
          .getIamPolicy(projectId, new GetIamPolicyRequest()).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == 404) {
        return Optional.of(Response.forStatus(
            BAD_REQUEST.withReasonPhrase("Project does not exist: " + projectId)));
      }
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Check if the principal has been granted the service account user role in the project of the SA
    for (com.google.api.services.cloudresourcemanager.model.Binding binding : projectPolicy.getBindings()) {
      if (SERVICE_ACCOUNT_USER_ROLE.equals(binding.getRole())
          && binding.getMembers().stream().anyMatch(principalMemberEntry::equals)) {
        log.info("[AUDIT] Principal {} has role {} in project {}, authorizing use of service account {}",
            principalEmail, SERVICE_ACCOUNT_USER_ROLE, projectId, serviceAccount);
        return Optional.empty();
      }
    }

    // Check if the principal has been granted the service account user role on the SA itself
    final com.google.api.services.iam.v1.model.Policy saPolicy;
    try {
      saPolicy = iam.projects().serviceAccounts().getIamPolicy("projects/-/serviceAccounts/" + serviceAccount)
          .execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == 404) {
        return Optional.of(Response.forStatus(
            BAD_REQUEST.withReasonPhrase("Service account does not exist: " + serviceAccount)));
      }
      throw new RuntimeException(e);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    for (com.google.api.services.iam.v1.model.Binding binding : saPolicy.getBindings()) {
      if (SERVICE_ACCOUNT_USER_ROLE.equals(binding.getRole())
          && binding.getMembers().stream().anyMatch(principalMemberEntry::equals)) {
        log.info("[AUDIT] Principal {} has role {} on service account {}, authorizing use of service account {}",
            principalEmail, SERVICE_ACCOUNT_USER_ROLE, serviceAccount, serviceAccount);
        return Optional.empty();
      }
    }

    log.info("[AUDIT] Principal {} denied use of service account {}", principalEmail, serviceAccount);
    return Optional.of(Response.forStatus(FORBIDDEN.withReasonPhrase("Missing role " + SERVICE_ACCOUNT_USER_ROLE
        + " on either the project " + projectId + " or the service account " + serviceAccount)));
  }

  public static ServiceAccountUsageAuthorizer create() {

    final HttpTransport httpTransport;
    try {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException(e);
    }

    final JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
    final GoogleCredential credential;
    try {
      credential = GoogleCredential.getApplicationDefault().createScoped(IamScopes.all());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final String applicationName = "styx-api";

    final CloudResourceManager crm =
        new CloudResourceManager.Builder(httpTransport, jsonFactory, credential)
            .setApplicationName(applicationName)
            .build();

    final Iam iam = new Iam.Builder(
        httpTransport, jsonFactory, credential)
        .setApplicationName(applicationName)
        .build();

    return new ServiceAccountUsageAuthorizer(iam, crm);
  }
}
