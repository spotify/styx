/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2018 Spotify AB
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

import static com.github.rholder.retry.StopStrategies.stopAfterDelay;
import static com.github.rholder.retry.WaitStrategies.exponentialWait;
import static com.spotify.apollo.Status.FORBIDDEN;
import static com.spotify.apollo.Status.NOT_FOUND;
import static com.spotify.styx.util.GoogleApiClientUtil.executeWithRetries;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.github.rholder.retry.StopStrategy;
import com.github.rholder.retry.WaitStrategy;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.Ancestor;
import com.google.api.services.cloudresourcemanager.model.GetAncestryRequest;
import com.google.api.services.cloudresourcemanager.model.GetAncestryResponse;
import com.google.api.services.cloudresourcemanager.model.ListProjectsResponse;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.cloudresourcemanager.model.ResourceId;
import com.google.api.services.iam.v1.Iam;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authenticates and authorizes Google ID tokens for incoming Styx API requests.
 */
public class Authenticator {

  private static final Logger logger = LoggerFactory.getLogger(Authenticator.class);

  private static final Pattern SERVICE_ACCOUNT_PATTERN =
      Pattern.compile("^.+\\.gserviceaccount\\.com$");

  private static final Pattern USER_CREATED_SERVICE_ACCOUNT_PATTERN =
      Pattern.compile("^.+@(.+)\\.iam\\.gserviceaccount\\.com$");

  private static final long VALIDATED_EMAIL_CACHE_SIZE = 1000;

  private final GoogleIdTokenVerifier googleIdTokenVerifier;

  private final CloudResourceManager cloudResourceManager;

  private final Iam iam;

  private final Set<String> domainWhitelist;
  private final Set<ResourceId> resourceWhitelist;

  // TODO: projects and folders can move, we should have some cache invalidation
  private final Set<ResourceId> resourceCache = Sets.newConcurrentHashSet();

  private final Cache<String, String> validatedEmailCache = CacheBuilder.newBuilder()
      .maximumSize(VALIDATED_EMAIL_CACHE_SIZE)
      .build();

  private final Collection<String> allowedAudiences;

  private static final StopStrategy DEFAULT_RETRY_STOP_STRATEGY = stopAfterDelay(30, SECONDS);
  private static final WaitStrategy DEFAULT_RETRY_WAIT_STRATEGY = exponentialWait();

  private final WaitStrategy retryWaitStrategy;
  private final StopStrategy retryStopStrategy;

  Authenticator(GoogleIdTokenVerifier googleIdTokenVerifier,
                CloudResourceManager cloudResourceManager,
                Iam iam,
                AuthenticatorConfiguration configuration) {
    this(googleIdTokenVerifier, cloudResourceManager, iam, configuration,
        DEFAULT_RETRY_WAIT_STRATEGY,
        DEFAULT_RETRY_STOP_STRATEGY);
  }

  Authenticator(GoogleIdTokenVerifier googleIdTokenVerifier,
                CloudResourceManager cloudResourceManager,
                Iam iam,
                AuthenticatorConfiguration configuration,
                WaitStrategy retryWaitStrategy,
                StopStrategy retryStopStrategy) {
    this.googleIdTokenVerifier =
        Objects.requireNonNull(googleIdTokenVerifier, "googleIdTokenVerifier");
    this.cloudResourceManager =
        Objects.requireNonNull(cloudResourceManager, "cloudResourceManager");
    this.iam = Objects.requireNonNull(iam, "iam");
    this.domainWhitelist = configuration.domainWhitelist();
    this.resourceWhitelist = configuration.resourceWhitelist();
    this.allowedAudiences = configuration.allowedAudiences();
    this.retryWaitStrategy = Objects.requireNonNull(retryWaitStrategy, "retryWaitStrategy");
    this.retryStopStrategy = Objects.requireNonNull(retryStopStrategy, "retryStopStrategy");
  }

  void cacheResources() throws IOException {
    final CloudResourceManager.Projects.List request = cloudResourceManager.projects().list();

    ListProjectsResponse response;
    do {
      response = executeWithRetries(request, retryWaitStrategy, retryStopStrategy);
      if (response.getProjects() == null) {
        continue;
      }
      for (Project project : response.getProjects()) {
        final boolean access = resolveProject(project);
        logger.info("Resolved project: {}, access={}", project.getProjectId(), access);
      }
      request.setPageToken(response.getNextPageToken());
    } while (response.getNextPageToken() != null);

    logger.info("Resource cache loaded");
  }

  /**
   * Authenticate and authorize a Google ID token string from an incoming Styx API request.
   *
   * @param token The Google ID token string.
   *
   * @return A {@link GoogleIdToken} instance if the token was valid and the user is authorized.
   */
  GoogleIdToken authenticate(String token) {
    final GoogleIdToken googleIdToken;
    try {
      googleIdToken = verifyIdToken(token);
    } catch (IOException e) {
      logger.warn("Rejecting auth token: Failed to verify token", e);
      return null;
    }

    if (googleIdToken == null) {
      logger.warn("Rejecting auth token: verifyIdToken returned null");
      return null;
    }

    final String email = googleIdToken.getPayload().getEmail();
    if (email == null) {
      logger.warn("Rejecting auth token: No email in id token");
      return null;
    }

    final String domain = getDomain(email);
    if (domain == null) {
      logger.warn("Rejecting auth token: Invalid email address {}", email);
      return null;
    } else if (domainWhitelist.contains(domain)) {
      logger.debug("Validating auth token: Domain {} in whitelist", domain);
      return googleIdToken;
    }

    if (validatedEmailCache.getIfPresent(email) != null) {
      logger.debug("Validating auth token: Cache hit for {}", email);
      return googleIdToken;
    }

    // Is this a GCP service account?
    if (!SERVICE_ACCOUNT_PATTERN.matcher(email).matches()) {
      logger.warn("Rejecting auth token: Not a service account: {}", email);
      return null;
    }

    // TODO: Also verify audience for user tokens. Currently this would require changing the auth flow in styx
    //  clients and make users explicitly "log in" to Styx via the OAuth consent screen.
    // Verify that this ID token was intended for Styx.
    if (!allowedAudiences.isEmpty()
        // TODO: Remove this null check and require tokens to have a target audience
        && googleIdToken.getPayload().getAudience() != null) {
      if (!googleIdToken.verifyAudience(allowedAudiences)) {
        logger.warn("Rejecting auth token: ID token wasn't intended for Styx");
        return null;
      }
    }

    final String projectId;
    try {
      projectId = checkServiceAccountProject(email);
    } catch (IOException e) {
      logger.warn("Rejecting auth token: Cannot authenticate {}", email);
      return null;
    }

    if (projectId != null) {
      validatedEmailCache.put(email, projectId);
      return googleIdToken;
    }

    logger.warn("Rejecting auth token");
    return null;
  }

  @VisibleForTesting
  void clearResourceCache() {
    resourceCache.clear();
  }

  private boolean isWhitelisted(ResourceId resourceId) {
    return resourceWhitelist.contains(resourceId) || resourceCache.contains(resourceId);
  }

  @VisibleForTesting
  static ResourceId resourceId(String type, String id) {
    return new ResourceId().setType(type).setId(id);
  }

  @VisibleForTesting
  static ResourceId resourceId(Project project) {
    return resourceId("project", project.getProjectId());
  }

  private GoogleIdToken verifyIdToken(String token) throws IOException {
    try {
      return googleIdTokenVerifier.verify(token);
    } catch (GeneralSecurityException e) {
      logger.warn("Caught GeneralSecurityException when validating token", e);
      return null;
    }
  }

  private String checkServiceAccountProject(String email) throws IOException {
    String projectId = null;

    // Check if this is a user created SA with the project id in the email domain
    final Matcher matcher = USER_CREATED_SERVICE_ACCOUNT_PATTERN.matcher(email);
    if (matcher.matches()) {
      projectId = matcher.group(1);
    }

    if (projectId == null) {
      // Not recognized as a user created SA, could be GCE default or app engine, etc
      logger.debug("Email {} doesn't contain project id, looking up its project", email);
      projectId = getProjectIdOfServiceAccount(email);
    }

    if (projectId == null) {
      return null;
    }

    if (isWhitelisted(resourceId("project", projectId))) {
      logger.debug("Hit cache for project id {}", projectId);
      return projectId;
    }

    if (resolveProjectAccess(projectId)) {
      return projectId;
    }

    return null;
  }

  private String getProjectIdOfServiceAccount(String email) throws IOException {
    var request = iam.projects().serviceAccounts().get("projects/-/serviceAccounts/" + email);
    try {
      var serviceAccount = executeWithRetries(request, retryWaitStrategy, retryStopStrategy);
      return serviceAccount.getProjectId();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == NOT_FOUND.code()) {
        logger.debug("Service account {} doesn't exist", email, e);
        return null;
      }

      logger.info("Cannot get project id for service account {}", email, e);
      return null;
    }
  }

  private boolean resolveProjectAccess(String projectId) throws IOException {
    final GetAncestryResponse ancestry;
    var request = cloudResourceManager.projects().getAncestry(projectId, new GetAncestryRequest());
    try {
      ancestry = executeWithRetries(request, retryWaitStrategy, retryStopStrategy);
    } catch (GoogleJsonResponseException e) {
      // GCP returns 403 in case of project not found for security reason, but that makes it impossible
      // to differentiate that from missing permission; we take the risk here assuming Styx service account does have
      // proper permissions
      if (e.getStatusCode() == FORBIDDEN.code()) {
        logger.debug("Project {} doesn't exist", projectId, e);
      } else {
        logger.info("Cannot get project with id {}", projectId, e);
      }
      return false;
    }
    if (ancestry.getAncestor() == null) {
      return false;
    }
    return resolveAccess(ancestry.getAncestor());
  }

  private boolean resolveProject(Project project) throws IOException {
    final ResourceId resourceId = resourceId(project);
    if (isWhitelisted(resourceId)) {
      return true;
    }
    if (project.getParent() != null && isWhitelisted(project.getParent())) {
      return true;
    }
    return resolveProjectAccess(project.getProjectId());
  }

  private boolean resolveAccess(List<Ancestor> ancestry) {
    for (int i = 0; i < ancestry.size(); i++) {
      final Ancestor ancestor = ancestry.get(i);
      if (isWhitelisted(ancestor.getResourceId())) {
        // Cache descendants
        for (Ancestor descendant : ancestry.subList(0, i)) {
          if (resourceCache.add(descendant.getResourceId())) {
            logger.debug("Whitelist cached {}/{}, descendant of {}/{}",
                descendant.getResourceId().getType(), descendant.getResourceId().getId(),
                ancestor.getResourceId().getType(), ancestor.getResourceId().getId());
          }
        }
        return true;
      }
    }
    return false;
  }

  private static String getDomain(String email) {
    final int index = email.indexOf('@');
    if (index == -1) {
      return null;
    } else {
      return email.substring(index + 1);
    }
  }
}
