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
import com.google.api.services.iam.v1.model.ServiceAccount;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  Authenticator(GoogleIdTokenVerifier googleIdTokenVerifier,
      CloudResourceManager cloudResourceManager,
      Iam iam,
      AuthenticatorConfiguration configuration) {
    this.googleIdTokenVerifier =
        Objects.requireNonNull(googleIdTokenVerifier, "googleIdTokenVerifier");
    this.cloudResourceManager =
        Objects.requireNonNull(cloudResourceManager, "cloudResourceManager");
    this.iam = Objects.requireNonNull(iam, "iam");
    this.domainWhitelist = configuration.domainWhitelist();
    this.resourceWhitelist = configuration.resourceWhitelist();
  }

  void cacheResources() throws IOException {
    final CloudResourceManager.Projects.List request = cloudResourceManager.projects().list();

    ListProjectsResponse response;
    do {
      response = request.execute();
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

  GoogleIdToken authenticate(String token) {
    final GoogleIdToken googleIdToken;
    try {
      googleIdToken = verifyIdToken(token);
    } catch (IOException e) {
      logger.warn("Failed to verify token");
      return null;
    }

    if (googleIdToken == null) {
      return null;
    }

    final String email = googleIdToken.getPayload().getEmail();
    if (email == null) {
      logger.debug("No email in id token");
      return null;
    }

    final String domain = getDomain(email);
    if (domain != null) {
      if (domainWhitelist.contains(domain)) {
        logger.debug("Domain {} in whitelist", domain);
        return googleIdToken;
      }
    } else {
      logger.warn("Invalid email address {}", email);
      return null;
    }

    if (validatedEmailCache.getIfPresent(email) != null) {
      return googleIdToken;
    }

    // Is this a GCP service account?
    if (!SERVICE_ACCOUNT_PATTERN.matcher(email).matches()) {
      return null;
    }

    final String projectId;
    try {
      projectId = checkServiceAccountProject(email);
    } catch (IOException e) {
      logger.info("Cannot authenticate {}", email);
      return null;
    }

    if (projectId != null) {
      validatedEmailCache.put(email, projectId);
      return googleIdToken;
    }

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
    try {
      final ServiceAccount serviceAccount =
          iam.projects().serviceAccounts().get("projects/-/serviceAccounts/" + email).execute();
      return serviceAccount.getProjectId();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == 404) {
        logger.debug("Service account {} doesn't exist", email, e);
        return null;
      }

      logger.info("Cannot get project id for service account {}", email, e);
      return null;
    }
  }

  private boolean resolveProjectAccess(String projectId) throws IOException {
    final GetAncestryResponse ancestry;
    try {
      ancestry = cloudResourceManager.projects().getAncestry(projectId, new GetAncestryRequest()).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == 404) {
        logger.debug("Project {} doesn't exist", projectId, e);
        return false;
      }

      // TODO: handle 403 quota exhausted?
      logger.info("Cannot get project with id {}", projectId, e);
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
