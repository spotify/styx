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

package com.spotify.styx.util;

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.ListProjectsResponse;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.model.ServiceAccount;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Authenticator {

  private static final Logger logger = LoggerFactory.getLogger(Authenticator.class);

  private static final Pattern SERVICE_ACCOUNT_PATTERN =
      Pattern.compile("^.+@(.+)\\.iam\\.gserviceaccount\\.com$");

  private static final long VALIDATED_EMAIL_CACHE_SIZE = 1000;

  private final GoogleIdTokenVerifier googleIdTokenVerifier;

  private final CloudResourceManager cloudResourceManager;

  private final Iam iam;

  private final Set<String> domainWhitelist;

  private final Set<String> projectCache = Sets.newConcurrentHashSet();

  private final Cache<String, String> validatedEmailCache = CacheBuilder.newBuilder()
      .maximumSize(VALIDATED_EMAIL_CACHE_SIZE)
      .build();

  public Authenticator(GoogleIdTokenVerifier googleIdTokenVerifier,
                       CloudResourceManager cloudResourceManager,
                       Iam iam,
                       Set<String> domainWhitelist) {
    this.googleIdTokenVerifier =
        Objects.requireNonNull(googleIdTokenVerifier, "googleIdTokenVerifier");
    this.cloudResourceManager =
        Objects.requireNonNull(cloudResourceManager, "cloudResourceManager");
    this.iam = Objects.requireNonNull(iam, "iam");
    this.domainWhitelist = Objects.requireNonNull(domainWhitelist, "domainWhitelist");
  }

  public void cacheProjects() throws IOException {
    final CloudResourceManager.Projects.List request = cloudResourceManager.projects().list();

    ListProjectsResponse response;
    do {
      response = request.execute();
      if (response.getProjects() == null) {
        continue;
      }
      for (Project project : response.getProjects()) {
        projectCache.add(project.getProjectId());
      }
      request.setPageToken(response.getNextPageToken());
    } while (response.getNextPageToken() != null);

    logger.info("project cache loaded");
  }

  public GoogleIdToken authenticate(String token) {
    final GoogleIdToken googleIdToken;
    try {
      googleIdToken = verifyIdToken(token);
    } catch (IOException e) {
      logger.warn("failed to verify token");
      return null;
    }

    if (googleIdToken == null) {
      return null;
    }

    final String email = googleIdToken.getPayload().getEmail();

    final String domain = getDomain(email);
    if (domain != null) {
      if (domainWhitelist.contains(domain)) {
        logger.debug("domain {} in whitelist", domain);
        return googleIdToken;
      }
    } else {
      logger.warn("invalid email address {}", email);
      return null;
    }

    if (validatedEmailCache.getIfPresent(email) != null) {
      return googleIdToken;
    }

    final String projectId;
    try {
      projectId = checkProject(email);
    } catch (IOException e) {
      logger.info("cannot authenticate {}", email);
      return null;
    }

    if (projectId != null) {
      validatedEmailCache.put(email, projectId);
      return googleIdToken;
    }

    return null;
  }

  @VisibleForTesting
  void clearProjectCache() {
    projectCache.clear();
  }

  private GoogleIdToken verifyIdToken(String token) throws IOException {
    try {
      return googleIdTokenVerifier.verify(token);
    } catch (GeneralSecurityException e) {
      return null;
    }
  }

  private String checkProject(String email) throws IOException {
    String projectId = null;

    final Matcher matcher = SERVICE_ACCOUNT_PATTERN.matcher(email);
    if (matcher.matches()) {
      projectId = matcher.group(1);
    }

    if (projectId == null) {
      // no projectId, could be GCE default
      logger.debug("{} doesn't contain project id, try getting its project", email);
      projectId = getProjectIdOfServiceAccount(email);
    }

    if (projectId == null) {
      return null;
    }

    if (projectCache.contains(projectId)) {
      logger.debug("hit cache for project id {}", projectId);
      return projectId;
    } else if (checkProjectId(projectId)) {
      projectCache.add(projectId);
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
        logger.debug("service account {} doesn't exist", email, e);
        return null;
      }

      logger.info("cannot get project id for service account {}", email, e);
      return null;
    }
  }

  private boolean checkProjectId(String projectId) throws IOException {
    try {
      cloudResourceManager.projects().get(projectId).execute();
      return true;
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() == 404) {
        logger.debug("project {} doesn't exist", projectId, e);
        return false;
      }

      logger.info("cannot get project with id {}", projectId, e);
      return false;
    }
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
