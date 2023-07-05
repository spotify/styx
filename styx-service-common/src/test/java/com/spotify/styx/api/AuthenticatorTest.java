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

import static com.spotify.apollo.Status.FORBIDDEN;
import static com.spotify.apollo.Status.TOO_MANY_REQUESTS;
import static com.spotify.styx.api.Authenticator.resourceId;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.StopStrategy;
import com.github.rholder.retry.WaitStrategies;
import com.github.rholder.retry.WaitStrategy;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.Ancestor;
import com.google.api.services.cloudresourcemanager.model.GetAncestryResponse;
import com.google.api.services.cloudresourcemanager.model.ListProjectsResponse;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.cloudresourcemanager.model.ResourceId;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.model.ServiceAccount;
import com.spotify.apollo.Status;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AuthenticatorTest {

  private static final ResourceId ORGANIZATION_RESOURCE = resourceId("organization", "test-org");
  private static final ResourceId FOLDER_RESOURCE = resourceId("folder", "test-folder");

  private static final Project FOO_PROJECT = new Project().setProjectId("foo").setParent(ORGANIZATION_RESOURCE);
  private static final Project BAR_PROJECT = new Project().setProjectId("bar").setParent(FOLDER_RESOURCE);
  private static final Project BAZ_PROJECT = new Project().setProjectId("baz");
  private static final Project EXTERNAL_PROJECT = new Project().setProjectId("external");

  private static final List<Project> PROJECTS = List.of(
      FOO_PROJECT, BAR_PROJECT, BAZ_PROJECT, EXTERNAL_PROJECT);

  private static final List<ResourceId> WHITELIST = List.of(
      ORGANIZATION_RESOURCE, FOLDER_RESOURCE, resourceId(BAZ_PROJECT));

  private static final String DOMAIN1 = "example.com";
  private static final String DOMAIN2 = "test.com";

  private static final String STYX_OAUTH_CLIENT_ID =
      "123456789012-823nsdf8whq4r4tbsjdhg923nksrgj04.apps.googleusercontent.com";
  private static final String STYX_API_HOSTNAME = "https://styx.example.net";

  private static final AuthenticatorConfiguration CONFIGURATION = AuthenticatorConfiguration.builder()
      .domainWhitelist(DOMAIN1, DOMAIN2)
      .resourceWhitelist(WHITELIST)
      .allowedAudiences(STYX_API_HOSTNAME, STYX_OAUTH_CLIENT_ID)
      .service("test")
      .build();

  private static final ResourceId UNCACHED_FOLDER_RESOURCE = resourceId("folder", "uncached-test-folder");
  private static final Project UNCACHED_PROJECT = new Project()
      .setProjectId("uncached")
      .setParent(UNCACHED_FOLDER_RESOURCE);

  private static final ServiceAccount SERVICE_ACCOUNT = new ServiceAccount()
      .setProjectId("foo");

  private static final GoogleJsonResponseException PERMISSION_DENIED =
      new GoogleJsonResponseException(
          new HttpResponseException.Builder(FORBIDDEN.code(), "Forbidden", new HttpHeaders()),
          new GoogleJsonError().set("status", "PERMISSION_DENIED"));

  private static final GoogleJsonResponseException NOT_FOUND =
      new GoogleJsonResponseException(
          new HttpResponseException.Builder(Status.NOT_FOUND.code(), "Not found", new HttpHeaders()),
          new GoogleJsonError().set("status", "NOT_FOUND"));

  private static final GoogleJsonResponseException QUOTA_EXHAUSTED =
      new GoogleJsonResponseException(
          new HttpResponseException.Builder(TOO_MANY_REQUESTS.code(), "per-user search quota temporarily exhausted", new HttpHeaders()),
          new GoogleJsonError().set("status", "RESOURCE_EXHAUSTED"));

  private static final WaitStrategy RETRY_WAIT_STRATEGY = WaitStrategies.noWait();
  private static final StopStrategy RETRY_STOP_STRATEGY = StopStrategies.stopAfterAttempt(3);

  private Authenticator validator;

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Mock private GoogleIdToken idToken;
  @Mock private GoogleIdTokenVerifier verifier;
  @Mock private CloudResourceManager cloudResourceManager;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS) private Iam iam;
  @Mock private CloudResourceManager.Projects projects;
  @Mock private CloudResourceManager.Projects.List projectsList;
  @Mock private CloudResourceManager.Projects.GetAncestry projectsGetAncestry;
  @Mock private Iam.Projects.ServiceAccounts.Get serviceAccountsGet;

  private final GoogleIdToken.Payload idTokenPayload = new GoogleIdToken.Payload();

  @Before
  public void setUp() throws IOException, GeneralSecurityException {
    idTokenPayload.setAudience(STYX_API_HOSTNAME);
    when(idToken.getPayload()).thenReturn(idTokenPayload);
    when(verifier.verify(anyString())).thenReturn(idToken);

    lenient().when(cloudResourceManager.projects()).thenReturn(projects);

    when(projects.getAncestry(any(), any())).thenReturn(projectsGetAncestry);
    when(iam.projects().serviceAccounts().get(any())).thenReturn(serviceAccountsGet);
    verify(iam).projects();

    mockAncestryResponse(FOO_PROJECT, resourceId(FOO_PROJECT), ORGANIZATION_RESOURCE);
    mockAncestryResponse(BAR_PROJECT, resourceId(BAR_PROJECT), FOLDER_RESOURCE);
    mockAncestryResponse(BAZ_PROJECT, resourceId(BAZ_PROJECT));
    mockAncestryResponse(EXTERNAL_PROJECT, resourceId(EXTERNAL_PROJECT));

    when(projects.list()).thenReturn(projectsList);

    final ListProjectsResponse listProjectsResponse1 = new ListProjectsResponse();
    listProjectsResponse1.setProjects(PROJECTS);
    listProjectsResponse1.setNextPageToken("token");

    final ListProjectsResponse listProjectsResponse2 = new ListProjectsResponse();

    when(projectsList.execute())
        .thenReturn(listProjectsResponse1)
        .thenReturn(listProjectsResponse2);

    validator = new Authenticator(verifier, cloudResourceManager, iam, CONFIGURATION,
        RETRY_WAIT_STRATEGY, RETRY_STOP_STRATEGY);
    validator.cacheResources();
  }

  private void mockAncestryResponse(Project project, ResourceId... ancestors) throws IOException {
    final CloudResourceManager.Projects.GetAncestry ancestry = mock(CloudResourceManager.Projects.GetAncestry.class);
    doReturn(ancestryResponse(ancestors)).when(ancestry).execute();
    when(cloudResourceManager.projects().getAncestry(eq(project.getProjectId()), any()))
        .thenReturn(ancestry);
  }

  @Test
  public void shouldFailExternalProject() {
    idTokenPayload.setEmail("foo@external.iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));
  }

  @Test
  public void shouldFailToLoadCache() throws IOException {
    reset(projectsList);
    final IOException exception = new IOException();
    doThrow(exception).when(projectsList).execute();
    try {
      validator.cacheResources();
      fail("Expected exception");
    } catch (IOException e) {
      assertThat(e.getCause().getCause(), is(exception));
    }
    verify(projectsList, times(3)).execute();
  }

  @Test
  public void shouldRetryToLoadCacheOnQuotaExhausted() throws IOException {
    reset(projectsList);
    when(projectsList.execute())
        .thenThrow(QUOTA_EXHAUSTED)
        .thenReturn(new ListProjectsResponse().setProjects(PROJECTS));
    validator.cacheResources();
    verify(projectsList, times(2)).execute();
  }

  @Test
  public void shouldFailIfInvalidToken() throws GeneralSecurityException, IOException {
    when(verifier.verify(anyString())).thenThrow(new GeneralSecurityException());
    assertThat(validator.authenticate("token"), is(nullValue()));

    verifyNoMoreInteractions(idToken);
    verifyNoMoreInteractions(projectsGetAncestry);
    verifyNoMoreInteractions(iam);
  }

  @Test
  public void shouldFailToVerifyToken() throws GeneralSecurityException, IOException {
    when(verifier.verify(anyString())).thenThrow(new IOException());
    assertThat(validator.authenticate("token"), is(nullValue()));

    verifyNoMoreInteractions(idToken);
    verifyNoMoreInteractions(projectsGetAncestry);
    verifyNoMoreInteractions(iam);
  }

  @Test
  public void shouldBeWhitelisted() {
    idTokenPayload.setEmail("foo@example.com");
    assertThat(validator.authenticate("token"), is(idToken));

    verifyNoMoreInteractions(projectsGetAncestry);
    verifyNoMoreInteractions(iam);
  }

  @Test
  public void shouldFailIfInvalidEmailAddress() {
    idTokenPayload.setEmail("example.com");
    assertThat(validator.authenticate("token"), is(nullValue()));

    verifyNoMoreInteractions(projectsGetAncestry);
    verifyNoMoreInteractions(iam);
  }

  @Test
  public void shouldHitProjectCache() throws IOException {

    // Populate cache
    idTokenPayload.setEmail("foo@foo.iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(idToken));

    // Hit cache
    reset(cloudResourceManager.projects());
    idTokenPayload.setEmail("bar@foo.iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(idToken));

    verify(cloudResourceManager.projects(), never()).getAncestry(eq("foo"), any());
    verifyNoMoreInteractions(projectsGetAncestry);
    verifyNoMoreInteractions(iam);
  }

  @Test
  public void shouldMissProjectCache() throws IOException {
    when(projectsGetAncestry.execute()).thenReturn(
        ancestryResponse(resourceId(UNCACHED_PROJECT), UNCACHED_FOLDER_RESOURCE, ORGANIZATION_RESOURCE));

    idTokenPayload.setEmail("foo@" + UNCACHED_PROJECT.getProjectId() + ".iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(idToken));

    verify(cloudResourceManager.projects()).getAncestry(eq(UNCACHED_PROJECT.getProjectId()), any());
    verify(projectsGetAncestry).execute();
    verifyNoMoreInteractions(iam);
  }

  private static GetAncestryResponse ancestryResponse(ResourceId... ancestors) {
    return new GetAncestryResponse()
        .setAncestor(Stream.of(ancestors)
            .map(id -> new Ancestor().setResourceId(id))
            .collect(toList()));
  }

  @Test
  public void shouldFailToGetProject() throws IOException {
    doThrow(new IOException()).when(projectsGetAncestry).execute();

    idTokenPayload.setEmail("foo@barfoo.iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));

    verifyNoMoreInteractions(iam);
    verify(projectsGetAncestry, times(3)).execute();
  }

  @Test
  public void shouldFailForNonExistProject() throws IOException {
    doThrow(PERMISSION_DENIED).when(projectsGetAncestry).execute();

    idTokenPayload.setEmail("foo@barfoo.iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));

    verify(projectsGetAncestry).execute();
    verifyNoMoreInteractions(iam);
  }

  @Test
  public void shouldFailIfOtherErrorGettingProject() throws IOException {
    doThrow(QUOTA_EXHAUSTED).when(projectsGetAncestry).execute();

    idTokenPayload.setEmail("foo@barfoo.iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));

    verifyNoMoreInteractions(iam);
    verify(projectsGetAncestry, times(3)).execute();
  }

  @Test
  public void shouldHitValidatedEmailCache() throws IOException {
    when(projectsGetAncestry.execute()).thenReturn(
        ancestryResponse(resourceId(UNCACHED_PROJECT), UNCACHED_FOLDER_RESOURCE, ORGANIZATION_RESOURCE));

    idTokenPayload.setEmail("foo@uncached.iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(idToken));

    // TODO: this is a serious code smell
    validator.clearResourceCache();
    reset(projectsGetAncestry);
    assertThat(validator.authenticate("token"), is(idToken));

    verifyNoMoreInteractions(projectsGetAncestry);
    verifyNoMoreInteractions(iam);
  }

  @Test
  public void shouldGetProjectFromIAMAndThenHitProjectCache() throws IOException {
    mockAncestryResponse(FOO_PROJECT, resourceId(FOO_PROJECT), ORGANIZATION_RESOURCE);
    when(serviceAccountsGet.execute()).thenReturn(SERVICE_ACCOUNT);
    idTokenPayload.setEmail("foo@developer.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(idToken));

    verify(serviceAccountsGet).execute();
    verifyNoMoreInteractions(projectsGetAncestry);
  }

  @Test
  public void shouldFailToGetProjectFromIAM() throws IOException {
    doThrow(new IOException()).when(serviceAccountsGet).execute();
    idTokenPayload.setEmail("foo@developer.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));

    verify(serviceAccountsGet, times(3)).execute();
    verifyNoMoreInteractions(projectsGetAncestry);
  }

  @Test
  public void shouldFailForNonExistServiceAccount() throws IOException {
    doThrow(NOT_FOUND).when(serviceAccountsGet).execute();
    idTokenPayload.setEmail("foo@developer.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));

    verify(serviceAccountsGet).execute();
    verifyNoMoreInteractions(projectsGetAncestry);
  }

  @Test
  public void shouldFailIfNoPermissionGettingServiceAccountFromIAM() throws IOException {
    doThrow(PERMISSION_DENIED).when(serviceAccountsGet).execute();
    idTokenPayload.setEmail("foo@developer.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));

    verify(serviceAccountsGet).execute();
    verifyNoMoreInteractions(projectsGetAncestry);
  }

  @Test
  public void shouldFailTokenWithNoEmail() {
    idTokenPayload.setEmail(null);
    assertThat(validator.authenticate("token"), is(nullValue()));
    verifyNoMoreInteractions(projectsGetAncestry);
    verifyNoMoreInteractions(iam);
  }

  @Test
  public void shouldDenyWhitelistedEmailNonSA() {
    idTokenPayload.setEmail("foo@bar.com");
    assertThat(validator.authenticate("token"), is(nullValue()));
    verifyNoMoreInteractions(projectsGetAncestry);
    verifyNoMoreInteractions(iam);
  }

  @Test
  public void shouldDenyIncorrectAudience() {
    idTokenPayload.setAudience("https://foo.example.net");
    idTokenPayload.setEmail("foo@foo.iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));
    verifyNoMoreInteractions(projectsGetAncestry);
    verifyNoMoreInteractions(iam);
  }

  @Test
  public void shouldAllowIncorrectAudienceIfNoAllowedAudiencesConfigured() {
    var configurationWithNoAllowedAudiences = AuthenticatorConfigurationBuilder.from(CONFIGURATION)
        .allowedAudiences(Set.of())
        .build();
    validator = new Authenticator(verifier, cloudResourceManager, iam, configurationWithNoAllowedAudiences);
    idTokenPayload.setAudience("https://foo.example.net");
    idTokenPayload.setEmail("foo@foo.iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(idToken));
  }

  @Test
  public void shouldAllowNoAudience() {
    idTokenPayload.setAudience(null);
    idTokenPayload.setEmail("foo@foo.iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(idToken));
  }
}
