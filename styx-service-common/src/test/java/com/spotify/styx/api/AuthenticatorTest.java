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

import static com.spotify.styx.api.Authenticator.resourceId;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

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
          new HttpResponseException.Builder(403, "Forbidden", new HttpHeaders()),
          new GoogleJsonError().set("status", "PERMISSION_DENIED"));

  private static final GoogleJsonResponseException NOT_FOUND =
      new GoogleJsonResponseException(
          new HttpResponseException.Builder(404, "Not found", new HttpHeaders()),
          new GoogleJsonError().set("status", "NOT_FOUND"));

  private Authenticator validator;

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Mock private GoogleIdToken idToken;
  @Mock private GoogleIdTokenVerifier verifier;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS) private CloudResourceManager cloudResourceManager;
  @Mock(answer = Answers.RETURNS_DEEP_STUBS) private Iam iam;
  @Mock private CloudResourceManager.Projects.List projectsList;
  @Mock private CloudResourceManager.Projects.GetAncestry projectsGetAncestry;
  @Mock private Iam.Projects.ServiceAccounts.Get serviceAccountsGet;

  private final GoogleIdToken.Payload idTokenPayload = new GoogleIdToken.Payload();

  @Before
  public void setUp() throws IOException, GeneralSecurityException {
    idTokenPayload.setAudience(STYX_API_HOSTNAME);
    when(idToken.getPayload()).thenReturn(idTokenPayload);
    when(verifier.verify(anyString())).thenReturn(idToken);

    when(cloudResourceManager.projects().getAncestry(any(), any())).thenReturn(projectsGetAncestry);

    mockAncestryResponse(FOO_PROJECT, resourceId(FOO_PROJECT), ORGANIZATION_RESOURCE);
    mockAncestryResponse(BAR_PROJECT, resourceId(BAR_PROJECT), FOLDER_RESOURCE);
    mockAncestryResponse(BAZ_PROJECT, resourceId(BAZ_PROJECT));
    mockAncestryResponse(EXTERNAL_PROJECT, resourceId(EXTERNAL_PROJECT));

    when(cloudResourceManager.projects().list()).thenReturn(projectsList);

    final ListProjectsResponse listProjectsResponse1 = new ListProjectsResponse();
    listProjectsResponse1.setProjects(PROJECTS);
    listProjectsResponse1.setNextPageToken("token");

    final ListProjectsResponse listProjectsResponse2 = new ListProjectsResponse();

    when(projectsList.execute())
        .thenReturn(listProjectsResponse1)
        .thenReturn(listProjectsResponse2);

    validator = new Authenticator(verifier, cloudResourceManager, iam, CONFIGURATION);
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
    final IOException exception = new IOException();
    when(projectsList.execute()).thenThrow(exception);
    expectedException.expect(is(exception));
    validator.cacheResources();
  }

  @Test
  public void shouldFailIfInvalidToken() throws GeneralSecurityException, IOException {
    when(verifier.verify(anyString())).thenThrow(new GeneralSecurityException());
    assertThat(validator.authenticate("token"), is(nullValue()));

    verifyZeroInteractions(idToken);
    verifyZeroInteractions(projectsGetAncestry);
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldFailToVerifyToken() throws GeneralSecurityException, IOException {
    when(verifier.verify(anyString())).thenThrow(new IOException());
    assertThat(validator.authenticate("token"), is(nullValue()));

    verifyZeroInteractions(idToken);
    verifyZeroInteractions(projectsGetAncestry);
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldBeWhitelisted() {
    idTokenPayload.setEmail("foo@example.com");
    assertThat(validator.authenticate("token"), is(idToken));

    verifyZeroInteractions(projectsGetAncestry);
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldFailIfInvalidEmailAddress() {
    idTokenPayload.setEmail("example.com");
    assertThat(validator.authenticate("token"), is(nullValue()));

    verifyZeroInteractions(projectsGetAncestry);
    verifyZeroInteractions(iam);
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
    verifyZeroInteractions(projectsGetAncestry);
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldMissProjectCache() throws IOException {
    when(projectsGetAncestry.execute()).thenReturn(
        ancestryResponse(resourceId(UNCACHED_PROJECT), UNCACHED_FOLDER_RESOURCE, ORGANIZATION_RESOURCE));

    idTokenPayload.setEmail("foo@" + UNCACHED_PROJECT.getProjectId() + ".iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(idToken));

    verify(cloudResourceManager.projects()).getAncestry(eq(UNCACHED_PROJECT.getProjectId()), any());
    verify(projectsGetAncestry).execute();
    verifyZeroInteractions(iam);
  }

  private static GetAncestryResponse ancestryResponse(ResourceId... ancestors) {
    return new GetAncestryResponse()
        .setAncestor(Stream.of(ancestors)
            .map(id -> new Ancestor().setResourceId(id))
            .collect(toList()));
  }

  @Test
  public void shouldFailToGetProject() throws IOException {
    when(cloudResourceManager.projects().getAncestry(any(), any())).thenThrow(new IOException());

    idTokenPayload.setEmail("foo@barfoo.iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));

    verify(cloudResourceManager.projects()).getAncestry(eq("barfoo"), any());
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldFailForNonExistProject() throws IOException {
    when(cloudResourceManager.projects().getAncestry(any(), any())).thenThrow(NOT_FOUND);

    idTokenPayload.setEmail("foo@barfoo.iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));

    verify(cloudResourceManager.projects()).getAncestry(eq("barfoo"), any());
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldFailIfNoPermissionGettingProject() throws IOException {
    when(cloudResourceManager.projects().getAncestry(any(), any())).thenThrow(PERMISSION_DENIED);

    idTokenPayload.setEmail("foo@barfoo.iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));

    verify(cloudResourceManager.projects()).getAncestry(eq("barfoo"), any());
    verifyZeroInteractions(iam);
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

    verifyZeroInteractions(projectsGetAncestry);
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldGetProjectFromIAMAndThenHitProjectCache() throws IOException {
    mockAncestryResponse(FOO_PROJECT, resourceId(FOO_PROJECT), ORGANIZATION_RESOURCE);
    when(serviceAccountsGet.execute()).thenReturn(SERVICE_ACCOUNT);
    when(iam.projects().serviceAccounts().get(anyString())).thenReturn(serviceAccountsGet);
    idTokenPayload.setEmail("foo@developer.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(idToken));

    verify(serviceAccountsGet).execute();
    verifyZeroInteractions(projectsGetAncestry);
  }

  @Test
  public void shouldFailToGetProjectFromIAM() throws IOException {
    when(iam.projects().serviceAccounts().get(anyString())).thenThrow(new IOException());
    idTokenPayload.setEmail("foo@developer.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));

    verify(iam.projects().serviceAccounts()).get(anyString());
    verifyZeroInteractions(projectsGetAncestry);
  }

  @Test
  public void shouldFailForNonExistServiceAccount() throws IOException {
    when(iam.projects().serviceAccounts().get(anyString())).thenThrow(NOT_FOUND);
    idTokenPayload.setEmail("foo@developer.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));

    verify(iam.projects().serviceAccounts()).get(anyString());
    verifyZeroInteractions(projectsGetAncestry);
  }

  @Test
  public void shouldFailIfNoPermissionGettingServiceAccountFromIAM() throws IOException {
    when(iam.projects().serviceAccounts().get(anyString())).thenThrow(PERMISSION_DENIED);
    idTokenPayload.setEmail("foo@developer.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));

    verify(iam.projects().serviceAccounts()).get(anyString());
    verifyZeroInteractions(projectsGetAncestry);
  }

  @Test
  public void shouldFailTokenWithNoEmail() {
    idTokenPayload.setEmail(null);
    assertThat(validator.authenticate("token"), is(nullValue()));
    verifyZeroInteractions(projectsGetAncestry);
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldDenyWhitelistedEmailNonSA() {
    idTokenPayload.setEmail("foo@bar.com");
    assertThat(validator.authenticate("token"), is(nullValue()));
    verifyZeroInteractions(projectsGetAncestry);
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldDenyIncorrectAudience() {
    idTokenPayload.setAudience("https://foo.example.net");
    idTokenPayload.setEmail("foo@foo.iam.gserviceaccount.com");
    assertThat(validator.authenticate("token"), is(nullValue()));
    verifyZeroInteractions(projectsGetAncestry);
    verifyZeroInteractions(iam);
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
}
