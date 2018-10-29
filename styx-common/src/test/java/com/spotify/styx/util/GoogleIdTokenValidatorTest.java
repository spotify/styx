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

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
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
import com.google.api.services.cloudresourcemanager.model.ListProjectsResponse;
import com.google.api.services.cloudresourcemanager.model.Project;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.model.ServiceAccount;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GoogleIdTokenValidatorTest {

  private static final Set<String> DOMAIN_WHITELIST = ImmutableSet.of("example.com", "test.com");

  private static final List<Project> PROJECTS = ImmutableList.of("foo", "bar", "foobar").stream()
      .map(id -> {
        final Project project = new Project();
        project.setProjectId(id);
        return project;
      })
      .collect(toList());

  private static final Project PROJECT;

  private static final ServiceAccount SERVICE_ACCOUNT;

  static {
    PROJECT = new Project();
    PROJECT.setProjectId("barfoo");

    SERVICE_ACCOUNT = new ServiceAccount();
    SERVICE_ACCOUNT.setProjectId("foo");
  }

  private static final GoogleJsonResponseException PERMISSION_DENIED =
      new GoogleJsonResponseException(
          new HttpResponseException.Builder(403, "Forbidden", new HttpHeaders()),
          new GoogleJsonError().set("status", "PERMISSION_DENIED"));

  private static final GoogleJsonResponseException NOT_FOUND =
      new GoogleJsonResponseException(
          new HttpResponseException.Builder(404, "Not found", new HttpHeaders()),
          new GoogleJsonError().set("status", "NOT_FOUND"));

  private GoogleIdTokenValidator validator;

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  @Mock public GoogleIdToken idToken;

  @Mock public GoogleIdToken.Payload idTokenPayload;

  @Mock private GoogleIdTokenVerifier verifier;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private CloudResourceManager cloudResourceManager;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Iam iam;

  @Mock CloudResourceManager.Projects.List projectsList;

  @Mock CloudResourceManager.Projects.Get projectsGet;

  @Mock Iam.Projects.ServiceAccounts.Get serviceAccountsGet;

  @Before
  public void setUp() throws IOException, GeneralSecurityException {
    when(idToken.getPayload()).thenReturn(idTokenPayload);
    when(verifier.verify(anyString())).thenReturn(idToken);

    when(cloudResourceManager.projects().list()).thenReturn(projectsList);

    final ListProjectsResponse listProjectsResponse1 = new ListProjectsResponse();
    listProjectsResponse1.setProjects(PROJECTS);
    listProjectsResponse1.setNextPageToken("token");

    final ListProjectsResponse listProjectsResponse2 = new ListProjectsResponse();

    when(projectsList.execute())
        .thenReturn(listProjectsResponse1)
        .thenReturn(listProjectsResponse2);

//    when(projectsGet.execute()).thenReturn(PROJECT);

    validator = new GoogleIdTokenValidator(verifier, cloudResourceManager, iam, DOMAIN_WHITELIST);
    validator.cacheProjects();
  }

  @Test
  public void shouldFailToLoadCache() throws IOException {
    final IOException exception = new IOException();
    when(projectsList.execute()).thenThrow(exception);
    expectedException.expect(is(exception));
    validator.cacheProjects();
  }

  @Test
  public void shouldFailIfInvalidToken() throws GeneralSecurityException, IOException {
    when(verifier.verify(anyString())).thenThrow(new GeneralSecurityException());
    assertThat(validator.validate("token"), is(nullValue()));

    verifyZeroInteractions(idToken);
    verifyZeroInteractions(projectsGet);
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldFailToVerifyToken() throws GeneralSecurityException, IOException {
    when(verifier.verify(anyString())).thenThrow(new IOException());
    assertThat(validator.validate("token"), is(nullValue()));

    verifyZeroInteractions(idToken);
    verifyZeroInteractions(projectsGet);
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldBeWhitelisted() {
    when(idTokenPayload.getEmail()).thenReturn("foo@example.com");
    assertThat(validator.validate("token"), is(idToken));

    verifyZeroInteractions(projectsGet);
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldFailIfInvalidEmailAddress() {
    when(idTokenPayload.getEmail()).thenReturn("example.com");
    assertThat(validator.validate("token"), is(nullValue()));

    verifyZeroInteractions(projectsGet);
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldHitProjectCache() {
    when(idTokenPayload.getEmail()).thenReturn("foo@foo.iam.gserviceaccount.com");
    assertThat(validator.validate("token"), is(idToken));

    verifyZeroInteractions(projectsGet);
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldMissProjectCache() throws IOException {
    when(projectsGet.execute()).thenReturn(PROJECT);
    when(cloudResourceManager.projects().get(anyString())).thenReturn(projectsGet);

    when(idTokenPayload.getEmail()).thenReturn("foo@barfoo.iam.gserviceaccount.com");
    assertThat(validator.validate("token"), is(idToken));

    verify(projectsGet).execute();
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldFailToGetProject() throws IOException {
    when(cloudResourceManager.projects().get(anyString())).thenThrow(new IOException());

    when(idTokenPayload.getEmail()).thenReturn("foo@barfoo.iam.gserviceaccount.com");
    assertThat(validator.validate("token"), is(nullValue()));

    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldFailForNonExistProject() throws IOException {
    when(cloudResourceManager.projects().get(anyString())).thenThrow(NOT_FOUND);

    when(idTokenPayload.getEmail()).thenReturn("foo@barfoo.iam.gserviceaccount.com");
    assertThat(validator.validate("token"), is(nullValue()));

    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldFailIfNoPermissionGettingProject() throws IOException {
    when(cloudResourceManager.projects().get(anyString())).thenThrow(PERMISSION_DENIED);

    when(idTokenPayload.getEmail()).thenReturn("foo@barfoo.iam.gserviceaccount.com");
    assertThat(validator.validate("token"), is(nullValue()));

    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldHitValidatedEmailCache() {
    when(idTokenPayload.getEmail()).thenReturn("foo@foo.iam.gserviceaccount.com");
    assertThat(validator.validate("token"), is(idToken));

    validator.clearProjectCache();
    assertThat(validator.validate("token"), is(idToken));

    verifyZeroInteractions(projectsGet);
    verifyZeroInteractions(iam);
  }

  @Test
  public void shouldGetProjectFromIAMAndThenHitProjectCache() throws IOException {
    when(serviceAccountsGet.execute()).thenReturn(SERVICE_ACCOUNT);
    when(iam.projects().serviceAccounts().get(anyString())).thenReturn(serviceAccountsGet);
    when(idTokenPayload.getEmail()).thenReturn("foo@developer.gserviceaccount.com");
    assertThat(validator.validate("token"), is(idToken));

    verifyZeroInteractions(projectsGet);
  }

  @Test
  public void shouldFailToGetProjectFromIAM() throws IOException {
    when(iam.projects().serviceAccounts().get(anyString())).thenThrow(new IOException());
    when(idTokenPayload.getEmail()).thenReturn("foo@developer.gserviceaccount.com");
    assertThat(validator.validate("token"), is(nullValue()));

    verifyZeroInteractions(projectsGet);
  }

  @Test
  public void shouldFailForNonExistServiceAccount() throws IOException {
    when(iam.projects().serviceAccounts().get(anyString())).thenThrow(NOT_FOUND);
    when(idTokenPayload.getEmail()).thenReturn("foo@developer.gserviceaccount.com");
    assertThat(validator.validate("token"), is(nullValue()));

    verifyZeroInteractions(projectsGet);
  }

  @Test
  public void shouldFailIfNoPermissionGettingServiceAccountFromIAM() throws IOException {
    when(iam.projects().serviceAccounts().get(anyString())).thenThrow(PERMISSION_DENIED);
    when(idTokenPayload.getEmail()).thenReturn("foo@developer.gserviceaccount.com");
    assertThat(validator.validate("token"), is(nullValue()));

    verifyZeroInteractions(projectsGet);
  }
}
