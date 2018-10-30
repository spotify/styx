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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.cloudresourcemanager.model.ListProjectsResponse;
import com.google.api.services.iam.v1.Iam;
import com.google.common.collect.ImmutableSet;
import com.spotify.styx.util.GoogleIdTokenValidatorFactory.DefaultGoogleIdTokenValidatorFactory;
import java.io.IOException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class GoogleIdTokenValidatorFactoryTest {

  @Rule public final ExpectedException expectedException = ExpectedException.none();

  private GoogleIdTokenValidatorFactory googleIdTokenValidatorFactory;
  
  @Mock GoogleIdTokenVerifier googleIdTokenVerifier;
  
  @Mock GoogleCredential googleCredential;
  
  @Mock Iam iam;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  CloudResourceManager cloudResourceManager;

  @Mock CloudResourceManager.Projects.List projectsList;
  
  @Mock HttpTransport httpTransport;
  
  @Mock JsonFactory jsonFactory;
  
  private class TestGoogleIdTokenValidatorFactory extends DefaultGoogleIdTokenValidatorFactory {

    @Override
    GoogleIdTokenVerifier buildGoogleIdTokenVerifier(HttpTransport httpTransport,
                                                     JsonFactory jsonFactory) {
      return googleIdTokenVerifier;
    }

    @Override
    GoogleCredential loadCredential() {
      return googleCredential;
    }

    @Override
    Iam buildIam(HttpTransport httpTransport, JsonFactory jsonFactory,
                 GoogleCredential credential,
                 String service) {
      return iam;
    }

    @Override
    CloudResourceManager buildCloudResourceManager(HttpTransport httpTransport,
                                                   JsonFactory jsonFactory,
                                                   GoogleCredential credential,
                                                   String service) {
      return cloudResourceManager;
    }
  }

  @Before
  public void setUp() throws Exception {
    when(projectsList.execute()).thenReturn(new ListProjectsResponse());
    when(cloudResourceManager.projects().list()).thenReturn(projectsList);
    googleIdTokenValidatorFactory = new TestGoogleIdTokenValidatorFactory();
  }
  
  @Test
  public void shouldCreateGoogleIdTokenValidator() throws IOException {
    assertThat(googleIdTokenValidatorFactory.apply(ImmutableSet.of(), "test"), is(notNullValue()));
    verify(projectsList).execute();
  }
  
  @Test
  public void shouldFailToCreateGoogleIdTokenValidator() throws IOException {
    final IOException exception = new IOException();
    when(projectsList.execute()).thenThrow(exception);
    expectedException.expect(RuntimeException.class);
    expectedException.expectCause(is(exception));

    googleIdTokenValidatorFactory.apply(ImmutableSet.of(), "test");
  }
  
  @Test
  public void shouldBuildGoogleIdTokenVerifier() {
    final GoogleIdTokenVerifier googleIdTokenVerifier = new DefaultGoogleIdTokenValidatorFactory()
        .buildGoogleIdTokenVerifier(httpTransport, jsonFactory);
    assertThat(googleIdTokenVerifier.getTransport(), is(httpTransport));
    assertThat(googleIdTokenVerifier.getJsonFactory(), is(jsonFactory));
  }

  @Test
  public void shouldBuildIam() {
    final Iam iam = new DefaultGoogleIdTokenValidatorFactory()
        .buildIam(httpTransport, jsonFactory, googleCredential, "test");
    assertThat(iam.getRequestFactory().getTransport(), is(httpTransport));
    assertThat(iam.getJsonFactory(), is(jsonFactory));
    assertThat(iam.getRequestFactory().getInitializer(), is(googleCredential));
    assertThat(iam.getApplicationName(), is("test"));
  }
  
  @Test
  public void shouldBuildCloudResourceManager() {
    final CloudResourceManager cloudResourceManager = new DefaultGoogleIdTokenValidatorFactory()
        .buildCloudResourceManager(httpTransport, jsonFactory, googleCredential, "test");
    assertThat(cloudResourceManager.getRequestFactory().getTransport(), is(httpTransport));
    assertThat(cloudResourceManager.getJsonFactory(), is(jsonFactory));
    assertThat(cloudResourceManager.getRequestFactory().getInitializer(), is(googleCredential));
    assertThat(cloudResourceManager.getApplicationName(), is("test"));
  }
}
