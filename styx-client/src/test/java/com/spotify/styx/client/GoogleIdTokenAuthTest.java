/*-
 * -\-\-
 * Spotify Styx API Client
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

package com.spotify.styx.client;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;;
import static org.junit.Assert.fail;

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.api.client.googleapis.util.Utils;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nullable;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class GoogleIdTokenAuthTest {

  private static final String TEST_ID_TOKEN = "foobar";

  private static final GoogleIdTokenVerifier VERIFIER = new GoogleIdTokenVerifier(
      Utils.getDefaultTransport(), Utils.getDefaultJsonFactory());

  @Nullable private GoogleCredentials credentials;

  private final MockWebServer metadataServer = new MockWebServer();

  @Rule public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Before
  public void setUp() throws IOException {
    try {
      credentials = GoogleCredentials.getApplicationDefault();
    } catch (IOException e) {
      // Require credentials to be available in CI environment
      if (System.getenv("CI") != null) {
        throw e;
      }
      credentials = null;
    }
  }

  @After
  public void tearDown() throws Exception {
    metadataServer.close();
  }

  @Test
  public void testGCEMetadataToken() throws IOException, GeneralSecurityException, InterruptedException {
    metadataServer.setDispatcher(new Dispatcher() {
      @Override
      public MockResponse dispatch(RecordedRequest request) {
        final MockResponse response = new MockResponse()
            .setHeader("Metadata-Flavor", "Google");
        if (request.getPath().equals("/")) {
          return response;
        }
        if (!"Google".equals(request.getHeader("Metadata-Flavor"))) {
          return response.setResponseCode(404);
        }
        if (request.getPath().startsWith("/computeMetadata/v1/instance/service-accounts/default/identity?")) {
          return response
              .setBody(TEST_ID_TOKEN)
              .setHeader("Metadata-Flavor", "Google");
        }
        return response.setResponseCode(404);
      }
    });
    metadataServer.start();
    environmentVariables.set("GCE_METADATA_HOST", "127.0.0.1:" + metadataServer.getPort());
    final GoogleIdTokenAuth idTokenAuth = GoogleIdTokenAuth.of(ComputeEngineCredentials.create());
    final Optional<String> token = idTokenAuth.getToken("http://styx.foo.bar");
    assertThat(token, is(Optional.of(TEST_ID_TOKEN)));
    final RecordedRequest tokenRequest = metadataServer.takeRequest();
    assertThat(tokenRequest.getPath(), is("/computeMetadata/v1/instance/service-accounts/default/identity"
                                          + "?audience=http://styx.foo.bar&format=full"));
    assertThat(tokenRequest.getHeader("Metadata-Flavor"), is("Google"));
  }

  @Test
  public void testImpersonatedCredentials() throws IOException, GeneralSecurityException {
    Assume.assumeNotNull(credentials);
    final ImpersonatedCredentials impersonatedCredentials = ImpersonatedCredentials.newBuilder()
        .setScopes(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"))
        .setSourceCredentials(credentials)
        .setTargetPrincipal("styx-test-user@styx-oss-test.iam.gserviceaccount.com")
        .setLifetime(300)
        .setDelegates(ImmutableList.of())
        .build();
    assertThat(canAcquireIdToken(impersonatedCredentials), is(true));
  }

  @Test
  public void testServiceAccountCredentials() throws IOException, GeneralSecurityException {
    Assume.assumeNotNull(credentials);
    final GoogleCredentials serviceAccountCredentials;
    final URI keyUri = URI.create("gs://styx-oss-test/styx-test-user.json");
    try (InputStream is = Files.newInputStream(Paths.get(keyUri))) {
      serviceAccountCredentials = GoogleCredentials.fromStream(is);
    }
    assertThat(canAcquireIdToken(serviceAccountCredentials), is(true));
  }

  @Test
  public void testServiceAccountWithoutTokenCreatorRoleOnSelfFails() throws GeneralSecurityException {
    Assume.assumeNotNull(credentials);
    final String serviceAccount = "styx-test-unusable-user@styx-oss-test.iam.gserviceaccount.com";
    final ImpersonatedCredentials serviceAccountCredentials = ImpersonatedCredentials.newBuilder()
        .setScopes(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"))
        .setSourceCredentials(credentials)
        .setTargetPrincipal(serviceAccount)
        .setLifetime(300)
        .setDelegates(ImmutableList.of())
        .build();
    final GoogleIdTokenAuth idTokenAuth = GoogleIdTokenAuth.of(serviceAccountCredentials);
    try {
      idTokenAuth.getToken("http://styx.foo.bar");
      fail();
    } catch (IOException e) {
      assertThat(e.getMessage(), is("Unable to get ID token, "
                                    + "missing Service Account Token Creator role for self on "
                                    + serviceAccount + " or IAM Service Account Credentials API "
                                    + "not enabled?"));
    }
  }

  @Test
  public void testServiceAccountCredentialsWithAccessToken() throws IOException, GeneralSecurityException {
    Assume.assumeNotNull(credentials);
    final GoogleCredentials serviceAccountCredentials;
    final URI keyUri = URI.create("gs://styx-oss-test/styx-test-user.json");
    try (InputStream is = Files.newInputStream(Paths.get(keyUri))) {
      serviceAccountCredentials = GoogleCredentials.fromStream(is)
          .createScoped(ImmutableList.of(
              "https://www.googleapis.com/auth/cloud-platform",
              "https://www.googleapis.com/auth/userinfo.email"));
    }
    serviceAccountCredentials.refresh();
    final GoogleCredentials accessTokenCredentials = GoogleCredentials.newBuilder()
        .setAccessToken(serviceAccountCredentials.getAccessToken())
        .build();
    assertThat(canAcquireIdToken(accessTokenCredentials), is(true));
  }

  @Test
  public void testServiceAccountCredentialsWithAccessTokenFailsIfMissingEmailScope() throws IOException,
                                                                                            GeneralSecurityException {
    Assume.assumeNotNull(credentials);
    final GoogleCredentials serviceAccountCredentials;
    final URI keyUri = URI.create("gs://styx-oss-test/styx-test-user.json");
    try (InputStream is = Files.newInputStream(Paths.get(keyUri))) {
      serviceAccountCredentials = GoogleCredentials.fromStream(is)
          .createScoped(ImmutableList.of(
              "https://www.googleapis.com/auth/cloud-platform"));
    }
    serviceAccountCredentials.refresh();
    final GoogleCredentials accessTokenCredentials = GoogleCredentials.newBuilder()
        .setAccessToken(serviceAccountCredentials.getAccessToken())
        .build();
    final GoogleIdTokenAuth idTokenAuth = GoogleIdTokenAuth.of(accessTokenCredentials);
    try {
      idTokenAuth.getToken("http://styx.foo.bar");
      fail();
    } catch (IOException e) {
      assertThat(e.getMessage(), is("Unable to look up principal email, credentials missing email scope?"));
    }
  }

  @Test
  public void testUserCredentialsWithAccessTokenFails() throws IOException,
                                                               GeneralSecurityException {
    Assume.assumeThat(Objects.requireNonNull(credentials), is(instanceOf(UserCredentials.class)));
    credentials.refresh();
    final GoogleCredentials accessTokenCredentials = GoogleCredentials.newBuilder()
        .setAccessToken(credentials.getAccessToken())
        .build();
    final GoogleIdTokenAuth idTokenAuth = GoogleIdTokenAuth.of(accessTokenCredentials);
    try {
      idTokenAuth.getToken("http://styx.foo.bar");
      fail();
    } catch (IOException e) {
      assertThat(e.getMessage(), startsWith("Principal is not a service account, unable to acquire id token:"));
    }
  }

  @Test
  public void testUserCredentials() throws IOException, GeneralSecurityException {
    Assume.assumeThat(credentials, is(instanceOf(UserCredentials.class)));
    assertThat(canAcquireIdToken(credentials), is(true));
  }

  @Test
  public void testMockUserCredentials() throws IOException, GeneralSecurityException, InterruptedException {
    final MockResponse tokenResponse = new MockResponse()
        .setBody(Utils.getDefaultJsonFactory().toString(ImmutableMap.of("id_token", "test-id-token")));
    metadataServer.enqueue(tokenResponse);
    metadataServer.start();

    final AccessToken accessToken = new AccessToken("test-access-token",
        Date.from(Instant.now().plus(Duration.ofDays(1))));
    final GoogleCredentials credentials = UserCredentials.newBuilder()
        .setTokenServerUri(URI.create("http://localhost:" + metadataServer.getPort() + "/get-test-token"))
        .setAccessToken(accessToken)
        .setRefreshToken("user-refresh-token")
        .setClientId("user-id")
        .setClientSecret("user-secret")
        .build();
    Assume.assumeThat(credentials, is(instanceOf(UserCredentials.class)));
    final GoogleIdTokenAuth idTokenAuth = GoogleIdTokenAuth.of(credentials);
    final Optional<String> token = idTokenAuth.getToken("http://styx.foo.bar");
    assertThat(token, is(Optional.of("test-id-token")));

    final RecordedRequest recordedRequest = metadataServer.takeRequest();
    final Map<String, String> requestBody = Splitter.on('&').withKeyValueSeparator('=')
        .split(recordedRequest.getBody().readUtf8());
    assertThat(requestBody, is(ImmutableMap.of(
        "grant_type", "refresh_token",
        "refresh_token", "user-refresh-token",
        "client_id", "user-id",
        "client_secret", "user-secret")));
    assertThat(recordedRequest.getPath(), is("/get-test-token"));
    assertThat(recordedRequest.getHeader("Authorization"), is("Bearer test-access-token"));
  }

  @Test
  public void testDefaultCredentials() throws IOException, GeneralSecurityException {
    final GoogleIdTokenAuth idTokenAuth = GoogleIdTokenAuth.ofDefaultCredential();
    final Optional<String> token = idTokenAuth.getToken("http://styx.foo.bar");
    if (credentials == null) {
      assertThat(token, is(Optional.empty()));
    } else {
      final GoogleIdToken verifiedToken = VERIFIER.verify(token.orElseThrow());
      assertThat(verifiedToken, is(notNullValue()));
    }
  }

  @Test
  public void testNoCredentials() throws IOException, GeneralSecurityException {
    final GoogleIdTokenAuth idTokenAuth = GoogleIdTokenAuth.of(Optional.empty());
    final Optional<String> token = idTokenAuth.getToken("http://styx.foo.bar");
    assertThat(token, is(Optional.empty()));
  }

  private static boolean canAcquireIdToken(GoogleCredentials credentials)
      throws IOException, GeneralSecurityException {
    final GoogleIdTokenAuth idTokenAuth = GoogleIdTokenAuth.of(credentials);
    final String targetAudience = "http://styx.foo.bar";
    final Optional<String> token = idTokenAuth.getToken(targetAudience);
    final GoogleIdToken verifiedToken = VERIFIER.verify(token.orElseThrow());
    assertThat(verifiedToken, is(notNullValue()));
    if (!(credentials instanceof UserCredentials)) {
      // TODO: can we procure user id tokens with the styx service audience?
      assertThat(verifiedToken.verifyAudience(ImmutableList.of(targetAudience)), is(true));
    }
    return true;
  }
}
