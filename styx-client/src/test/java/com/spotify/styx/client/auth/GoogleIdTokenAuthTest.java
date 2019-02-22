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

package com.spotify.styx.client.auth;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Optional;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class GoogleIdTokenAuthTest {

  private static final String TEST_ID_TOKEN = "foobar";

  @Rule public final MockWebServer metadataServer = new MockWebServer();
  @Rule public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Before
  public void setUp() throws Exception {
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
    environmentVariables.set("GCE_METADATA_HOST", "127.0.0.1:" + metadataServer.getPort());
  }

  @Test
  public void testGCEMetadataToken() throws IOException, GeneralSecurityException, InterruptedException {
    environmentVariables.set("GOOGLE_APPLICATION_CREDENTIALS", "");
    environmentVariables.set("CLOUDSDK_CONFIG", "/non/existent/path");
    final GoogleIdTokenAuth idTokenAuth = GoogleIdTokenAuth.ofDefaultCredential();
    assertThat(idTokenAuth.isGCE(), is(true));
    final Optional<String> token = idTokenAuth.getToken("http://styx.foo.bar");
    assertThat(token, is(Optional.of(TEST_ID_TOKEN)));
    final RecordedRequest detectionRequest = metadataServer.takeRequest();
    assertThat(detectionRequest.getPath(), is("/"));
    final RecordedRequest tokenRequest = metadataServer.takeRequest();
    assertThat(tokenRequest.getPath(), is("/computeMetadata/v1/instance/service-accounts/default/identity"
                                          + "?audience=http://styx.foo.bar&format=full"));
    assertThat(tokenRequest.getHeader("Metadata-Flavor"), is("Google"));
  }
}