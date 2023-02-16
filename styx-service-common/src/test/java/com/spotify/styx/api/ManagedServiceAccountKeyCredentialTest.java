/*-
 * -\-\-
 * Spotify Styx Service Common
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

package com.spotify.styx.api;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ManagedServiceAccountKeyCredentialTest {

  private static final String SERVICE_ACCOUNT = "styx-test-user@styx-oss-test.iam.gserviceaccount.com";

  private IamCredentialsClient iamCredentialsClient;

  @Before
  public void setUp() throws Exception {
    var defaultCredentials = GoogleCredentials.getApplicationDefault();

    var serviceCredentials = ImpersonatedCredentials.create(
        defaultCredentials, SERVICE_ACCOUNT,
        List.of(), List.of("https://www.googleapis.com/auth/cloud-platform"), 300);

    try {
      serviceCredentials.refreshAccessToken();
    } catch (IOException e) {
      // Do not run this test if we do not have permission to impersonate the test user.
      Assume.assumeNoException(e);
    }

    iamCredentialsClient = IamCredentialsClient.create();
  }

  @Test
  public void testRefreshToken() throws IOException {
    var credentials = new ManagedServiceAccountKeyCredential.Builder(iamCredentialsClient)
        .setServiceAccountId(SERVICE_ACCOUNT)
        .setServiceAccountUser(SERVICE_ACCOUNT)
        .setServiceAccountScopes(Set.of("https://www.googleapis.com/auth/cloud-platform"))
        .build();
    var token = credentials.refreshToken();
    assertThat(token, is(notNullValue()));
  }
}
