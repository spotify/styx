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

import static com.spotify.styx.util.ClassEnforcer.assertNotInstantiable;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.security.PrivateKey;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ServiceAccountsTest {

  private static final String SERVICE_ACCOUNT = "styx-test-user@styx-oss-test.iam.gserviceaccount.com";

  @Rule public ExpectedException exception = ExpectedException.none();

  @Mock private GoogleCredentials sourceCredentials;
  @Mock private PrivateKey privateKey;

  @Test
  public void serviceAccountEmailImpersonatedCredentials() {
    var credentials = ImpersonatedCredentials.create(
        sourceCredentials, SERVICE_ACCOUNT, List.of(), List.of(), 300);
    assertThat(ServiceAccounts.serviceAccountEmail(credentials), is(SERVICE_ACCOUNT));
  }

  @Test
  public void serviceAccountEmailServiceAccountCredentials() {
    var credentials = ServiceAccountCredentials.newBuilder()
        .setClientEmail(SERVICE_ACCOUNT)
        .setPrivateKey(privateKey)
        .build();
    assertThat(ServiceAccounts.serviceAccountEmail(credentials), is(SERVICE_ACCOUNT));
  }

  @Test
  public void serviceAccountEmailShouldFailIfNotServiceAccount() {
    var credentials = GoogleCredentials.newBuilder().build();
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("Credential is not a service account");
    ServiceAccounts.serviceAccountEmail(credentials);
  }

  @Test
  public void testSingleton() throws ReflectiveOperationException {
    assertThat(assertNotInstantiable(ServiceAccounts.class), is(true));
  }
}
