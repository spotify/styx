/*-
 * -\-\-
 * Spotify Styx API Service
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

package com.spotify.styx;

import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.hamcrest.future.CompletableFutureMatchers.stageWillCompleteWithValueThat;
import static com.spotify.styx.StyxApi.AUTHORIZATION_GSUITE_USER_CONFIG;
import static com.spotify.styx.StyxApi.AUTHORIZATION_REQUIRE_ALL_CONFIG;
import static com.spotify.styx.StyxApi.AUTHORIZATION_REQUIRE_WORKFLOWS;
import static com.spotify.styx.StyxApi.AUTHORIZATION_SERVICE_ACCOUNT_USER_ROLE_CONFIG;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.apollo.Request;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.test.ServiceHelper;
import com.spotify.styx.api.Authenticator;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer.AuthorizationPolicy;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.storage.Storage;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.security.PrivateKey;
import java.util.concurrent.CompletionStage;
import okio.ByteString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StyxApiTest {

  private static final String SERVICE_ACCOUNT_USER_ROLE = "organizations/3141592/roles/StyxWorkflowServiceAccountUser";
  private static final String GSUITE_USER = "gsuite-user@example.com";

  @Mock private PrivateKey privateKey;

  private GoogleCredential credential;

  @Before
  public void setUp() {
    credential = new GoogleCredential.Builder()
        .setServiceAccountPrivateKey(privateKey)
        .setServiceAccountId("styx@bar.iam.gserviceaccount.com")
        .build();
  }

  @Test
  public void shouldCreateServiceAccountUsageAuthorizerWithRole() {
    final Config config = ConfigFactory.parseMap(ImmutableMap.of(
        AUTHORIZATION_SERVICE_ACCOUNT_USER_ROLE_CONFIG, SERVICE_ACCOUNT_USER_ROLE,
        AUTHORIZATION_GSUITE_USER_CONFIG, GSUITE_USER));
    final ServiceAccountUsageAuthorizer authorizer = StyxApi.serviceAccountUsageAuthorizer(config, credential, "foo");
    assertThat(authorizer, is(instanceOf(ServiceAccountUsageAuthorizer.Impl.class)));
  }

  @Test
  public void shouldCreateNopServiceAccountUsageAuthorizer() {
    final Config config = ConfigFactory.parseMap(ImmutableMap.of());
    final ServiceAccountUsageAuthorizer authorizer = StyxApi.serviceAccountUsageAuthorizer(config, credential, "foo");
    assertThat(authorizer, is(ServiceAccountUsageAuthorizer.NOP));
  }

  @Test
  public void shouldCreateAllAuthorizationPolicy() {
    final Config config = ConfigFactory.parseMap(ImmutableMap.of(AUTHORIZATION_REQUIRE_ALL_CONFIG, "true"));
    final AuthorizationPolicy policy = StyxApi.authorizationPolicy(config);
    assertThat(policy, is(instanceOf(ServiceAccountUsageAuthorizer.AllAuthorizationPolicy.class)));
  }

  @Test
  public void shouldCreateWhitelistAuthorizationPolicy() {
    final Config config = ConfigFactory.parseMap(ImmutableMap.of(AUTHORIZATION_REQUIRE_WORKFLOWS,
        ImmutableList.of("foo#bar", "baz#quux")));
    final AuthorizationPolicy policy = StyxApi.authorizationPolicy(config);
    assertThat(policy, is(instanceOf(ServiceAccountUsageAuthorizer.WhitelistAuthorizationPolicy.class)));
  }

  @Test
  public void shouldCreateNoAuthorizationPolicy() {
    final AuthorizationPolicy policy = StyxApi.authorizationPolicy(ConfigFactory.empty());
    assertThat(policy, is(instanceOf(ServiceAccountUsageAuthorizer.NoAuthorizationPolicy.class)));
  }

  @Test
  public void shouldCreateAPingableService() throws Exception {
    StyxApi styxApi = StyxApi.newBuilder()
        .setAuthenticatorFactory(conf -> mock(Authenticator.class))
        .setServiceName(StyxApi.SERVICE_NAME)
        .setStatsFactory(env -> mock(Stats.class))
        .setStorageFactory((env, stats) -> mock(Storage.class))
        .setWorkflowConsumerFactory((env, stats) -> ((wfa, wfb) -> {}))
        .setCredential(credential)
        .build();
    ServiceHelper serviceHelper = ServiceHelper.create(styxApi, StyxApi.SERVICE_NAME)
                                      .startTimeoutSeconds(30);
    serviceHelper.start();

    CompletionStage<Response<ByteString>> response = serviceHelper.serviceClient().send(Request.forUri("/ping"));

    assertThat(response, stageWillCompleteWithValueThat(hasStatus(equalTo(Status.OK))));

    serviceHelper.close();
  }
}
