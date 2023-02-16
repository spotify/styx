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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;

import com.spotify.apollo.Request;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.test.ServiceHelper;
import com.spotify.styx.api.Authenticator;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.storage.Storage;
import okio.ByteString;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StyxApiTest {

  @Test
  public void shouldCreateAPingableService() throws Exception {
    StyxApi styxApi = StyxApi.newBuilder()
        .setAuthenticatorFactory(conf -> mock(Authenticator.class))
        .setServiceName(StyxApi.SERVICE_NAME)
        .setStatsFactory(env -> mock(Stats.class))
        .setStorageFactory((env, stats) -> mock(Storage.class))
        .setWorkflowConsumerFactory((env, stats) -> (wfa, wfb) -> {})
        .setServiceAccountUsageAuthorizerFactory((cfg, sn) -> mock(ServiceAccountUsageAuthorizer.class))
        .build();
    ServiceHelper serviceHelper = ServiceHelper.create(styxApi, StyxApi.SERVICE_NAME)
                                      .startTimeoutSeconds(30);
    serviceHelper.start();

    Response<ByteString> response = serviceHelper.serviceClient().send(Request.forUri("/ping"))
        .toCompletableFuture()
        .get();

    assertThat(response, hasStatus(equalTo(Status.OK)));

    serviceHelper.close();
  }
}
