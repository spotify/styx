/*-
 * -\-\-
 * Spotify Styx API Client
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

import com.google.auth.oauth2.GoogleCredentials;

/**
 * Factory to get a StyxClient implementation.
 *
 * <p>Using ImpersonatedCredentials:</p>
 * <code>
 *   ImpersonatedCredentials credentials = ImpersonatedCredentials.newBuilder()
 *         .setScopes(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"))
 *         .setSourceCredentials(GoogleCredentials.getApplicationDefault())
 *         .setTargetPrincipal("target-service-account@example.iam.gserviceaccount.com")
 *         .setLifetime(300)
 *         .setDelegates(ImmutableList.of())
 *         .build();
 *   StyxClient client = StyxClientFactory.create("https://styx.example.com", credentials)
 * </code>
 */
public class StyxClientFactory {

  private StyxClientFactory() {
    throw new UnsupportedOperationException();
  }

  public static StyxClient create(String apiHost) {
    return StyxOkHttpClient.create(apiHost);
  }

  public static StyxClient create(String apiHost, GoogleCredentials credentials) {
    return StyxOkHttpClient.create(apiHost, credentials);
  }

  public static StyxStatusClient createStatusClient(String apiHost) {
    return StyxOkHttpClient.create(apiHost);
  }

  public static StyxBackfillClient createBackfillClient(String apiHost) {
    return StyxOkHttpClient.create(apiHost);
  }

  public static StyxSchedulerClient createSchedulerClient(String apiHost) {
    return StyxOkHttpClient.create(apiHost);
  }

  public static StyxResourceClient createResourceClient(String apiHost) {
    return StyxOkHttpClient.create(apiHost);
  }

  public static StyxWorkflowClient createWorkflowClient(String apiHost) {
    return StyxOkHttpClient.create(apiHost);
  }
}
