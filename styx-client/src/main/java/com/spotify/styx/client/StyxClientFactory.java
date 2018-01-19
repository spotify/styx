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

import com.spotify.apollo.Client;

/**
 * Factory to get a StyxClient implementation.
 */
public class StyxClientFactory {

  private StyxClientFactory() {
    throw new UnsupportedOperationException();
  }

  public static StyxClient create(Client client, String apiHost) {
    return new StyxApolloClient(client, apiHost);
  }

  public static StyxStatusClient createStatusClient(Client client, String apiHost) {
    return new StyxApolloClient(client, apiHost);
  }

  public static StyxBackfillClient createBackfillClient(Client client, String apiHost) {
    return new StyxApolloClient(client, apiHost);
  }

  public static StyxSchedulerClient createSchedulerClient(Client client, String apiHost) {
    return new StyxApolloClient(client, apiHost);
  }

  public static StyxResourceClient createResourceClient(Client client, String apiHost) {
    return new StyxApolloClient(client, apiHost);
  }

  public static StyxWorkflowClient createWorkflowClient(Client client, String apiHost) {
    return new StyxApolloClient(client, apiHost);
  }
}
