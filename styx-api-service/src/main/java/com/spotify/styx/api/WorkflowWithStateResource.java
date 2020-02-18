/*-
 * -\-\-
 * Spotify Styx API Service
 * --
 * Copyright (C) 2017 Spotify AB
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

import static com.spotify.styx.api.Api.Version.V3;
import static com.spotify.styx.api.Middlewares.json;

import com.spotify.apollo.Response;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Route;
import com.spotify.styx.model.WorkflowWithState;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;
import okio.ByteString;

public class WorkflowWithStateResource {

  private static final String BASE = "/workflowsWithState";
  private final Storage storage;

  public WorkflowWithStateResource(final Storage storage) {
    this.storage = storage;
  }

  public Stream<Route<AsyncHandler<Response<ByteString>>>> routes() {
    final List<Route<AsyncHandler<Response<ByteString>>>> routes = Arrays.asList(
        Route.with(
            json(), "GET", BASE,
            rc -> workflowsWithState()));

    return Api.prefixRoutes(routes, V3);
  }

  private Response<Collection<WorkflowWithState>> workflowsWithState() {
    try {
      return Response.forPayload(storage.workflowsWithStates().values());
    } catch (IOException e) {
      throw new RuntimeException("Failed to get workflows with states", e);
    }
  }

}
