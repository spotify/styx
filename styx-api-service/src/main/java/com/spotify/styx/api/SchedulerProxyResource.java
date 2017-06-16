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

import static com.spotify.styx.api.Api.Version.V2;

import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Route;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import okio.ByteString;

/**
 * API endpoints for scheduler
 */
public class SchedulerProxyResource {

  static final String BASE = "/scheduler";
  private static final String SCHEDULER_BASE_PATH = "/api/v0";

  private final String schedulerServiceBaseUrl;

  public SchedulerProxyResource(String schedulerServiceBaseUrl) {
    this.schedulerServiceBaseUrl = Objects.requireNonNull(schedulerServiceBaseUrl);
  }

  public Stream<Route<AsyncHandler<Response<ByteString>>>> routes() {
    final List<Route<AsyncHandler<Response<ByteString>>>> schedulerProxies = Arrays.asList(
        Route.async(
            "GET", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + rc.pathArgs().get("endpoint"), rc)),
        Route.async(
            "POST", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + rc.pathArgs().get("endpoint"), rc)),
        Route.async(
            "DELETE", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + rc.pathArgs().get("endpoint"), rc)),
        Route.async(
            "PATCH", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + rc.pathArgs().get("endpoint"), rc)),
        Route.async(
            "PUT", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + rc.pathArgs().get("endpoint"), rc))
    );

    return Api.prefixRoutes(schedulerProxies, V2);
  }

  private CompletionStage<Response<ByteString>> proxyToScheduler(String path, RequestContext rc) {
    return rc.requestScopedClient()
        .send(rc.request().withUri(schedulerServiceBaseUrl + SCHEDULER_BASE_PATH + path));
  }
}
