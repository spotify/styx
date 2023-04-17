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

import com.google.common.collect.ImmutableSortedMap;
import com.spotify.apollo.Client;
import com.spotify.apollo.Request;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Route;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import okhttp3.HttpUrl;
import okio.ByteString;
import org.slf4j.MDC;

/**
 * API endpoints for scheduler
 */
public class SchedulerProxyResource {

  static final String BASE = "/scheduler";
  private static final String SCHEDULER_BASE_PATH = "/api/v0";

  private final String schedulerServiceBaseUrl;
  private final Client client;
  private final String schedulerHost;

  public SchedulerProxyResource(String schedulerServiceBaseUrl, Client client) {
    this.schedulerServiceBaseUrl = Objects.requireNonNull(schedulerServiceBaseUrl);
    this.schedulerHost = URI.create(schedulerServiceBaseUrl).getHost();
    this.client = Objects.requireNonNull(client, "client");
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

    return Api.prefixRoutes(schedulerProxies, V3);
  }

  private CompletionStage<Response<ByteString>> proxyToScheduler(String path, RequestContext rc) {
    final HttpUrl.Builder builder =
        Objects.requireNonNull(HttpUrl.parse(schedulerServiceBaseUrl + SCHEDULER_BASE_PATH + path))
            .newBuilder();
    ImmutableSortedMap.copyOf(rc.request().parameters()).forEach((name, values) ->
        values.forEach(value -> builder.addQueryParameter(name, value)));
    return client.send(withRequestId(rc.request().withUri(builder.build().toString())).withHeader("Host", schedulerHost));
  }

  private Request withRequestId(Request request) {
    if (request.headers().containsKey("X-Request-Id")) {
      return request;
    }
    // Unfortunately it is not possible for middleware to set headers on
    // incoming requests so we manually propagate it here from the MDC.
    final String requestId = MDC.get("request-id");
    if (requestId == null) {
      return request;
    }
    return request.withHeader("X-Request-Id", requestId);
  }
}
