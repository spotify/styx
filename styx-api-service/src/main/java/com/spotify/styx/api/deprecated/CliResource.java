/*-
 * -\-\-
 * Spotify Styx API Service
 * --
 * Copyright (C) 2016 Spotify AB
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

package com.spotify.styx.api.deprecated;

import static com.spotify.styx.api.Api.Version.V0;
import static com.spotify.styx.api.Api.Version.V1;
import static com.spotify.styx.util.StreamUtil.cat;
import static java.util.stream.Collectors.toList;

import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.entity.EntityMiddleware;
import com.spotify.apollo.entity.JacksonEntityCodec;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.Route;
import com.spotify.styx.api.Api;
import com.spotify.styx.api.EventsPayload;
import com.spotify.styx.api.SchedulerResource;
import com.spotify.styx.api.StatusResource;
import com.spotify.styx.serialization.Json;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import okio.ByteString;

/**
 * API endpoints for the cli
 */
@Deprecated
public class CliResource {

  static final String BASE = "/cli";
  private final StatusResource statusResource;
  private SchedulerResource schedulerResource;

  public CliResource(StatusResource statusResource, SchedulerResource schedulerResource) {
    this.statusResource = Objects.requireNonNull(statusResource);
    this.schedulerResource = Objects.requireNonNull(schedulerResource);
  }

  public Stream<? extends Route<? extends AsyncHandler<? extends Response<ByteString>>>> routes() {
    final EntityMiddleware em =
        EntityMiddleware.forCodec(JacksonEntityCodec.forMapper(Json.OBJECT_MAPPER));

    final List<Route<AsyncHandler<Response<ByteString>>>> routes = Stream.of(
        Route.with(
            em.serializerDirect(RunStateDataPayload.class),
            "GET", BASE + "/activeStates",
            this::activeStates),
        Route.with(
            em.serializerDirect(EventsPayload.class),
            "GET", BASE + "/events/<cid>/<eid>/<iid>",
            rc -> eventsForWorkflowInstance(arg("cid", rc), arg("eid", rc), arg("iid", rc))))

        .map(r -> r.withMiddleware(Middleware::syncToAsync))
        .collect(toList());

    final List<Route<AsyncHandler<Response<ByteString>>>> schedulerProxies = Arrays.asList(
        Route.async(
            "GET", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + arg("endpoint", rc), rc)),
        Route.async(
            "POST", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + arg("endpoint", rc), rc)),
        Route.async(
            "DELETE", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + arg("endpoint", rc), rc)),
        Route.async(
            "PATCH", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + arg("endpoint", rc), rc)),
        Route.async(
            "PUT", BASE + "/<endpoint:path>",
            rc -> proxyToScheduler("/" + arg("endpoint", rc), rc))
    );

    return cat(
        Api.prefixRoutes(routes, V0, V1),
        Api.prefixRoutes(schedulerProxies, V0, V1)
    );
  }

  private CompletionStage<Response<ByteString>> proxyToScheduler(String path, RequestContext rc) {
    return schedulerResource.proxyToScheduler(path, rc);
  }

  private static String arg(String name, RequestContext rc) {
    return rc.pathArgs().get(name);
  }

  private RunStateDataPayload activeStates(RequestContext requestContext) {
    return statusResource.activeStates(requestContext);
  }

  private EventsPayload eventsForWorkflowInstance(String cid, String eid, String iid) {
    return statusResource.eventsForWorkflowInstance(cid, eid, iid);
  }
}
