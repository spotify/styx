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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.spotify.apollo.Request;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.entity.EntityMiddleware;
import com.spotify.apollo.entity.JacksonEntityCodec;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.Route;
import com.spotify.styx.api.Api;
import com.spotify.styx.api.EventsPayload;
import com.spotify.styx.api.StatusResource;
import com.spotify.styx.model.deprecated.WorkflowInstance;
import com.spotify.styx.serialization.Json;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import okio.ByteString;

/**
 * API endpoints for the cli
 */
@Deprecated
public class CliResource {

  static final String BASE = "/cli";
  private static final String SCHEDULER_BASE_PATH = "/api/v0";
  private static final String EVENTS_PATH = "/events";
  private static final String TRIGGER_PATH = "/trigger";

  private final StatusResource statusResource;
  private final String schedulerServiceBaseUrl;

  public CliResource(StatusResource statusResource, String schedulerServiceBaseUrl) {
    this.statusResource = Objects.requireNonNull(statusResource);
    this.schedulerServiceBaseUrl = Objects.requireNonNull(schedulerServiceBaseUrl);
  }

  public Stream<Route<AsyncHandler<Response<ByteString>>>> routes() {
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

    final List<Route<AsyncHandler<Response<ByteString>>>> schedulerProxies = Stream.of(
        Route.async(
            "POST", BASE + EVENTS_PATH,
            this::proxyEvent),
        Route.with(
            em.asyncResponse(WorkflowInstance.class),
            "POST", BASE + TRIGGER_PATH,
            rc -> workflowInstance -> proxyTrigger(workflowInstance, rc)))
        .collect(toList());

    return cat(
        Api.prefixRoutes(routes, V0, V1),
        Api.prefixRoutes(schedulerProxies, V0, V1)
    );
  }

  private CompletionStage<Response<ByteString>> proxyEvent(RequestContext rc) {
    return rc.requestScopedClient()
        .send(rc.request().withUri(schedulerServiceBaseUrl + SCHEDULER_BASE_PATH + EVENTS_PATH));
  }


  private CompletionStage<Response<WorkflowInstance>> proxyTrigger(
      WorkflowInstance workflowInstance, RequestContext rc) {
    final ByteString payload;
    try {
      payload = Json.serialize(WorkflowInstance.create(workflowInstance));
    } catch (JsonProcessingException e) {
      return CompletableFuture.completedFuture(
          Response.forStatus(Status.BAD_REQUEST.withReasonPhrase("Bad json payload")));
    }
    final Request request = rc.request()
        .withUri(schedulerServiceBaseUrl + SCHEDULER_BASE_PATH + TRIGGER_PATH)
        .withPayload(payload);
    return rc.requestScopedClient().send(request).thenApply(response ->
        response.withPayload(response.payload().map(p -> {
          try {
            return WorkflowInstance.create(
                Json.deserialize(p, com.spotify.styx.model.WorkflowInstance.class));
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }).orElse(null)));
  }

  private static String arg(String name, RequestContext rc) {
    return rc.pathArgs().get(name);
  }

  private RunStateDataPayload activeStates(RequestContext requestContext) {
    return RunStateDataPayload.create(statusResource.activeStates(requestContext));
  }

  private EventsPayload eventsForWorkflowInstance(String cid, String eid, String iid) {
    return statusResource.eventsForWorkflowInstance(cid, eid, iid);
  }
}
