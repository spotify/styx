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
import static java.util.stream.Collectors.toList;

import com.google.api.client.util.Lists;
import com.google.common.base.Throwables;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.entity.EntityMiddleware;
import com.spotify.apollo.entity.JacksonEntityCodec;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.Route;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.RunState;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import okio.ByteString;

/**
 * API endpoints for the retrieving events and active states
 */
public class StatusResource {

  static final String BASE = "/status";

  private final Storage storage;

  public StatusResource(Storage storage) {
    this.storage = Objects.requireNonNull(storage);
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
            "GET", BASE + "/events/<cid>/<wfid>/<iid>",
            rc -> eventsForWorkflowInstance(arg("cid", rc), arg("wfid", rc), arg("iid", rc))))

        .map(r -> r.withMiddleware(Middleware::syncToAsync))
        .collect(toList());

    return Api.prefixRoutes(routes, V3);
  }

  private static String arg(String name, RequestContext rc) {
    return rc.pathArgs().get(name);
  }

  public RunStateDataPayload activeStates(RequestContext requestContext) {
    final Optional<String> componentOpt = requestContext.request().parameter("component");

    final List<RunStateDataPayload.RunStateData> runStates = Lists.newArrayList();
    try {

      final Map<WorkflowInstance, RunState> activeStates = componentOpt.isPresent()
          ? storage.readActiveWorkflowInstances(componentOpt.get())
          : storage.readActiveWorkflowInstances();

      runStates.addAll(
          activeStates.values().stream().map(this::runStateToRunStateData).collect(toList()));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    return RunStateDataPayload.create(runStates);
  }

  private RunStateDataPayload.RunStateData runStateToRunStateData(RunState state) {
    return RunStateDataPayload.RunStateData.create(
        state.workflowInstance(),
        state.state().toString(),
        state.data()
    );
  }

  public EventsPayload eventsForWorkflowInstance(String cid, String eid, String iid) {
    final WorkflowId workflowId = WorkflowId.create(cid, eid);
    final WorkflowInstance workflowInstance = WorkflowInstance.create(workflowId, iid);

    try {
      final Set<SequenceEvent> sequenceEvents = storage.readEvents(workflowInstance);
      final List<EventsPayload.TimestampedEvent> timestampedEvents = sequenceEvents.stream()
          .map(sequenceEvent -> EventsPayload.TimestampedEvent.create(
              sequenceEvent.event(),
              sequenceEvent.timestamp()))
          .collect(toList());

      return EventsPayload.create(timestampedEvents);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
