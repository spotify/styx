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
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import com.google.api.client.util.Lists;
import com.google.common.collect.ImmutableMap;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.entity.EntityMiddleware;
import com.spotify.apollo.entity.JacksonEntityCodec;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.Route;
import com.spotify.styx.api.RunStateDataPayload.RunStateData;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer.ServiceAccountUsageAuthorizationResult;
import com.spotify.styx.api.util.InvalidParametersException;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.RunState;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import okio.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * API endpoints for the retrieving events and active states
 */
public class StatusResource {

  private static final Logger log = LoggerFactory.getLogger(StatusResource.class);

  static final String BASE = "/status";

  private final Storage storage;
  private final ServiceAccountUsageAuthorizer accountUsageAuthorizer;

  public StatusResource(Storage storage, ServiceAccountUsageAuthorizer accountUsageAuthorizer) {
    this.storage = requireNonNull(storage);
    this.accountUsageAuthorizer = requireNonNull(accountUsageAuthorizer);
  }

  public Stream<Route<AsyncHandler<Response<ByteString>>>> routes() {
    final EntityMiddleware em =
        EntityMiddleware.forCodec(JacksonEntityCodec.forMapper(Json.OBJECT_MAPPER));

    final List<Route<AsyncHandler<Response<ByteString>>>> routes = Stream.of(
        Route.with(
            em.serializerResponse(RunStateDataPayload.class),
            "GET", BASE + "/activeStates",
            this::activeStates),
        Route.with(
            em.serializerDirect(EventsPayload.class),
            "GET", BASE + "/events/<cid>/<wfid>/<iid>",
            rc -> eventsForWorkflowInstance(arg("cid", rc), arg("wfid", rc), arg("iid", rc))),
        Route.with(
            em.response(TestServiceAccountUsageAuthorizationRequest.class,
                TestServiceAccountUsageAuthorizationResponse.class),
            "POST", BASE + "/testServiceAccountUsageAuthorization",
            rc -> this::testServiceAccountUsageAuthorization)
    )

        .map(r -> r.withMiddleware(Middleware::syncToAsync))
        .collect(toList());

    return Api.prefixRoutes(routes, V3);
  }

  private Response<TestServiceAccountUsageAuthorizationResponse> testServiceAccountUsageAuthorization(
      TestServiceAccountUsageAuthorizationRequest request) {
    final ServiceAccountUsageAuthorizationResult result =
        accountUsageAuthorizer.checkServiceAccountUsageAuthorization(request.serviceAccount(), request.principal());

    result.errorResponse().ifPresent(e -> { throw new ResponseException(e); });

    final TestServiceAccountUsageAuthorizationResponse response =
        new TestServiceAccountUsageAuthorizationResponseBuilder()
            .authorized(result.authorized())
            .blacklisted(result.blacklisted())
            .serviceAccount(request.serviceAccount())
            .principal(request.principal())
            .message(result.message())
            .build();

    return Response.forPayload(response);
  }

  private static String arg(String name, RequestContext rc) {
    return rc.pathArgs().get(name);
  }

  private Response<RunStateDataPayload> activeStates(RequestContext requestContext) {
    final Optional<String> componentOpt = requestContext.request().parameter("component");
    final Optional<String> workflowOpt = requestContext.request().parameter("workflow");
    final Optional<String> componentsOpt = requestContext.request().parameter("components");
    final Map<WorkflowInstance, RunState> activeStates;

    final List<RunStateData> runStates = Lists.newArrayList();
    try {
      activeStates = componentsOpt.isPresent() ? getActiveStates(componentsOpt.get()):
                       getActiveStates(componentOpt, workflowOpt);

    } catch (InvalidParametersException e) {
      return Response.forStatus(Status.BAD_REQUEST.withReasonPhrase(e.getMessage()));
    } catch (IOException e) {
      var errorMsg = "Could not read Active states: " + e;
      log.error(errorMsg);
      return Response.forStatus(Status.INTERNAL_SERVER_ERROR.withReasonPhrase(errorMsg));
    }
    runStates.addAll(
        activeStates.values().stream().map(this::runStateToRunStateData).collect(toList()));

    return Response.forPayload(RunStateDataPayload.create(runStates));
  }

  private Map<WorkflowInstance, RunState> getActiveStates(String componentsStr) throws IOException {
    final List<String> components = Arrays.asList(componentsStr.split(","));
    final ImmutableMap.Builder<WorkflowInstance, RunState> mapBuilder = ImmutableMap.builder();

    final List<Optional<IOException>> exceptions = components.parallelStream().map( componentId -> {
      Optional<IOException> exception = Optional.empty();
      try{
        final Map<WorkflowInstance, RunState> stateMap = storage.readActiveStates(componentId);
        for (WorkflowInstance instance : stateMap.keySet()) {
          mapBuilder.put(instance, stateMap.get(instance));
        }
      } catch (IOException e) {
        exception = Optional.of(e);
      }
      return exception;
      }).filter(Optional::isPresent).collect(toList());
    if (!exceptions.isEmpty()) {
      throw exceptions.get(0).orElseThrow();
    }

    return mapBuilder.build();
  }

  private Map<WorkflowInstance, RunState> getActiveStates(Optional<String> componentOpt, Optional<String> workflowOpt)
      throws IOException {
    if (workflowOpt.isPresent()) {
      if (componentOpt.isPresent()){
        return storage.readActiveStates(componentOpt.get(), workflowOpt.get());
      } else {
        throw new InvalidParametersException("No component id specified!");
      }
    } else if(componentOpt.isPresent()) {
      return storage.readActiveStates(componentOpt.get());
    } else {
      return storage.readActiveStates();
    }
  }

  private RunStateData runStateToRunStateData(RunState state) {
    return RunStateData.newBuilder()
        .workflowInstance(state.workflowInstance())
        .state(state.state().name())
        .stateData(state.data())
        .latestTimestamp(state.timestamp())
        .build();
  }

  private EventsPayload eventsForWorkflowInstance(String cid, String eid, String iid) {
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
      throw new RuntimeException(e);
    }
  }
}
