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

package com.spotify.styx.api;

import static com.spotify.styx.model.EventSerializer.convertEventToPersistentEvent;
import static com.spotify.styx.util.ReplayEvents.replayActiveStates;
import static com.spotify.styx.util.StreamUtil.cat;

import com.google.api.client.util.Lists;
import com.google.common.base.Throwables;
import com.spotify.apollo.Client;
import com.spotify.apollo.Request;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.entity.EntityMiddleware;
import com.spotify.apollo.entity.JacksonEntityCodec;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.Route;
import com.spotify.styx.api.cli.ActiveStatesPayload;
import com.spotify.styx.api.cli.EventsPayload;
import com.spotify.styx.api.cli.EventsPayload.TimestampedPersistentEvent;
import com.spotify.styx.model.EventSerializer.PersistentEvent;
import com.spotify.styx.model.EventVisitor;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.storage.EventStorage;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import okio.ByteString;

/**
 * API endpoints for the cli
 */
public class CliResource {

  public static final String BASE = "/cli";
  public static final String SCHEDULER_BASE_PATH = "/api/v0";

  private final String schedulerServiceBaseUrl;
  private final EventStorage eventStorage;

  private final EventVisitor<Boolean> lastExecutionEventVisitor = new CliResource.LastExecutionEventVisitor();

  public CliResource(String schedulerServiceBaseUrl, EventStorage eventStorage) {
    this.schedulerServiceBaseUrl = Objects.requireNonNull(schedulerServiceBaseUrl);
    this.eventStorage = Objects.requireNonNull(eventStorage);
  }

  public Stream<? extends Route<? extends AsyncHandler<? extends Response<ByteString>>>> routes() {
    final EntityMiddleware em =
        EntityMiddleware.forCodec(JacksonEntityCodec.forMapper(Middlewares.OBJECT_MAPPER));

    final List<Route<AsyncHandler<Response<ByteString>>>> routes = Stream.of(
        Route.with(
            em.serializerDirect(ActiveStatesPayload.class),
            "GET", BASE + "/activeStates",
            this::activeStates),
        Route.with(
            em.serializerDirect(EventsPayload.class),
            "GET", BASE + "/events/<cid>/<eid>/<iid>",
            rc -> eventsForWorkflowInstance(arg("cid", rc), arg("eid", rc), arg("iid", rc))))

        .map(r -> r.withMiddleware(Middleware::syncToAsync))
        .collect(Collectors.toList());

    final List<Route<AsyncHandler<Response<ByteString>>>> proxies = Arrays.asList(
        Route.async(
            "POST", BASE + "/events",
            this::injectEventProxy),
        Route.async(
            "POST", BASE + "/trigger",
            this::triggerWorkflowInstanceProxy));

    return cat(
        routes.stream().map(r -> r.withPrefix(Api.Version.V0.prefix())),
        routes.stream().map(r -> r.withPrefix(Api.Version.V1.prefix())),
        proxies.stream().map(r -> r.withPrefix(Api.Version.V0.prefix())),
        proxies.stream().map(r -> r.withPrefix(Api.Version.V1.prefix()))
    );
  }

  private static String arg(String name, RequestContext rc) {
    return rc.pathArgs().get(name);
  }

  private ActiveStatesPayload activeStates(RequestContext requestContext) {
    final Optional<String> componentOpt = requestContext.request().parameter("component");

    final List<ActiveStatesPayload.ActiveState> runStates = Lists.newArrayList();
    try {

      final Map<WorkflowInstance, Long> activeStates = componentOpt.isPresent()
          ? eventStorage.readActiveWorkflowInstances(componentOpt.get())
          : eventStorage.readActiveWorkflowInstances();

      final Map<RunState, Long> map = replayActiveStates(activeStates, eventStorage, false);
      runStates.addAll(
          map.keySet().stream().map(this::runStateToActiveState).collect(Collectors.toList()));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    return ActiveStatesPayload.create(runStates);
  }

  private ActiveStatesPayload.ActiveState runStateToActiveState(RunState state) {
    return ActiveStatesPayload.ActiveState.create(
        state.workflowInstance(),
        state.state().toString(),
        state.data().executionId().orElse("<no execution id>"),
        getPreviousExecutionLastEvent(state)
    );
  }

  private Optional<PersistentEvent> getPreviousExecutionLastEvent(RunState state) {
    Optional<PersistentEvent> lastEvent;
    try {
      final SortedSet<SequenceEvent> sequenceEvents = eventStorage.readEvents(state.workflowInstance());
      final Optional<SequenceEvent> lastExecutionSequenceEvent = sequenceEvents
          .stream()
          .filter((sequenceEvent) -> sequenceEvent.event().accept(lastExecutionEventVisitor))
          .reduce((a, b) -> b);
      if (lastExecutionSequenceEvent.isPresent()) {
        lastEvent = Optional.of(convertEventToPersistentEvent(lastExecutionSequenceEvent.get().event()));
      } else {
        lastEvent = Optional.empty();
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return lastEvent;
  }

  private EventsPayload eventsForWorkflowInstance(String cid, String eid, String iid) {
    final WorkflowId workflowId = WorkflowId.create(cid, eid);
    final WorkflowInstance workflowInstance = WorkflowInstance.create(workflowId, iid);

    try {
      final Set<SequenceEvent> sequenceEvents = eventStorage.readEvents(workflowInstance);
      final List<TimestampedPersistentEvent> timestampedPersistentEvents = sequenceEvents.stream()
          .map(sequenceEvent -> TimestampedPersistentEvent.create(
              convertEventToPersistentEvent(sequenceEvent.event()),
              sequenceEvent.timestamp()))
          .collect(Collectors.toList());

      return EventsPayload.create(timestampedPersistentEvents);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private CompletionStage<Response<ByteString>> injectEventProxy(RequestContext requestContext) {
    final Client client = requestContext.requestScopedClient();
    final Request proxyRequest = requestContext.request()
        .withUri(schedulerServiceBaseUrl + SCHEDULER_BASE_PATH + "/events");

    return client.send(proxyRequest);
  }

  private CompletionStage<Response<ByteString>> triggerWorkflowInstanceProxy(RequestContext requestContext) {
    final Client client = requestContext.requestScopedClient();
    final Request proxyRequest = requestContext.request()
        .withUri(schedulerServiceBaseUrl + SCHEDULER_BASE_PATH + "/trigger");

    return client.send(proxyRequest);
  }

  private class LastExecutionEventVisitor implements EventVisitor<Boolean> {

    @Override
    public Boolean timeTrigger(WorkflowInstance workflowInstance) {
      return Boolean.FALSE;
    }

    @Override
    public Boolean triggerExecution(WorkflowInstance workflowInstance, String triggerId) {
      return Boolean.FALSE;
    }

    @Override
    public Boolean created(WorkflowInstance workflowInstance, String executionId, String dockerImage) {
      return Boolean.FALSE;
    }

    @Override
    public Boolean submit(WorkflowInstance workflowInstance, ExecutionDescription executionDescription) {
      return Boolean.FALSE;
    }

    @Override
    public Boolean submitted(WorkflowInstance workflowInstance, String executionId) {
      return Boolean.FALSE;
    }

    @Override
    public Boolean started(WorkflowInstance workflowInstance) {
      return Boolean.FALSE;
    }

    @Override
    public Boolean terminate(WorkflowInstance workflowInstance, int exitCode) {
      return Boolean.TRUE;
    }

    @Override
    public Boolean runError(WorkflowInstance workflowInstance, String message) {
      return Boolean.TRUE;
    }

    @Override
    public Boolean success(WorkflowInstance workflowInstance) {
      return Boolean.FALSE;
    }

    @Override
    public Boolean retryAfter(WorkflowInstance workflowInstance, long delayMillis) {
      return Boolean.FALSE;
    }

    @Override
    public Boolean retry(WorkflowInstance workflowInstance) {
      return Boolean.FALSE;
    }

    @Override
    public Boolean stop(WorkflowInstance workflowInstance) {
      return Boolean.FALSE;
    }

    @Override
    public Boolean timeout(WorkflowInstance workflowInstance) {
      return Boolean.FALSE;
    }

    @Override
    public Boolean halt(WorkflowInstance workflowInstance) {
      return Boolean.FALSE;
    }
  }
}
