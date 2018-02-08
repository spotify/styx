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

import static com.spotify.apollo.StatusType.Family.SUCCESSFUL;
import static com.spotify.styx.api.Api.Version.V3;
import static com.spotify.styx.serialization.Json.serialize;
import static com.spotify.styx.util.ParameterUtil.rangeOfInstants;
import static com.spotify.styx.util.ParameterUtil.toParameter;
import static com.spotify.styx.util.StreamUtil.cat;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.spotify.apollo.Client;
import com.spotify.apollo.Request;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.entity.EntityMiddleware;
import com.spotify.apollo.entity.JacksonEntityCodec;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.Route;
import com.spotify.futures.CompletableFutures;
import com.spotify.styx.api.RunStateDataPayload.RunStateData;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillBuilder;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.RandomGenerator;
import com.spotify.styx.util.ReplayEvents;
import com.spotify.styx.util.TimeUtil;
import com.spotify.styx.util.WorkflowValidator;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import okio.ByteString;

public final class BackfillResource {

  static final String BASE = "/backfills";
  private static final String SCHEDULER_BASE_PATH = "/api/v0";
  private static final String UNKNOWN = "UNKNOWN";
  private static final String WAITING = "WAITING";

  private final Storage storage;
  private final String schedulerServiceBaseUrl;
  private final WorkflowValidator workflowValidator;

  public BackfillResource(String schedulerServiceBaseUrl, Storage storage,
      WorkflowValidator workflowValidator) {
    this.schedulerServiceBaseUrl = Objects.requireNonNull(schedulerServiceBaseUrl);
    this.storage = Objects.requireNonNull(storage);
    this.workflowValidator = Objects.requireNonNull(workflowValidator, "workflowValidator");
  }

  public Stream<Route<AsyncHandler<Response<ByteString>>>> routes() {
    final EntityMiddleware em =
        EntityMiddleware.forCodec(JacksonEntityCodec.forMapper(Json.OBJECT_MAPPER));

    final List<Route<AsyncHandler<Response<ByteString>>>> entityRoutes = Stream.of(
        Route.with(
            em.serializerDirect(BackfillsPayload.class),
            "GET", BASE,
            this::getBackfills),
        Route.with(
            em.response(BackfillInput.class, Backfill.class),
            "POST", BASE,
            rc -> this::postBackfill),
        Route.with(
            em.serializerResponse(BackfillPayload.class),
            "GET", BASE + "/<bid>",
            rc -> getBackfill(rc, rc.pathArgs().get("bid"))),
        Route.with(
            em.response(Backfill.class),
            "PUT", BASE + "/<bid>",
            rc -> payload -> updateBackfill(rc.pathArgs().get("bid"), payload))
    )
        .map(r -> r.withMiddleware(Middleware::syncToAsync))
        .collect(toList());

    final List<Route<AsyncHandler<Response<ByteString>>>> routes = Collections.singletonList(
        Route.async(
            "DELETE", BASE + "/<bid>",
            rc -> haltBackfill(rc.pathArgs().get("bid"), rc))
    );

    return cat(
        Api.prefixRoutes(entityRoutes, V3),
        Api.prefixRoutes(routes, V3)
    );
  }

  public BackfillsPayload getBackfills(RequestContext rc) {
    final Optional<String> componentOpt = rc.request().parameter("component");
    final Optional<String> workflowOpt = rc.request().parameter("workflow");
    final boolean includeStatuses = rc.request().parameter("status").orElse("false").equals("true");
    final boolean showAll = rc.request().parameter("showAll").orElse("false").equals("true");

    final Stream<Backfill> backfills;
    try {
      if (componentOpt.isPresent() && workflowOpt.isPresent()) {
        final WorkflowId workflowId = WorkflowId.create(componentOpt.get(), workflowOpt.get());
        backfills = storage.backfillsForWorkflowId(showAll, workflowId).stream();
      } else if (componentOpt.isPresent()) {
        final String component = componentOpt.get();
        backfills = storage.backfillsForComponent(showAll, component).stream();
      } else if (workflowOpt.isPresent()) {
        final String workflow = workflowOpt.get();
        backfills = storage.backfillsForWorkflow(showAll, workflow).stream();
      } else {
        backfills = storage.backfills(showAll).stream();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final List<BackfillPayload> backfillPayloads = backfills.parallel()
        .map(backfill -> BackfillPayload.create(
            backfill,
            includeStatuses
            ? Optional.of(RunStateDataPayload.create(retrieveBackfillStatuses(backfill)))
            : Optional.empty()))
        .collect(toList());

    return BackfillsPayload.create(backfillPayloads);
  }

  public Response<BackfillPayload> getBackfill(RequestContext rc, String id) {
    final boolean includeStatuses = rc.request().parameter("status").orElse("true").equals("true");
    try {
      final Optional<Backfill> backfillOpt = storage.backfill(id);
      if (!backfillOpt.isPresent()) {
        return Response.forStatus(Status.NOT_FOUND);
      }
      final Backfill backfill = backfillOpt.get();
      if (includeStatuses) {
        final List<RunStateData> statuses = retrieveBackfillStatuses(backfill);
        return Response.forPayload(BackfillPayload.create(
            backfill, Optional.of(RunStateDataPayload.create(statuses))));
      } else {
        return Response.forPayload(BackfillPayload.create(backfill, Optional.empty()));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String schedulerApiUrl(CharSequence... parts) {
    return schedulerServiceBaseUrl + SCHEDULER_BASE_PATH + "/" + String.join("/", parts);
  }

  public CompletionStage<Response<ByteString>> haltBackfill(String id, RequestContext rc) {
    try {
      final Optional<Backfill> backfillOptional = storage.backfill(id);
      if (backfillOptional.isPresent()) {
        final Backfill backfill = backfillOptional.get();
        storage.storeBackfill(backfill.builder().halted(true).build());
        return haltActiveBackfillInstances(backfill, rc.requestScopedClient());
      } else {
        return CompletableFuture.completedFuture(
            Response.forStatus(Status.NOT_FOUND.withReasonPhrase("backfill not found")));
      }
    } catch (IOException e) {
      return CompletableFuture.completedFuture(Response.forStatus(
          Status.INTERNAL_SERVER_ERROR
              .withReasonPhrase("could not halt backfill: " + e.getMessage())));
    }
  }

  private CompletionStage<Response<ByteString>> haltActiveBackfillInstances(Backfill backfill, Client client) {
    return CompletableFutures.allAsList(
        retrieveBackfillStatuses(backfill).stream()
            .filter(BackfillResource::isActiveState)
            .map(RunStateData::workflowInstance)
            .map(workflowInstance -> haltActiveBackfillInstance(workflowInstance, client))
            .collect(toList()))
        .handle((result, throwable) -> {
          if (throwable != null || result.contains(Boolean.FALSE)) {
            return Response.forStatus(
                Status.INTERNAL_SERVER_ERROR
                    .withReasonPhrase(
                        "some active instances cannot be halted, however no new ones will be triggered"));
          } else {
            return Response.ok();
          }
        });
  }

  private CompletionStage<Boolean> haltActiveBackfillInstance(WorkflowInstance workflowInstance,
                                                              Client client) {
    try {
      final ByteString payload = serialize(Event.halt(workflowInstance));
      final Request request = Request.forUri(schedulerApiUrl("events"), "POST")
          .withPayload(payload);
      return client.send(request)
          .thenApply(response -> response.status().family().equals(SUCCESSFUL));
    } catch (JsonProcessingException e) {
      return CompletableFuture.completedFuture(false);
    }
  }

  private static boolean isActiveState(RunStateData runStateData) {
    final String state  = runStateData.state();
    switch (state) {
      case UNKNOWN: return false;
      case WAITING: return false;
      default:      return !RunState.State.valueOf(state).isTerminal();
    }
  }

  public Response<Backfill> postBackfill(BackfillInput input) {
    final BackfillBuilder builder = Backfill.newBuilder();

    final String id = RandomGenerator.DEFAULT.generateUniqueId("backfill");
    final Schedule schedule;

    final WorkflowId workflowId = WorkflowId.create(input.component(), input.workflow());

    final Workflow workflow;
    final Set<WorkflowInstance> activeWorkflowInstances;
    try {
      activeWorkflowInstances = storage.readActiveWorkflowInstances(input.component()).keySet();
      final Optional<Workflow> workflowOpt = storage.workflow(workflowId);
      if (!workflowOpt.isPresent()) {
        return Response.forStatus(Status.NOT_FOUND.withReasonPhrase("workflow not found"));
      }
      workflow = workflowOpt.get();
      schedule = workflow.configuration().schedule();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    if (!workflow.configuration().dockerImage().isPresent()) {
      return Response.forStatus(Status.BAD_REQUEST.withReasonPhrase("Workflow is missing docker image"));
    }
    final Collection<String> errors = workflowValidator.validateWorkflow(workflow);
    if (!errors.isEmpty()) {
      return Response.forStatus(Status.BAD_REQUEST.withReasonPhrase("Invalid workflow configuration: "
          + String.join(", ", errors)));
    }

    if (!TimeUtil.isAligned(input.start(), schedule)) {
      return Response.forStatus(
          Status.BAD_REQUEST.withReasonPhrase("start parameter not aligned with schedule"));
    }

    if (!TimeUtil.isAligned(input.end(), schedule)) {
      return Response.forStatus(
          Status.BAD_REQUEST.withReasonPhrase("end parameter not aligned with schedule"));
    }

    final List<WorkflowInstance> alreadyActive =
        rangeOfInstants(input.start(), input.end(), schedule).stream()
            .map(instant -> WorkflowInstance.create(workflowId, toParameter(schedule, instant)))
            .filter(activeWorkflowInstances::contains)
            .collect(toList());

    if (!alreadyActive.isEmpty()) {
      final String alreadyActiveMessage = alreadyActive.stream()
          .map(WorkflowInstance::parameter)
          .collect(Collectors.joining(", "));
      return Response.forStatus(
          Status.CONFLICT
              .withReasonPhrase("these partitions are already active: " + alreadyActiveMessage));
    }

    builder
        .id(id)
        .allTriggered(false)
        .workflowId(workflowId)
        .concurrency(input.concurrency())
        .start(input.start())
        .end(input.end())
        .schedule(schedule)
        .nextTrigger(input.start())
        .description(input.description())
        .halted(false);

    final Backfill backfill = builder.build();

    try {
      storage.storeBackfill(backfill);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return Response.forPayload(backfill);
  }

  public Response<Backfill> updateBackfill(String id, Backfill backfill) {
    if (!backfill.id().equals(id)) {
      return Response.forStatus(
          Status.BAD_REQUEST.withReasonPhrase("ID of payload does not match ID in uri."));
    }

    try {
      storage.storeBackfill(backfill);
    } catch (IOException e) {
      return Response
          .forStatus(
              Status.INTERNAL_SERVER_ERROR.withReasonPhrase("Failed to store backfill."));
    }

    return Response.forStatus(Status.OK).withPayload(backfill);
  }

  private List<RunStateData> retrieveBackfillStatuses(Backfill backfill) {
    final List<RunStateData> processedStates;
    final List<RunStateData> waitingStates;

    Map<WorkflowInstance, RunState> activeWorkflowInstances;
    try {
      activeWorkflowInstances = storage.readActiveWorkflowInstances();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final List<Instant> processedInstants = rangeOfInstants(
        backfill.start(), backfill.nextTrigger(), backfill.schedule());
    processedStates = processedInstants.parallelStream()
        .map(instant -> {
          final WorkflowInstance wfi = WorkflowInstance
              .create(backfill.workflowId(), toParameter(backfill.schedule(), instant));
          Optional<RunState> restoredStateOpt = ReplayEvents.getBackfillRunState(
              wfi,
              activeWorkflowInstances,
              storage,
              backfill.id());
          if (restoredStateOpt.isPresent()) {
            RunState state = restoredStateOpt.get();
            return RunStateData.create(state.workflowInstance(), state.state().name(), state.data());
          } else {
            return RunStateData.create(wfi, UNKNOWN, StateData.zero());
          }
        })
        .collect(toList());

    final List<Instant> waitingInstants = rangeOfInstants(
        backfill.nextTrigger(), backfill.end(), backfill.schedule());
    waitingStates = waitingInstants.stream()
        .map(instant -> {
          final WorkflowInstance wfi = WorkflowInstance.create(
              backfill.workflowId(), toParameter(backfill.schedule(), instant));
          return RunStateData.create(wfi, WAITING, StateData.zero());
        })
        .collect(toList());

    return Stream.concat(processedStates.stream(), waitingStates.stream()).collect(toList());
  }
}
