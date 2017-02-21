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

import static com.spotify.styx.api.Api.Version.V1;
import static com.spotify.styx.util.ParameterUtil.rangeOfInstants;
import static com.spotify.styx.util.ParameterUtil.toParameter;
import static com.spotify.styx.util.ParameterUtil.truncateInstant;
import static java.util.stream.Collectors.toList;

import com.google.common.base.Throwables;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.entity.EntityMiddleware;
import com.spotify.apollo.entity.JacksonEntityCodec;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.Route;
import com.spotify.styx.api.cli.RunStateDataPayload;
import com.spotify.styx.api.cli.RunStateDataPayload.RunStateData;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillBuilder;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.RandomGenerator;
import com.spotify.styx.util.ReplayEvents;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import okio.ByteString;

public final class BackfillResource {

  static final String BASE = "/backfills";

  private final Storage storage;

  public BackfillResource(Storage storage) {
    this.storage = Objects.requireNonNull(storage);
  }

  public Stream<? extends Route<? extends AsyncHandler<? extends Response<ByteString>>>> routes() {
    final EntityMiddleware em =
        EntityMiddleware.forCodec(JacksonEntityCodec.forMapper(Json.OBJECT_MAPPER));

    final List<Route<AsyncHandler<Response<ByteString>>>> routes = Stream.of(
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
            rc -> getBackfill(arg("bid", rc))),
        Route.with(
            em.serializerResponse(Void.class),
            "DELETE", BASE + "/<bid>",
            rc -> haltBackfill(arg("bid", rc))),
        Route.with(
            em.response(Backfill.class),
            "PUT", BASE + "/<bid>",
            rc -> payload -> updateBackfill(arg("bid", rc), payload))
    )
        .map(r -> r.withMiddleware(Middleware::syncToAsync))
        .collect(toList());

    return Api.prefixRoutes(routes, V1);
  }

  private BackfillsPayload getBackfills(RequestContext requestContext) {
    final Optional<String> componentOpt = requestContext.request().parameter("component");
    final Optional<String> workflowOpt = requestContext.request().parameter("workflow");
    final Optional<String> statusesFlagOpt = requestContext.request().parameter("status");

    Stream<Backfill> backfills;
    try {
      backfills = storage.backfills().stream();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    if (componentOpt.isPresent()) {
      final String component = componentOpt.get();
      // TODO: filter in datastore
      backfills = backfills
          .filter(backfill -> backfill.workflowId().componentId().equals(component));
    }
    if (workflowOpt.isPresent()) {
      final String workflow = workflowOpt.get();
      // TODO: filter in datastore
      backfills = backfills
          .filter(backfill -> backfill.workflowId().endpointId().equals(workflow));
    }

    final List<BackfillPayload> backfillPayloads = backfills
        .map(backfill -> BackfillPayload.create(
            backfill,
            "true".equals(statusesFlagOpt.orElse("false"))
            ? Optional.of(RunStateDataPayload.create(retrieveBackfillStatuses(backfill)))
            : Optional.empty()))
        .collect(toList());

    return BackfillsPayload.create(backfillPayloads);
  }

  private Response<BackfillPayload> getBackfill(String id) {
    try {
      Optional<Backfill> backfillOpt = storage.backfill(id);
      if (backfillOpt.isPresent()) {
        List<RunStateData> statuses = retrieveBackfillStatuses(backfillOpt.get());
        return Response.forPayload(BackfillPayload.create(
            backfillOpt.get(), Optional.of(RunStateDataPayload.create(statuses))));
      } else {
        return Response.forStatus(Status.NOT_FOUND);
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private Response<Void> haltBackfill(String id) {
    try {
      final Optional<Backfill> backfillOptional = storage.backfill(id);
      if (backfillOptional.isPresent()) {
        storage.storeBackfill(backfillOptional.get().builder().halted(true).build());
      } else {
        return Response.forStatus(Status.NOT_FOUND.withReasonPhrase("backfill not found"));
      }
    } catch (IOException e) {
      return Response.forStatus(
          Status.INTERNAL_SERVER_ERROR
              .withReasonPhrase("could not halt backfill: " + e.getMessage()));
    }
    return Response.ok();
  }

  private Response<Backfill> postBackfill(BackfillInput input) {
    final BackfillBuilder builder = Backfill.newBuilder();

    final String id = RandomGenerator.DEFAULT.generateUniqueId("backfill");
    final Partitioning partitioning;

    final WorkflowId workflowId = WorkflowId.create(input.component(), input.workflow());
    final Set<WorkflowInstance> activeWorkflowInstances;
    try {
      activeWorkflowInstances = storage.readActiveWorkflowInstances(input.component()).keySet();
      final Optional<Workflow> workflowOpt = storage.workflow(workflowId);
      if (!workflowOpt.isPresent()) {
        return Response.forStatus(Status.NOT_FOUND.withReasonPhrase("workflow not found"));
      }
      partitioning = workflowOpt.get().schedule().partitioning();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }

    if (truncateInstant(input.start(), partitioning) != input.start()) {
      return Response.forStatus(
          Status.BAD_REQUEST.withReasonPhrase("start parameter not aligned with partitioning"));
    }

    if (truncateInstant(input.end(), partitioning) != input.end()) {
      return Response.forStatus(
          Status.BAD_REQUEST.withReasonPhrase("end parameter not aligned with partitioning"));
    }

    final List<WorkflowInstance> alreadyActive =
        rangeOfInstants(input.start(), input.end(), partitioning).stream()
            .map(instant -> WorkflowInstance.create(workflowId, toParameter(partitioning, instant)))
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
        .partitioning(partitioning)
        .nextTrigger(input.start())
        .halted(false);

    final Backfill backfill = builder.build();

    try {
      storage.storeBackfill(backfill);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    return Response.forPayload(backfill);
  }

  private Response<Backfill> updateBackfill(String id, Backfill backfill) {
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

    final List<Instant> processedInstants = rangeOfInstants(
        backfill.start(), backfill.nextTrigger(), backfill.partitioning());
    processedStates = processedInstants.stream()
        .map(instant -> {
          final WorkflowInstance wfi = WorkflowInstance
              .create(backfill.workflowId(), toParameter(backfill.partitioning(), instant));
          Optional<RunState> restoredStateOpt = ReplayEvents.getBackfillRunState(
              wfi,
              storage,
              backfill.id());
          if (restoredStateOpt.isPresent()) {
            RunState state = restoredStateOpt.get();
            return RunStateData.create(state.workflowInstance(), state.state().name(), state.data());
          } else {
            return RunStateData.create(wfi, "UNKNOWN", StateData.zero());
          }
        })
        .collect(toList());

    final List<Instant> waitingInstants = rangeOfInstants(
        backfill.nextTrigger(), backfill.end(), backfill.partitioning());
    waitingStates = waitingInstants.stream()
        .map(instant -> {
          final WorkflowInstance wfi = WorkflowInstance.create(
              backfill.workflowId(), toParameter(backfill.partitioning(), instant));
          return RunStateData.create(wfi, "WAITING", StateData.zero());
        })
        .collect(toList());

    return Stream.concat(processedStates.stream(), waitingStates.stream()).collect(toList());
  }

  private static String arg(String name, RequestContext rc) {
    return rc.pathArgs().get(name);
  }
}
