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
import static com.spotify.styx.api.Middlewares.authedEntity;
import static com.spotify.styx.serialization.Json.serialize;
import static com.spotify.styx.util.CloserUtil.register;
import static com.spotify.styx.util.ParameterUtil.toParameter;
import static com.spotify.styx.util.TimeUtil.instantsInRange;
import static com.spotify.styx.util.TimeUtil.nextInstant;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import com.google.common.io.Closer;
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
import com.spotify.styx.api.Middlewares.AuthContext;
import com.spotify.styx.api.RunStateDataPayload.RunStateData;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillBuilder;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.EditableBackfillInput;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import com.spotify.styx.util.RandomGenerator;
import com.spotify.styx.util.ReplayEvents;
import com.spotify.styx.util.ResourceNotFoundException;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TimeUtil;
import com.spotify.styx.util.WorkflowValidator;
import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import okio.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BackfillResource implements Closeable {

  private static final Logger log = LoggerFactory.getLogger(WorkflowResource.class);

  static final String BASE = "/backfills";
  private static final String SCHEDULER_BASE_PATH = "/api/v0";
  private static final String UNKNOWN = "UNKNOWN";
  private static final String WAITING = "WAITING";
  private static final int CONCURRENCY = 64;

  private final Closer closer = Closer.create();

  private final Storage storage;
  private final String schedulerServiceBaseUrl;
  private final WorkflowValidator workflowValidator;
  private final Time time;

  private final ForkJoinPool forkJoinPool;
  private final WorkflowActionAuthorizer workflowActionAuthorizer;

  public BackfillResource(String schedulerServiceBaseUrl, Storage storage,
                          WorkflowValidator workflowValidator,
                          Time time,
                          WorkflowActionAuthorizer workflowActionAuthorizer) {
    this.schedulerServiceBaseUrl = Objects.requireNonNull(schedulerServiceBaseUrl, "schedulerServiceBaseUrl");
    this.storage = Objects.requireNonNull(storage, "storage");
    this.workflowValidator = Objects.requireNonNull(workflowValidator, "workflowValidator");
    this.time = Objects.requireNonNull(time, "time");
    this.workflowActionAuthorizer = Objects.requireNonNull(workflowActionAuthorizer,
        "workflowActionAuthorizer");
    this.forkJoinPool = register(closer, new ForkJoinPool(CONCURRENCY), "backfill-resource");
  }

  public Stream<Route<AsyncHandler<Response<ByteString>>>> routes(RequestAuthenticator authenticator) {
    final EntityMiddleware em =
        EntityMiddleware.forCodec(JacksonEntityCodec.forMapper(Json.OBJECT_MAPPER));

    final List<Route<AsyncHandler<Response<ByteString>>>> entityRoutes = Stream.of(
        Route.with(
            em.serializerDirect(BackfillsPayload.class),
            "GET", BASE,
            this::getBackfills),
        Route.with(
            authedEntity(authenticator, em.response(BackfillInput.class, Backfill.class)),
            "POST", BASE,
            ac -> rc -> payload -> postBackfill(ac, rc, payload)),
        Route.with(
            em.serializerResponse(BackfillPayload.class),
            "GET", BASE + "/<bid>",
            rc -> getBackfill(rc, rc.pathArgs().get("bid"))),
        Route.with(
            authedEntity(authenticator, em.response(EditableBackfillInput.class, Backfill.class)),
            "PUT", BASE + "/<bid>",
            ac -> rc -> payload -> updateBackfill(ac, rc.pathArgs().get("bid"), payload))
    )
        .map(r -> r.withMiddleware(Middleware::syncToAsync))
        .collect(toList());

    final List<Route<AsyncHandler<Response<ByteString>>>> routes = Collections.singletonList(
        Route.async(
            "DELETE", BASE + "/<bid>",
            rc -> haltBackfill(rc.pathArgs().get("bid"), rc, authenticator))
    );

    return Streams.concat(
        Api.prefixRoutes(entityRoutes, V3),
        Api.prefixRoutes(routes, V3)
    );
  }

  @Override
  public void close() throws IOException {
    closer.close();
  }

  private BackfillsPayload getBackfills(RequestContext rc) {
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

    final List<BackfillPayload> backfillPayloads = backfills
        .map(backfill -> forkJoinPool.submit(() -> BackfillPayload.create(
            backfill,
            includeStatuses
            ? Optional.of(RunStateDataPayload.create(retrieveBackfillStatuses(backfill)))
            : Optional.empty())))
        .collect(toList())
        .stream()
        .map(ForkJoinTask::join)
        .collect(toList());

    return BackfillsPayload.create(backfillPayloads);
  }

  private Response<BackfillPayload> getBackfill(RequestContext rc, String id) {
    final boolean includeStatuses = rc.request().parameter("status").orElse("true").equals("true");
    final Optional<Backfill> backfillOpt;
    try {
      backfillOpt = storage.backfill(id);
    } catch (IOException e) {
      final String message = String.format("Couldn't read backfill %s. ", id);
      log.warn(message, e);
      return Response.forStatus(Status.INTERNAL_SERVER_ERROR.withReasonPhrase("Error in internal storage"));
    }
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
  }

  private String schedulerApiUrl(CharSequence... parts) {
    return schedulerServiceBaseUrl + SCHEDULER_BASE_PATH + "/" + String.join("/", parts);
  }

  private CompletionStage<Response<ByteString>> haltBackfill(String id, RequestContext rc,
                                                             RequestAuthenticator authenticator) {
    final AuthContext authContext = authenticator.authenticate(rc.request());
    try {
      // TODO: run in transction
      final Optional<Backfill> backfillOptional = storage.backfill(id);
      if (backfillOptional.isPresent()) {
        final Backfill backfill = backfillOptional.get();
        workflowActionAuthorizer.authorizeWorkflowAction(authContext, backfill.workflowId());
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

  private Optional<String> validate(RequestContext rc,
                                    BackfillInput input,
                                    Workflow workflow) {
    if (!workflow.configuration().dockerImage().isPresent()) {
      return Optional.of("Workflow is missing docker image");
    }

    final Collection<String> errors = workflowValidator.validateWorkflow(workflow);
    if (!errors.isEmpty()) {
      return Optional.of("Invalid workflow configuration: " + String.join(", ", errors));
    }

    final Schedule schedule = workflow.configuration().schedule();

    if (!input.start().isBefore(input.end())) {
      return Optional.of("start must be before end");
    }

    if (!TimeUtil.isAligned(input.start(), schedule)) {
      return Optional.of("start parameter not aligned with schedule");
    }

    if (!TimeUtil.isAligned(input.end(), schedule)) {
      return Optional.of("end parameter not aligned with schedule");
    }

    final boolean allowFuture =
        Boolean.parseBoolean(rc.request().parameter("allowFuture").orElse("false"));
    if (!allowFuture &&
        (input.start().isAfter(time.get()) ||
         TimeUtil.previousInstant(input.end(), schedule).isAfter(time.get()))) {
      return Optional.of("Cannot backfill future partitions");
    }

    return Optional.empty();
  }

  private Response<Backfill> postBackfill(AuthContext ac, RequestContext rc,
      BackfillInput input) {

    try {
      return storage.runInTransactionWithRetries(tx -> postBackfill0(tx, ac, rc, input));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Response<Backfill> postBackfill0(StorageTransaction tx, AuthContext ac, RequestContext rc,
                                           BackfillInput input) throws IOException {
    var builder = Backfill.newBuilder();

    var id = RandomGenerator.DEFAULT.generateUniqueId("backfill");

    var workflowId = WorkflowId.create(input.component(), input.workflow());
    var workflow = tx.workflow(workflowId)
        .orElseThrow(() -> new ResponseException(
            Response.forStatus(Status.NOT_FOUND.withReasonPhrase("workflow not found"))));

    workflowActionAuthorizer.authorizeWorkflowAction(ac, workflow);

    var activeWorkflowInstances = storage.readActiveStates(input.component()).keySet();

    // Validate backfill & workflow
    var validationError = validate(rc, input, workflow);
    if (validationError.isPresent()) {
      return Response.forStatus(Status.BAD_REQUEST.withReasonPhrase(validationError.get()));
    }

    // Forbid backfill creation if there are already active instances in the backfill range
    // TODO: Does this make sense?
    final Schedule schedule = workflow.configuration().schedule();

    final List<Instant> instants = instantsInRange(input.start(), input.end(), schedule);
    final List<WorkflowInstance> alreadyActive =
        instants.stream()
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
        .nextTrigger(input.reverse()
                     ? Iterables.getLast(instants)
                     : input.start())
        .description(input.description())
        .reverse(input.reverse())
        .triggerParameters(input.triggerParameters())
        .halted(false);

    final Backfill backfill = builder.build();

    tx.store(backfill);

    return Response.forPayload(backfill);
  }

  private Response<Backfill> updateBackfill(AuthContext ac, String id,
      EditableBackfillInput backfillInput) {
    if (!backfillInput.id().equals(id)) {
      return Response.forStatus(
          Status.BAD_REQUEST.withReasonPhrase("ID of payload does not match ID in uri."));
    }

    final Backfill backfill;
    try {
      backfill = storage.runInTransactionWithRetries(tx -> {
        final Backfill oldBackfill = tx.backfill(id)
            .orElseThrow(() -> new ResourceNotFoundException(String.format("Backfill %s not found.", id)));
        workflowActionAuthorizer.authorizeWorkflowAction(ac, oldBackfill.workflowId());
        final BackfillBuilder backfillBuilder = oldBackfill.builder();
        backfillInput.concurrency().ifPresent(backfillBuilder::concurrency);
        backfillInput.description().ifPresent(backfillBuilder::description);
        return tx.store(backfillBuilder.build());
      });
    } catch (ResourceNotFoundException e) {
      return Response.forStatus(Status.NOT_FOUND.withReasonPhrase(e.getMessage()));
    } catch (IOException e) {
      return Response.forStatus(
          Status.INTERNAL_SERVER_ERROR.withReasonPhrase("Failed to store backfill."));
    }

    return Response.forStatus(Status.OK).withPayload(backfill);
  }

  private List<RunStateData> retrieveBackfillStatuses(Backfill backfill) {
    final List<RunStateData> processedStates;
    final List<RunStateData> waitingStates;

    final Map<WorkflowInstance, RunState> activeWorkflowInstances;
    try {
      // this is weakly consistent and is tolerable in this case because no critical action
      // depends on this
      activeWorkflowInstances = storage.readActiveStatesByTriggerId(backfill.id());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final List<Instant> processedInstants;
    if (backfill.reverse()) {
      final Instant firstInstant = nextInstant(backfill.nextTrigger(), backfill.schedule());
      processedInstants = instantsInRange(firstInstant, backfill.end(), backfill.schedule());
    } else {
      processedInstants = instantsInRange(backfill.start(), backfill.nextTrigger(), backfill.schedule());
    }
    processedStates = processedInstants.stream()
        .map(instant -> forkJoinPool.submit(() ->
            getRunStateData(backfill, activeWorkflowInstances, instant)))
        .collect(toList())
        .stream()
        .map(ForkJoinTask::join)
        .collect(toList());

    final List<Instant> waitingInstants;
    if (backfill.reverse()) {
      final Instant lastInstant = nextInstant(backfill.nextTrigger(), backfill.schedule());
      waitingInstants = instantsInRange(backfill.start(), lastInstant, backfill.schedule());
    } else {
      waitingInstants = instantsInRange(backfill.nextTrigger(), backfill.end(), backfill.schedule());
    }
    waitingStates = waitingInstants.stream()
        .map(instant -> {
          final WorkflowInstance wfi = WorkflowInstance.create(
              backfill.workflowId(), toParameter(backfill.schedule(), instant));
          return RunStateData.create(wfi, WAITING, StateData.zero());
        })
        .collect(toList());

    return backfill.reverse()
        ? Stream.concat(waitingStates.stream(), processedStates.stream()).collect(toList())
        : Stream.concat(processedStates.stream(), waitingStates.stream()).collect(toList());
  }

  private RunStateData getRunStateData(Backfill backfill,
      Map<WorkflowInstance, RunState> activeWorkflowInstances, Instant instant) {

    final WorkflowInstance wfi = WorkflowInstance
        .create(backfill.workflowId(), toParameter(backfill.schedule(), instant));

    if (activeWorkflowInstances.containsKey(wfi)) {
      final RunState state = activeWorkflowInstances.get(wfi);
      return RunStateData.newBuilder()
          .workflowInstance(state.workflowInstance())
          .state(state.state().name())
          .stateData(state.data())
          .latestTimestamp(state.timestamp())
          .build();
    }

    return ReplayEvents.getBackfillRunStateData(wfi, storage, backfill.id())
        .orElse(RunStateData.create(wfi, UNKNOWN, StateData.zero()));
  }
}
