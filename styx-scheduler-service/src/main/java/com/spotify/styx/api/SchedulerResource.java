/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

import static com.spotify.apollo.Status.BAD_REQUEST;
import static com.spotify.apollo.Status.CONFLICT;
import static com.spotify.apollo.Status.INTERNAL_SERVER_ERROR;
import static com.spotify.apollo.Status.OK;
import static com.spotify.styx.util.ExceptionUtil.findCause;
import static com.spotify.styx.util.ParameterUtil.parseAlignedInstant;

import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.StatusType;
import com.spotify.apollo.entity.EntityMiddleware;
import com.spotify.apollo.entity.JacksonEntityCodec;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.Route;
import com.spotify.styx.TriggerListener;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.TriggerRequest;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.EventUtil;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.RandomGenerator;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.WorkflowValidator;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import okio.ByteString;

public class SchedulerResource {

  public static final String BASE = "/api/v0";
  private static final String AD_HOC_CLI_TRIGGER_PREFIX = "ad-hoc-cli";

  private final StateManager stateManager;
  private final TriggerListener triggerListener;
  private final Storage storage;
  private final Time time;
  private final WorkflowValidator workflowValidator;

  private final RandomGenerator randomGenerator = RandomGenerator.DEFAULT;

  public SchedulerResource(
      StateManager stateManager,
      TriggerListener triggerListener,
      Storage storage,
      Time time,
      WorkflowValidator workflowValidator) {
    this.stateManager = Objects.requireNonNull(stateManager);
    this.triggerListener = Objects.requireNonNull(triggerListener);
    this.storage = Objects.requireNonNull(storage);
    this.time = Objects.requireNonNull(time);
    this.workflowValidator = Objects.requireNonNull(workflowValidator, "workflowValidator");
  }

  public Stream<Route<AsyncHandler<Response<ByteString>>>> routes() {
    final EntityMiddleware em =
        EntityMiddleware.forCodec(JacksonEntityCodec.forMapper(Json.OBJECT_MAPPER));

    return Stream.of(
        Route.with(
            em.response(Event.class),
            "POST", BASE + "/events",
            rc -> this::injectEvent),
        Route.with(
            em.response(TriggerRequest.class),
            "POST", BASE + "/trigger",
            rc -> this::triggerWorkflowInstance),
        Route.with(
            em.response(WorkflowInstance.class),
            "POST", BASE + "/retry",
            rc -> payload -> retryWorkflowInstanceAfter(rc, payload)),
        Route.with(
            em.response(WorkflowInstance.class),
            "POST", BASE + "/halt",
            rc -> this::haltWorkflowInstance)
    )

        .map(r -> r.withMiddleware(Middleware::syncToAsync));
  }

  private Response<WorkflowInstance> haltWorkflowInstance(WorkflowInstance workflowInstance) {
    final Event event = Event.halt(workflowInstance);
    return Response.forStatus(eventInjectorHelper(event)).withPayload(workflowInstance);
  }

  private Response<WorkflowInstance> retryWorkflowInstanceAfter(RequestContext rc,
                                                                WorkflowInstance workflowInstance) {
    final long delay;
    try {
      delay = Long.parseLong(rc.request().parameter("delay").orElse("0"));
    } catch (NumberFormatException e) {
      return Response.forStatus(BAD_REQUEST.withReasonPhrase(
          "Delay parameter could not be parsed"));
    }
    final Event event = Event.retryAfter(workflowInstance, delay);
    return Response.forStatus(eventInjectorHelper(event)).withPayload(workflowInstance);
  }

  private Response<Event> injectEvent(Event event) {
    if ("dequeue".equals(EventUtil.name(event))) {
      // For backwards compatibility
      return Response.forStatus(eventInjectorHelper(
          Event.retryAfter(event.workflowInstance(), 0L))).withPayload(event);
    } else if ("halt".equals(EventUtil.name(event))) {
      // For backwards compatibility
      return Response.forStatus(eventInjectorHelper(event));
    } else if ("timeout".equals(EventUtil.name(event))) {
      // This is for manually getting out of a stale state
      return Response.forStatus(eventInjectorHelper(event));
    } else {
      return Response.forStatus(BAD_REQUEST.withReasonPhrase(
          "This API for injecting generic events is deprecated, refer to the specific API for the "
          + "event you want to send to the scheduler"));
    }
  }

  private StatusType eventInjectorHelper(Event event) {
    try {
      stateManager.receive(event).toCompletableFuture().get();
    } catch (IsClosedException | InterruptedException e) {
      return INTERNAL_SERVER_ERROR.withReasonPhrase(e.getMessage());
    } catch (ExecutionException e) {
      if (e.getCause() instanceof IllegalArgumentException
          || e.getCause() instanceof IllegalStateException) {
        return BAD_REQUEST.withReasonPhrase(e.getCause().getMessage());
      } else {
        return INTERNAL_SERVER_ERROR.withReasonPhrase(e.getMessage());
      }
    }
    return OK;
  }

  private Response<TriggerRequest> triggerWorkflowInstance(TriggerRequest triggerRequest) {
    final WorkflowInstance workflowInstance = WorkflowInstance.create(
        triggerRequest.workflowId(), triggerRequest.parameter());
    final Workflow workflow;
    final Instant instant;

    // Verifying workflow
    try {
      final Optional<Workflow> workflowResult = storage.workflow(workflowInstance.workflowId());
      if (workflowResult.isPresent()) {
        workflow = workflowResult.get();
      } else {
        return Response.forStatus(
            BAD_REQUEST.withReasonPhrase("The specified workflow is not found in the scheduler"));
      }
    } catch (IOException e) {
      return Response.forStatus(
          INTERNAL_SERVER_ERROR.withReasonPhrase(
              "An error occurred while retrieving workflow specifications"));
    }
    if (!workflow.configuration().dockerImage().isPresent()) {
      return Response.forStatus(BAD_REQUEST.withReasonPhrase("Workflow is missing docker image"));
    }
    final Collection<String> errors = workflowValidator.validateWorkflow(workflow);
    if (!errors.isEmpty()) {
      return Response.forStatus(BAD_REQUEST.withReasonPhrase("Invalid workflow configuration: "
          + String.join(", ", errors)));
    }

    // Verifying instant
    try {
      instant = parseAlignedInstant(
          workflowInstance.parameter(),
          workflow.configuration().schedule());
    } catch (IllegalArgumentException e) {
      return Response.forStatus(BAD_REQUEST.withReasonPhrase(e.getMessage()));
    }

    final TriggerParameters parameters =
        triggerRequest.triggerParameters().orElse(TriggerParameters.zero());
    final String triggerId = randomGenerator.generateUniqueId(AD_HOC_CLI_TRIGGER_PREFIX);
    final CompletionStage<Void> triggered = triggerListener.event(
        workflow, Trigger.adhoc(triggerId), instant, parameters);

    // TODO: return future instead of blocking
    try {
      triggered.toCompletableFuture().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      return handleException(e);
    }

    // todo: change payload to a struct returning the triggerId as well so the user can refer to it
    return Response.forPayload(triggerRequest);
  }

  private Response<TriggerRequest> handleException(final ExecutionException e) {
    Throwable cause;
    if ((cause = findCause(e, IllegalStateException.class)) != null
        || (cause = findCause(e, IllegalArgumentException.class)) != null) {
      // TODO: propagate error information using a more specific exception type
      return Response.forStatus(CONFLICT.withReasonPhrase(cause.getMessage()));
    } else if (findCause(e, AlreadyInitializedException.class) != null) {
      return Response.forStatus(CONFLICT.withReasonPhrase(
          "This workflow instance is already triggered. Did you want to `retry` running it instead?"));
    } else {
      return Response.forStatus(INTERNAL_SERVER_ERROR.withReasonPhrase(e.getCause().getMessage()));
    }
  }
}
