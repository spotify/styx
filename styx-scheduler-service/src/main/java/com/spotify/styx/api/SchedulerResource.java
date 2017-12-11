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
import static com.spotify.apollo.Status.INTERNAL_SERVER_ERROR;
import static com.spotify.styx.util.ParameterUtil.parseAlignedInstant;

import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.entity.EntityMiddleware;
import com.spotify.apollo.entity.JacksonEntityCodec;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.Route;
import com.spotify.styx.TriggerListener;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.DockerImageValidator;
import com.spotify.styx.util.EventUtil;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.RandomGenerator;
import com.spotify.styx.util.Time;
import com.spotify.styx.workflow.WorkflowInitializationException;
import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;
import okio.ByteString;

public class SchedulerResource {

  public static final String BASE = "/api/v0";
  private static final String AD_HOC_CLI_TRIGGER_PREFIX = "ad-hoc-cli";

  private final StateManager stateManager;
  private final TriggerListener triggerListener;
  private final Consumer<Workflow> workflowChangeListener;
  private final Consumer<Workflow> workflowRemoveListener;
  private final Storage storage;
  private final Time time;
  private final DockerImageValidator dockerImageValidator;

  private final RandomGenerator randomGenerator = RandomGenerator.DEFAULT;

  public SchedulerResource(
      StateManager stateManager,
      TriggerListener triggerListener,
      Consumer<Workflow> workflowChangeListener,
      Consumer<Workflow> workflowRemoveListener,
      Storage storage,
      Time time,
      DockerImageValidator dockerImageValidator) {
    this.stateManager = Objects.requireNonNull(stateManager);
    this.triggerListener = Objects.requireNonNull(triggerListener);
    this.workflowChangeListener = workflowChangeListener;
    this.workflowRemoveListener = workflowRemoveListener;
    this.storage = Objects.requireNonNull(storage);
    this.time = Objects.requireNonNull(time);
    this.dockerImageValidator = Objects.requireNonNull(dockerImageValidator, "dockerImageValidator");
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
            em.response(WorkflowInstance.class),
            "POST", BASE + "/trigger",
            rc -> this::triggerWorkflowInstance),
        Route.with(
            em.response(WorkflowConfiguration.class, Workflow.class),
            "POST", BASE + "/workflows/<cid>",
            rc -> workflow -> createOrUpdateWorkflow(rc.pathArgs().get("cid"), workflow)),
        Route.with(
            em.serializerResponse(ByteString.class),
            "DELETE", BASE + "/workflows/<cid>/<wfid>",
            rc -> deleteWorkflow(rc.pathArgs().get("cid"), rc.pathArgs().get("wfid")))
    )

        .map(r -> r.withMiddleware(Middleware::syncToAsync));
  }

  private Response<ByteString> deleteWorkflow(String cid, String wfid) {
    final Optional<Workflow> workflowOpt;
    try {
      workflowOpt = storage.workflow(WorkflowId.create(cid, wfid));
    } catch (IOException e) {
      return Response
          .forStatus(Status.INTERNAL_SERVER_ERROR.withReasonPhrase("Error in internal storage"));
    }
    if (!workflowOpt.isPresent()) {
      return Response.forStatus(Status.NOT_FOUND.withReasonPhrase("Workflow does not exist"));
    }
    workflowRemoveListener.accept(workflowOpt.get());
    return Response.forStatus(Status.NO_CONTENT);
  }

  private Response<Workflow> createOrUpdateWorkflow(String componentId, WorkflowConfiguration configuration) {
    if (!configuration.dockerImage().isPresent()) {
      return Response.forStatus(Status.BAD_REQUEST.withReasonPhrase("Missing docker image"));
    }
    final Collection<String> errors = dockerImageValidator.validateImageReference(
        configuration.dockerImage().get());
    if (!errors.isEmpty()) {
      return Response.forStatus(Status.BAD_REQUEST.withReasonPhrase("Invalid docker image: " + errors));
    }

    if (configuration.commitSha().isPresent()
        && !isValidSHA1(configuration.commitSha().get())) {
      return Response.forStatus(Status.BAD_REQUEST.withReasonPhrase("Invalid commit sha"));
    }

    final Workflow workflow = Workflow.create(componentId, configuration);

    try {
      workflowChangeListener.accept(workflow);
    } catch (WorkflowInitializationException e) {
      return Response.forStatus(Status.BAD_REQUEST.withReasonPhrase(e.getMessage()));
    }

    return Response.forPayload(workflow);
  }


  private Response<Event> injectEvent(Event event) {
    if (!stateManager.isActiveWorkflowInstance(event.workflowInstance())) {
      return Response.forStatus(BAD_REQUEST.withReasonPhrase("Workflow instance not found"));
    }

    try {
      if ("dequeue".equals(EventUtil.name(event))) {
        stateManager.receive(Event.retryAfter(event.workflowInstance(), 0L));
      } else {
        stateManager.receive(event);
      }
    } catch (IsClosedException isClosedException) {
      return Response.forStatus(INTERNAL_SERVER_ERROR);
    }

    return Response.forPayload(event);
  }

  private Response<WorkflowInstance> triggerWorkflowInstance(WorkflowInstance workflowInstance) {
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

    // Verifying instant
    try {
      instant = parseAlignedInstant(
          workflowInstance.parameter(),
          workflow.configuration().schedule());
    } catch (IllegalArgumentException e) {
      return Response.forStatus(BAD_REQUEST.withReasonPhrase(e.getMessage()));
    }

    // Verifying active
    if (stateManager.isActiveWorkflowInstance(workflowInstance)) {
      return Response.forStatus(
          BAD_REQUEST.withReasonPhrase("The specified instance is already "
                                       + "active in the scheduler"));
    }

    // Verifying future
    if (instant.isAfter(time.get())) {
      return Response.forStatus(BAD_REQUEST.withReasonPhrase(
          "Cannot trigger an instance of the future"));
    }

    final String triggerId = randomGenerator.generateUniqueId(AD_HOC_CLI_TRIGGER_PREFIX);
    triggerListener.event(workflow, Trigger.adhoc(triggerId), instant);

    // todo: change payload to a struct returning the triggerId as well so the user can refer to it
    return Response.forPayload(workflowInstance);
  }

  private static boolean isValidSHA1(String s) {
    return s.matches("[a-fA-F0-9]{40}");
  }
}
