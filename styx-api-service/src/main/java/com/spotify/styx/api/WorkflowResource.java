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

import static com.spotify.apollo.Status.BAD_REQUEST;
import static com.spotify.apollo.Status.INTERNAL_SERVER_ERROR;
import static com.spotify.apollo.Status.NOT_FOUND;
import static com.spotify.apollo.Status.NO_CONTENT;
import static com.spotify.styx.api.Api.Version.V3;
import static com.spotify.styx.api.Middlewares.json;
import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.spotify.apollo.Request;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.entity.EntityMiddleware;
import com.spotify.apollo.entity.JacksonEntityCodec;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Route;
import com.spotify.styx.api.workflow.WorkflowInitializationException;
import com.spotify.styx.api.workflow.WorkflowInitializer;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.ResourceNotFoundException;
import com.spotify.styx.util.WorkflowValidator;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import okio.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class WorkflowResource {

  private static final String BASE = "/workflows";
  private static final int DEFAULT_PAGE_LIMIT = 24 * 7;

  private static final Logger LOG = LoggerFactory.getLogger(WorkflowResource.class);

  private final WorkflowValidator workflowValidator;
  private final WorkflowInitializer workflowInitializer;

  private final Storage storage;


  public WorkflowResource(Storage storage, WorkflowValidator workflowValidator,
                          WorkflowInitializer workflowInitializer) {
    this.storage = Objects.requireNonNull(storage, "storage");
    this.workflowValidator = Objects.requireNonNull(workflowValidator, "workflowValidator");
    this.workflowInitializer = Objects.requireNonNull(workflowInitializer, "workflowInitializer");
  }

  public Stream<Route<AsyncHandler<Response<ByteString>>>> routes() {
    final EntityMiddleware em =
        EntityMiddleware.forCodec(JacksonEntityCodec.forMapper(Json.OBJECT_MAPPER));

    final List<Route<AsyncHandler<Response<ByteString>>>> routes = Arrays.asList(
        Route.with(
            json(), "GET", BASE + "/<cid>/<wfid>",
            rc -> workflow(arg("cid", rc), arg("wfid", rc))),
        Route.with(
            json(), "GET", BASE,
            rc -> workflows()),
        Route.with(
            json(), "GET", BASE + "/<cid>",
            rc -> workflows(arg("cid", rc))),
        Route.with(
            json(), "POST", BASE + "/<cid>",
            rc -> createOrUpdateWorkflow(arg("cid", rc), rc)),
        Route.with(
            json(), "DELETE", BASE + "/<cid>/<wfid>",
            rc -> deleteWorkflow(arg("cid", rc),arg("wfid", rc))),
        Route.with(
            json(), "GET", BASE + "/<cid>/<wfid>/instances",
            rc -> instances(arg("cid", rc), arg("wfid", rc), rc.request())),
        Route.with(
            json(), "GET", BASE + "/<cid>/<wfid>/instances/<iid>",
            rc -> instance(arg("cid", rc), arg("wfid", rc), arg("iid", rc))),
        Route.with(
            json(), "GET", BASE + "/<cid>/<wfid>/state",
            rc -> state(arg("cid", rc), arg("wfid", rc))),
        Route.with(
            json(), "PATCH", BASE + "/<cid>/<wfid>/state",
            rc -> patchState(arg("cid", rc), arg("wfid", rc), rc.request()))
    );

    return Api.prefixRoutes(routes, V3);
  }

  private Response<ByteString> deleteWorkflow(String cid, String wfid) {
    final WorkflowId workflowId = WorkflowId.create(cid, wfid);
    try {
      storage.delete(workflowId);
    } catch (IOException e) {
      final String message = String.format("Couldn't remove workflow %s. ", workflowId);
      LOG.warn(message, e);
      throw new RuntimeException(message, e);
    }
    LOG.info("Workflow removed: {}", workflowId);
    return Response.forStatus(NO_CONTENT);
  }

  private Response<Workflow> createOrUpdateWorkflow(String componentId,
                                                    RequestContext rc) {
    final Optional<ByteString> payload = rc.request().payload();
    if (!payload.isPresent()) {
      return Response.forStatus(BAD_REQUEST.withReasonPhrase("Missing payload."));
    }
    final WorkflowConfiguration workflowConfig;
    try {
      workflowConfig = OBJECT_MAPPER
          .readValue(payload.get().toByteArray(), WorkflowConfiguration.class);
    } catch (IOException e) {
      return Response.forStatus(BAD_REQUEST
          .withReasonPhrase("Invalid payload. " + e.getMessage()));
    }

    final Collection<String> errors =
        workflowValidator.validateWorkflowConfiguration(workflowConfig);
    if (!errors.isEmpty()) {
      return Response.forStatus(
          BAD_REQUEST.withReasonPhrase("Invalid workflow configuration: " + errors));
    }

    final Workflow workflow = Workflow.create(componentId, workflowConfig);
    final Optional<Workflow> oldWorkflowOptional;
    try {
      oldWorkflowOptional = workflowInitializer.inspectChange(workflow);
    } catch (WorkflowInitializationException e) {
      return Response.forStatus(BAD_REQUEST.withReasonPhrase(e.getMessage()));
    }

    if (oldWorkflowOptional.isPresent()) {
      LOG.info("Workflow modified, old config: {}, new config: {}", oldWorkflowOptional.get(),
          workflow);
    } else {
      LOG.info("Workflow added: {}", workflow);
    }

    return Response.forPayload(workflow);
  }

  private Response<Collection<Workflow>> workflows() {
    try {
      return Response.forPayload(storage.workflows().values());
    } catch (IOException e) {
      return Response.forStatus(INTERNAL_SERVER_ERROR.withReasonPhrase(
          "Failed to get workflows"));
    }
  }

  private Response<List<Workflow>> workflows(String componentId) {
    try {
      return Response.forPayload(storage.workflows(componentId));
    } catch (IOException e) {
      return Response.forStatus(INTERNAL_SERVER_ERROR.withReasonPhrase(
          "Failed to get workflows of component " + componentId));
    }
  }

  private Response<WorkflowState> patchState(String componentId, String id, Request request) {
    final Optional<ByteString> payload = request.payload();
    if (!payload.isPresent()) {
      return Response.forStatus(BAD_REQUEST.withReasonPhrase("Missing payload."));
    }

    final WorkflowId workflowId = WorkflowId.create(componentId, id);
    final WorkflowState patchState;
    try {
      final JsonNode json = OBJECT_MAPPER.readTree(payload.get().toByteArray());
      if (json.has("commit_sha") || json.has("docker_image")) {
        // TODO: remove this when nobody is doing PATCH with these fields (added 2017-11-08)
        return Response.forStatus(BAD_REQUEST.withReasonPhrase(
            "Invalid payload: commit_sha and docker_image not allowed."));
      }
      patchState = OBJECT_MAPPER.readValue(payload.get().toByteArray(), WorkflowState.class);
    } catch (IOException e) {
      return Response.forStatus(BAD_REQUEST.withReasonPhrase("Invalid payload."));
    }

    try {
      storage.patchState(workflowId, patchState);
    } catch (ResourceNotFoundException e) {
      return Response.forStatus(NOT_FOUND.withReasonPhrase(e.getMessage()));
    } catch (IOException e) {
      return Response
          .forStatus(
              INTERNAL_SERVER_ERROR.withReasonPhrase("Failed to update the state."));
    }

    return state(componentId, id);
  }

  private Response<Workflow> workflow(String componentId, String id) {
    try {
      return storage.workflow(WorkflowId.create(componentId, id))
          .map(Response::forPayload)
          .orElse(Response.forStatus(NOT_FOUND));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private Response<WorkflowState> state(String componentId, String id) {
    try {
      return Response.forPayload(storage.workflowState(WorkflowId.create(componentId, id)));
    } catch (IOException e) {
      return Response
          .forStatus(INTERNAL_SERVER_ERROR.withReasonPhrase("Couldn't fetch state."));
    }
  }

  private Response<List<WorkflowInstanceExecutionData>> instances(
      String componentId,
      String id,
      Request request) {
    final WorkflowId workflowId = WorkflowId.create(componentId, id);
    final String offset = request.parameter("offset").orElse("");
    final int limit = request.parameter("limit").map(Integer::parseInt).orElse(DEFAULT_PAGE_LIMIT);
    final String start = request.parameter("start").orElse("");
    final String stop = request.parameter("stop").orElse("");

    final List<WorkflowInstanceExecutionData> data;
    try {
      if (Strings.isNullOrEmpty(start)) {
        data = storage.executionData(workflowId, offset, limit);
      } else {
        data = storage.executionData(workflowId, start, stop);
      }
    } catch (IOException e) {
      return Response.forStatus(
          INTERNAL_SERVER_ERROR.withReasonPhrase("Couldn't fetch execution info."));
    }
    return Response.forPayload(data);
  }

  private Response<WorkflowInstanceExecutionData> instance(
      String componentId,
      String id,
      String instanceId) {
    final WorkflowId workflowId = WorkflowId.create(componentId, id);
    final WorkflowInstance workflowInstance = WorkflowInstance.create(workflowId, instanceId);

    try {
      final WorkflowInstanceExecutionData workflowInstanceExecutionData =
          storage.executionData(workflowInstance);

      return Response.forPayload(workflowInstanceExecutionData);
    } catch (IOException e) {
      return Response.forStatus(
          INTERNAL_SERVER_ERROR.withReasonPhrase("Couldn't fetch execution info."));
    }
  }

  private static String arg(String name, RequestContext rc) {
    return rc.pathArgs().get(name);
  }
}
