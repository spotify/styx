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
import static com.spotify.styx.api.Middlewares.json;
import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static com.spotify.styx.serialization.Json.serialize;
import static com.spotify.styx.util.StreamUtil.cat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.spotify.apollo.Request;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Route;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.DockerImageValidator;
import com.spotify.styx.util.ResourceNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import okio.ByteString;

public final class WorkflowResource {

  static final String BASE = "/workflows";
  public static final int DEFAULT_PAGE_LIMIT = 24 * 7;
  private static final String SCHEDULER_BASE_PATH = "/api/v0";
  private final DockerImageValidator dockerImageValidator;

  private final String schedulerServiceBaseUrl;
  private final Storage storage;


  public WorkflowResource(Storage storage, String schedulerServiceBaseUrl, DockerImageValidator dockerImageValidator) {
    this.storage = Objects.requireNonNull(storage);
    this.dockerImageValidator = Objects.requireNonNull(dockerImageValidator, "dockerImageValidator");
    this.schedulerServiceBaseUrl = Objects.requireNonNull(schedulerServiceBaseUrl);
  }

  public Stream<Route<AsyncHandler<Response<ByteString>>>> routes() {
    final List<Route<AsyncHandler<Response<ByteString>>>> routes = Arrays.asList(
        Route.with(
            json(), "GET", BASE + "/<cid>/<wfid>",
            rc -> workflow(arg("cid", rc), arg("wfid", rc))),
        Route.with(
            json(), "GET", BASE + "/<cid>",
            rc -> workflows(arg("cid", rc))),
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

    final List<Route<AsyncHandler<Response<ByteString>>>> forwardedRoutes = Arrays.asList(
        Route.async(
            "POST", BASE + "/<cid>",
            rc -> createOrUpdateWorkflow(arg("cid", rc), rc)
        ),
        Route.async(
            "DELETE", BASE + "/<cid>/<wfid>",
            rc -> deleteWorkflow(arg("cid", rc), arg("wfid", rc), rc)
        )
    );

    return cat(
        Api.prefixRoutes(routes, V3),
        Api.prefixRoutes(forwardedRoutes, V3)
    );
  }

  private CompletionStage<Response<ByteString>> deleteWorkflow(String cid, String wfid,
                                                               RequestContext rc) {
    // TODO: handle workflow crud directly in api service instead of proxying to scheduler
    return rc.requestScopedClient()
        .send(rc.request().withUri(schedulerApiUrl("workflows", cid, wfid)));
  }

  private CompletionStage<Response<ByteString>> createOrUpdateWorkflow(String componentId,
                                                                       RequestContext rc) {
    final Optional<ByteString> payload = rc.request().payload();
    if (!payload.isPresent()) {
      return CompletableFuture.completedFuture(
          Response.forStatus(Status.BAD_REQUEST.withReasonPhrase("Missing payload.")));
    }
    final WorkflowConfiguration workflowConfig;
    try {
      workflowConfig = OBJECT_MAPPER
          .readValue(payload.get().toByteArray(), WorkflowConfiguration.class);
    } catch (IOException e) {
      return CompletableFuture.completedFuture(
          Response.forStatus(Status.BAD_REQUEST.withReasonPhrase("Invalid payload.")));
    }

    final Workflow workflow = Workflow.create(componentId, workflowConfig);

    final ByteString requestPayload;
    try {
      requestPayload = serialize(workflow);
    } catch (JsonProcessingException e) {
      return CompletableFuture.completedFuture(Response.forStatus(
          Status.INTERNAL_SERVER_ERROR.withReasonPhrase("Failed to serialize proxy payload.")));
    }

    // TODO: handle workflow crud directly in api service instead of proxying to scheduler
    return rc.requestScopedClient()
        .send(rc.request().withPayload(requestPayload).withUri(schedulerApiUrl("workflows")));
  }

  private Response<List<Workflow>> workflows(String componentId) {
    try {
      return Response.forPayload(storage.workflows(componentId));
    } catch (IOException e) {
      return Response.forStatus(Status.INTERNAL_SERVER_ERROR.withReasonPhrase(
          "Failed to get workflows of component " + componentId));
    }
  }

  public Response<WorkflowState> patchState(String componentId, String id, Request request) {
    final Optional<ByteString> payload = request.payload();
    if (!payload.isPresent()) {
      return Response.forStatus(Status.BAD_REQUEST.withReasonPhrase("Missing payload."));
    }

    final WorkflowId workflowId = WorkflowId.create(componentId, id);
    final WorkflowState patchState;
    try {
      patchState = OBJECT_MAPPER.readValue(payload.get().toByteArray(), WorkflowState.class);
    } catch (IOException e) {
      return Response.forStatus(Status.BAD_REQUEST.withReasonPhrase("Invalid payload."));
    }

    if (patchState.dockerImage().isPresent()) {
      final Collection<String> errors = dockerImageValidator.validateImageReference(patchState.dockerImage().get());
      if (!errors.isEmpty()) {
        return Response.forStatus(Status.BAD_REQUEST.withReasonPhrase("Invalid docker image: " + errors));
      }
    }

    if (patchState.commitSha().isPresent()) {
      if (!isValidSHA1(patchState.commitSha().get())) {
        return Response.forStatus(Status.BAD_REQUEST.withReasonPhrase("Invalid SHA-1."));
      }
    }

    try {
      storage.patchState(workflowId, patchState);
    } catch (ResourceNotFoundException e) {
      return Response
          .forStatus(Status.NOT_FOUND.withReasonPhrase(e.getMessage()));
    } catch (IOException e) {
      return Response
          .forStatus(
              Status.INTERNAL_SERVER_ERROR.withReasonPhrase("Failed to update the state."));
    }

    return state(componentId, id);
  }

  public Response<Workflow> workflow(String componentId, String id) {
    final WorkflowId workflowId = WorkflowId.create(componentId, id);
    try {
      return storage.workflow(workflowId).map(Response::forPayload)
          .orElse(Response.forStatus(Status.NOT_FOUND));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public Response<WorkflowState> state(String componentId, String id) {
    final WorkflowId workflowId = WorkflowId.create(componentId, id);
    WorkflowState workflowState;
    try {
      workflowState = storage.workflowState(workflowId);
    } catch (IOException e) {
      return Response
          .forStatus(Status.INTERNAL_SERVER_ERROR.withReasonPhrase("Couldn't fetch state."));
    }
    return Response.forPayload(workflowState);
  }

  public Response<List<WorkflowInstanceExecutionData>> instances(
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
          Status.INTERNAL_SERVER_ERROR.withReasonPhrase("Couldn't fetch execution info."));
    }
    return Response.forPayload(data);
  }

  public Response<WorkflowInstanceExecutionData> instance(
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
          Status.INTERNAL_SERVER_ERROR.withReasonPhrase("Couldn't fetch execution info."));
    }
  }

  private String schedulerApiUrl(CharSequence... parts) {
    return schedulerServiceBaseUrl + SCHEDULER_BASE_PATH + "/" + String.join("/", parts);
  }

  private static boolean isValidSHA1(String s) {
    return s.matches("[a-fA-F0-9]{40}");
  }

  private static String arg(String name, RequestContext rc) {
    return rc.pathArgs().get(name);
  }
}
