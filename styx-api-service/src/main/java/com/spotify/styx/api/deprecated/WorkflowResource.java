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

package com.spotify.styx.api.deprecated;

import static com.spotify.styx.api.Api.Version.V0;
import static com.spotify.styx.api.Api.Version.V1;
import static com.spotify.styx.api.Middlewares.authed;
import static com.spotify.styx.api.Middlewares.json;
import static com.spotify.styx.util.StreamUtil.cat;

import com.spotify.apollo.Request;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Route;
import com.spotify.styx.api.Api;
import com.spotify.styx.api.Middlewares.AuthContext;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.deprecated.WorkflowInstanceExecutionData;
import com.spotify.styx.model.deprecated.Workflow;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import okio.ByteString;

@Deprecated
public final class WorkflowResource {

  static final String BASE = "/workflows";
  private com.spotify.styx.api.WorkflowResource workflowResource;

  public WorkflowResource(com.spotify.styx.api.WorkflowResource workflowResource) {
    this.workflowResource = Objects.requireNonNull(workflowResource);
  }

  public Stream<Route<AsyncHandler<Response<ByteString>>>> routes() {
    final List<Route<AsyncHandler<Response<ByteString>>>> v0Routes = Arrays.asList(
        Route.with(
            authed(), "GET", BASE + "/<cid>/<eid>",
            rc -> auth -> workflow(auth, arg("cid", rc), arg("eid", rc))),
        Route.with(
            json(), "GET", BASE + "/<cid>/<eid>/instances",
            rc -> Response.forStatus(Status.NOT_FOUND.withReasonPhrase("Use v1 api"))),
        Route.with(
            json(), "GET", BASE + "/<cid>/<eid>/instances/<iid>",
            rc -> Response.forStatus(Status.NOT_FOUND.withReasonPhrase("Use v1 api"))),
        Route.with(
            json(), "GET", BASE + "/<cid>/<eid>/state",
            rc -> state(arg("cid", rc), arg("eid", rc))),
        Route.with(
            json(), "PATCH", BASE + "/<cid>/<eid>/state",
            rc -> patchState(arg("cid", rc), arg("eid", rc), rc.request())),
        Route.with(
            json(), "PATCH", BASE + "/<cid>/state",
            rc -> patchState(arg("cid", rc), rc.request()))
    );

    final List<Route<AsyncHandler<Response<ByteString>>>> v1Routes = Arrays.asList(
        Route.with(
            json(), "GET", BASE + "/<cid>/<eid>/instances",
            rc -> instances(arg("cid", rc), arg("eid", rc), rc.request())),
        Route.with(
            json(), "GET", BASE + "/<cid>/<eid>/instances/<iid>",
            rc -> instance(arg("cid", rc), arg("eid", rc), arg("iid", rc)))
    );

    return cat(
        Api.prefixRoutes(v0Routes, V0, V1),
        Api.prefixRoutes(v1Routes, V1)
    );
  }

  private Response<WorkflowState> patchState(String componentId, String endpointId, Request request) {
    return workflowResource.patchState(componentId, endpointId, request);
  }

  private Response<WorkflowState> patchState(String componentId, Request request) {
    return workflowResource.patchState(componentId, request);
  }

  private Response<Workflow> workflow(AuthContext authContext, String componentId, String endpointId) {
    final Response<com.spotify.styx.model.Workflow> response =
        workflowResource.workflow(authContext, componentId, endpointId);
    return response.withPayload(response.payload().map(Workflow::create).orElse(null));
  }

  private Response<WorkflowState> state(String componentId, String endpointId) {
    return workflowResource.state(componentId, endpointId);
  }

  private Response<List<WorkflowInstanceExecutionData>> instances(
      String componentId,
      String endpointId,
      Request request) {
    final Response<List<com.spotify.styx.model.data.WorkflowInstanceExecutionData>>
        response = workflowResource.instances(componentId, endpointId, request);
    return response.withPayload(response.payload()
                                    .map(l -> l.stream()
                                        .map(WorkflowInstanceExecutionData::create)
                                        .collect(Collectors.toList()))
                                    .orElse(null));
  }

  private Response<WorkflowInstanceExecutionData> instance(
      String componentId,
      String endpointId,
      String instanceId) {
    final Response<com.spotify.styx.model.data.WorkflowInstanceExecutionData>
        response = workflowResource.instance(componentId, endpointId, instanceId);
    return response.withPayload(response.payload()
                                    .map(WorkflowInstanceExecutionData::create)
                                    .orElse(null));
  }

  private static String arg(String name, RequestContext rc) {
    return rc.pathArgs().get(name);
  }
}
