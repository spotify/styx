/*-
 * -\-\-
 * Spotify Styx Flyte Client
 * --
 * Copyright (C) 2016 - 2020 Spotify AB
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

package com.spotify.styx.flyte.client;

import flyteidl.admin.ExecutionOuterClass;
import flyteidl.core.IdentifierOuterClass;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.stub.StreamObserver;

public class TestAdminService extends AdminServiceGrpc.AdminServiceImplBase  {
  static final String WF_EXECUTION_ID = "wf_execution_id_1";

  @Override
  public void createExecution(final ExecutionOuterClass.ExecutionCreateRequest request,
                              final StreamObserver<ExecutionOuterClass.ExecutionCreateResponse> responseObserver) {
    responseObserver.onNext(ExecutionOuterClass.ExecutionCreateResponse
        .newBuilder()
        .setId(IdentifierOuterClass.WorkflowExecutionIdentifier
            .newBuilder()
            .setDomain(request.getDomain())
            .setProject(request.getProject())
            .setName(WF_EXECUTION_ID)
            .build())
        .build());
    responseObserver.onCompleted();
  }
}
