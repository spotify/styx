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

import static com.spotify.styx.flyte.client.FlyteAdminClientTest.DOMAIN;
import static com.spotify.styx.flyte.client.FlyteAdminClientTest.PROJECT;

import flyteidl.admin.Common;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.core.IdentifierOuterClass;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.List;

public class TestAdminService extends AdminServiceGrpc.AdminServiceImplBase  {

  @Override
  public void createExecution(final ExecutionOuterClass.ExecutionCreateRequest request,
                              final StreamObserver<ExecutionOuterClass.ExecutionCreateResponse> responseObserver) {
    responseObserver.onNext(ExecutionOuterClass.ExecutionCreateResponse
        .newBuilder()
        .setId(IdentifierOuterClass.WorkflowExecutionIdentifier
            .newBuilder()
            .setDomain(request.getDomain())
            .setProject(request.getProject())
            .setName(request.getName())
            .build())
        .build());
    responseObserver.onCompleted();
  }

  @Override
  public void getExecution(final ExecutionOuterClass.WorkflowExecutionGetRequest request,
                           final StreamObserver<ExecutionOuterClass.Execution> responseObserver) {
    responseObserver.onNext(ExecutionOuterClass.Execution
        .newBuilder()
        .setId(IdentifierOuterClass.WorkflowExecutionIdentifier
            .newBuilder()
            .setProject(request.getId().getProject())
            .setDomain(request.getId().getDomain())
            .setName(request.getId().getName())
            .build())
        .build());
    responseObserver.onCompleted();
  }

  @Override
  public void terminateExecution(final ExecutionOuterClass.ExecutionTerminateRequest request,
                                 final StreamObserver<ExecutionOuterClass.ExecutionTerminateResponse> responseObserver) {
    responseObserver.onNext(ExecutionOuterClass.ExecutionTerminateResponse.getDefaultInstance());
    responseObserver.onCompleted();
  }

  @Override
  public void listExecutions(final Common.ResourceListRequest request,
                             final StreamObserver<ExecutionOuterClass.ExecutionList> responseObserver) {
    final var executions = List.of(
        ExecutionOuterClass.Execution
            .newBuilder()
            .setId(IdentifierOuterClass.WorkflowExecutionIdentifier
                .newBuilder()
                .setProject(PROJECT)
                .setDomain(DOMAIN)
                .setName(request.getId().getName())
                .build())
            .build(),
        ExecutionOuterClass.Execution
            .newBuilder()
            .setId(IdentifierOuterClass.WorkflowExecutionIdentifier
                .newBuilder()
                .setProject(PROJECT)
                .setDomain(DOMAIN)
                .setName(request.getId().getName()))
            .build()
    );

    final ExecutionOuterClass.ExecutionList response = ExecutionOuterClass.ExecutionList.newBuilder()
        .addAllExecutions(executions)
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }
}
