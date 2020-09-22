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
import flyteidl.admin.ProjectOuterClass;
import flyteidl.core.IdentifierOuterClass;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.stub.StreamObserver;
import java.util.List;

public class TestAdminService extends AdminServiceGrpc.AdminServiceImplBase  {

  static final String EXEC_NAME_PREFIX = "exec_name_";
  static final int PAGE_SIZE = 2;

  private static final List<ExecutionOuterClass.Execution> ALL_EXECUTIONS =
      List.of(
          execution(PROJECT, DOMAIN, EXEC_NAME_PREFIX + 1),
          execution(PROJECT, DOMAIN, EXEC_NAME_PREFIX + 2),
          execution(PROJECT, DOMAIN, EXEC_NAME_PREFIX + 3),
          execution(PROJECT, DOMAIN, EXEC_NAME_PREFIX + 4),
          execution(PROJECT, DOMAIN, EXEC_NAME_PREFIX + 5),
          execution(PROJECT, DOMAIN, EXEC_NAME_PREFIX + 6)
      );
  private static final List<List<ExecutionOuterClass.Execution>> PAGED_EXECUTIONS =
      List.of(
          List.of(ALL_EXECUTIONS.get(0), ALL_EXECUTIONS.get(1)),
          List.of(ALL_EXECUTIONS.get(2), ALL_EXECUTIONS.get(3)),
          List.of(ALL_EXECUTIONS.get(4), ALL_EXECUTIONS.get(5))
      );
  private static final List<ExecutionOuterClass.Execution> FILTERED_EXECUTIONS =
      List.of(ALL_EXECUTIONS.get(0), ALL_EXECUTIONS.get(2), ALL_EXECUTIONS.get(4));
  private static final List<List<ExecutionOuterClass.Execution>> PAGED_FILTERED_EXECUTIONS =
      List.of(
          List.of(FILTERED_EXECUTIONS.get(0), FILTERED_EXECUTIONS.get(1)),
          List.of(FILTERED_EXECUTIONS.get(2))
      );


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
    responseObserver.onNext(
        execution(request.getId().getProject(), request.getId().getDomain(), request.getId().getName())
    );
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
    //not a real filter but it will work for detecting if we pass a filter or not
    final var pagedExecutions = request.getFilters().isEmpty()
                                     ? PAGED_EXECUTIONS
                                     : PAGED_FILTERED_EXECUTIONS;

    final var token = request.getToken();
    final var page = token.isEmpty() ? 0 : Integer.parseInt(token);
    final var lastPage = pagedExecutions.size();
    final var newToken = (page == lastPage) ? "" : Integer.toString(page + 1);
    final var executions = pagedExecutions.get(page);

    final ExecutionOuterClass.ExecutionList response = ExecutionOuterClass.ExecutionList.newBuilder()
        .addAllExecutions(executions)
        .setToken(newToken)
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
  }

  private boolean isEvenExecId(String execName) {
    return Integer.parseInt(execName.substring(EXEC_NAME_PREFIX.length())) % 2 == 0;
  }

  public static ExecutionOuterClass.Execution execution(String project, String domain, String execName1) {
    return ExecutionOuterClass.Execution
        .newBuilder()
        .setId(IdentifierOuterClass.WorkflowExecutionIdentifier
            .newBuilder()
            .setProject(project)
            .setDomain(domain)
            .setName(execName1)
            .build())
        .build();
  }

  @Override
  public void listProjects(ProjectOuterClass.ProjectListRequest request,
                           StreamObserver<ProjectOuterClass.Projects> responseObserver) {
    final var projects = ProjectOuterClass.Projects.newBuilder()
        .addProjects(ProjectOuterClass.Project.newBuilder()
            .setId(PROJECT)
            .setDescription("")
            .addDomains(ProjectOuterClass.Domain.newBuilder()
                .setId(DOMAIN)
                .build())
            .build())
        .build();
    responseObserver.onNext(projects);
    responseObserver.onCompleted();
  }
}
