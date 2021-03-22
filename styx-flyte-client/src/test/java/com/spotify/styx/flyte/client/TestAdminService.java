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
import static java.util.stream.Collectors.toUnmodifiableList;

import flyteidl.admin.Common;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.admin.ExecutionOuterClass.Execution;
import flyteidl.admin.ProjectOuterClass;

import flyteidl.admin.LaunchPlanOuterClass;
import flyteidl.core.IdentifierOuterClass;
import flyteidl.core.Interface;
import flyteidl.core.Types;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

public class TestAdminService extends AdminServiceGrpc.AdminServiceImplBase  {

  static final String EXEC_NAME_PREFIX = "exec_name_";
  static final String EXTRA_PARAMETER_NAME = "EXTRA_PARAMETER";
  static final int PAGE_SIZE = 2;

  private final List<Execution> allExecutions =
      new ArrayList<>(List.of(
          execution(EXEC_NAME_PREFIX + 1),
          execution(EXEC_NAME_PREFIX + 2),
          execution(EXEC_NAME_PREFIX + 3),
          execution(EXEC_NAME_PREFIX + 4),
          execution(EXEC_NAME_PREFIX + 5),
          execution(EXEC_NAME_PREFIX + 6)
      ));
  private static final Predicate<Execution> NO_FILTER = __ -> true;
  private static final Predicate<Execution> ONLY_ODDS = ex -> {
    var execNumber = ex.getId().getName().substring(EXEC_NAME_PREFIX.length());
    return Integer.parseInt(execNumber) % 2 == 1;
  };


  @Override
  public void createExecution(final ExecutionOuterClass.ExecutionCreateRequest request,
                              final StreamObserver<ExecutionOuterClass.ExecutionCreateResponse> responseObserver) {
    allExecutions.add(execution(request));
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
  public void getLaunchPlan(final flyteidl.admin.Common.ObjectGetRequest request,
                            final StreamObserver<LaunchPlanOuterClass.LaunchPlan> responseObserver) {
    responseObserver.onNext(LaunchPlanOuterClass.LaunchPlan
        .newBuilder()
        .setId(request.getId())
        .setSpec(LaunchPlanOuterClass.LaunchPlanSpec
            .newBuilder()
            .setDefaultInputs(Interface.ParameterMap.newBuilder()
                .putParameters(EXTRA_PARAMETER_NAME, Interface.Parameter.newBuilder()
                    .setVar(Interface.Variable.newBuilder()
                        .setType(Types.LiteralType.newBuilder().
                            setSimple(Types.SimpleType.DATETIME).build())
                        .build())
                    .setRequired(true)
                    .build())
                .build())
            .build())
        .build());
    responseObserver.onCompleted();
  }

  @Override
  public void getExecution(final ExecutionOuterClass.WorkflowExecutionGetRequest request,
                           final StreamObserver<Execution> responseObserver) {
    findAnyExecution(request.getId()).ifPresentOrElse(
        ex -> {
          responseObserver.onNext(ex);
          responseObserver.onCompleted();
        },
        () -> responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND))
    );
  }

  @Override
  public void terminateExecution(final ExecutionOuterClass.ExecutionTerminateRequest request,
                                 final StreamObserver<ExecutionOuterClass.ExecutionTerminateResponse> responseObserver) {
    findAnyExecution(request.getId()).ifPresentOrElse(
        ex -> {
          allExecutions.remove(ex);
          responseObserver.onNext(ExecutionOuterClass.ExecutionTerminateResponse.getDefaultInstance());
          responseObserver.onCompleted();
        },
        () -> responseObserver.onError(new StatusRuntimeException(Status.NOT_FOUND))
    );
  }

  @Override
  public void listExecutions(final Common.ResourceListRequest request,
                             final StreamObserver<ExecutionOuterClass.ExecutionList> responseObserver) {
    //not a real filter but it will work for detecting if we pass a filter or not
    final var filter = request.getFilters().isEmpty()
                                     ? NO_FILTER
                                     : ONLY_ODDS;


    final var token = request.getToken();
    final var page = token.isEmpty() ? 0 : Integer.parseInt(token);

    final var filteredExecutions = allExecutions.stream()
        .filter(filter)
        .collect(toUnmodifiableList());

    final var lastPage = filteredExecutions.size() / PAGE_SIZE;
    final var newToken = (page == lastPage) ? "" : Integer.toString(page + 1);
    final var executions = filteredExecutions.stream()
        .skip(page * PAGE_SIZE)
        .limit(PAGE_SIZE)
        .collect(toUnmodifiableList());

    final ExecutionOuterClass.ExecutionList response = ExecutionOuterClass.ExecutionList.newBuilder()
        .addAllExecutions(executions)
        .setToken(newToken)
        .build();
    responseObserver.onNext(response);
    responseObserver.onCompleted();
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

  private Execution execution(ExecutionOuterClass.ExecutionCreateRequest request) {
    return execution(request.getProject(), request.getDomain(), request.getName(),
        request.getSpec().getLabels().getValuesMap());
  }

  private static Execution execution(String execName) {
    return execution(FlyteAdminClientTest.PROJECT, FlyteAdminClientTest.DOMAIN, execName, Map.of());
  }

  private static Execution execution(String project, String domain, String execName, Map<String, String> labels) {
    return Execution
        .newBuilder()
        .setId(IdentifierOuterClass.WorkflowExecutionIdentifier
            .newBuilder()
            .setProject(project)
            .setDomain(domain)
            .setName(execName)
            .build())
        .setSpec(ExecutionOuterClass.ExecutionSpec.newBuilder()
            .setLabels(Common.Labels.newBuilder()
                .putAllValues(labels)
                .build())
            .build())
        .build();
  }

  public Optional<Execution> findAnyExecution(IdentifierOuterClass.WorkflowExecutionIdentifier searchId) {
    return allExecutions.stream()
        .filter(ex -> searchId.equals(ex.getId()))
        .findAny();
  }
}
