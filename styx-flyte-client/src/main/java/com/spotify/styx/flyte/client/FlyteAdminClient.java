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

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Verify.verifyNotNull;
import static com.spotify.styx.flyte.client.FlyteInputsUtils.fillParameterInInputs;

import com.google.common.annotations.VisibleForTesting;
import flyteidl.admin.Common;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.admin.LaunchPlanOuterClass;
import flyteidl.admin.ProjectOuterClass;
import flyteidl.core.IdentifierOuterClass;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannelBuilder;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlyteAdminClient {

  private static final Logger LOG = LoggerFactory.getLogger(FlyteAdminClient.class);
  private static final String TRIGGERING_PRINCIPAL = "styx";
  private static final int USER_TRIGGERED_EXECUTION_NESTING = 0;
  private static final int MAX_RETRY_ATTEMPTS = 3;

  private final AdminServiceGrpc.AdminServiceBlockingStub stub;

  @VisibleForTesting
  FlyteAdminClient(AdminServiceGrpc.AdminServiceBlockingStub stub) {
    this.stub = Objects.requireNonNull(stub, "stub");
  }

  public static FlyteAdminClient create(String target, boolean insecure,
                                        List<ClientInterceptor> interceptors) {
    var builder = ManagedChannelBuilder.forTarget(target);

    if (insecure) {
      builder.usePlaintext();
    }
    // Enable transparent retries:
    // https://github.com/grpc/proposal/blob/master/A6-client-retries.md#transparent-retries
    var channel = builder
        .enableRetry()
        .maxRetryAttempts(MAX_RETRY_ATTEMPTS)
        .intercept(interceptors)
        .build();

    return new FlyteAdminClient(AdminServiceGrpc.newBlockingStub(channel));
  }

  public ExecutionOuterClass.ExecutionCreateResponse createExecution(
      String project,
      String domain,
      String name,
      IdentifierOuterClass.Identifier launchPlanId,
      ExecutionOuterClass.ExecutionMetadata.ExecutionMode executionMode,
      Map<String, String> labels,
      Map<String, String> annotations,
      Map<String, String> userDefinedInputs,
      Map<String, String> styxVariables) {
    LOG.debug("createExecution {} {} {}", project, domain, launchPlanId);

    var metadata =
        ExecutionOuterClass.ExecutionMetadata.newBuilder()
            .setMode(executionMode)
            .setPrincipal(TRIGGERING_PRINCIPAL)
            .setNesting(USER_TRIGGERED_EXECUTION_NESTING)
            .build();

    var launchPlan = getLaunchPlan(launchPlanId);

    var inputs = launchPlan.getSpec().getDefaultInputs();

    var spec =
        ExecutionOuterClass.ExecutionSpec.newBuilder()
            .setLaunchPlan(launchPlanId)
            .setMetadata(metadata)
            .setLabels(Common.Labels.newBuilder()
                .putAllValues(labels)
                .build())
            .setAnnotations(Common.Annotations.newBuilder()
                .putAllValues(annotations)
                .build())
            .build();

    var response =
        stub.createExecution(
            ExecutionOuterClass.ExecutionCreateRequest.newBuilder()
                .setDomain(domain)
                .setProject(project)
                .setName(name)
                .setSpec(spec)
                .setInputs(fillParameterInInputs(inputs, userDefinedInputs, styxVariables))
                .build());

    verifyNotNull(
        response,
        "Unexpected null response when creating execution %s on project %s domain %s for %s",
        name,
        project,
        domain,
        launchPlanId);

    return response;
  }

  LaunchPlanOuterClass.LaunchPlan getLaunchPlan(IdentifierOuterClass.Identifier launchPlanId) {
    LOG.debug("getLaunchPlan {}", launchPlanId);
    var request = Common.ObjectGetRequest.newBuilder()
        .setId(launchPlanId)
        .build();
    return stub.getLaunchPlan(request);
  }

  public ExecutionOuterClass.Execution getExecution(String project, String domain, String name) {
    LOG.debug("getExecution {} {} {}", project, domain, name);
    var request =
        ExecutionOuterClass.WorkflowExecutionGetRequest.newBuilder()
            .setId(
                IdentifierOuterClass.WorkflowExecutionIdentifier.newBuilder()
                    .setProject(project)
                    .setDomain(domain)
                    .setName(name)
                    .build())
            .build();
    return stub.getExecution(request);
  }

  public ExecutionOuterClass.ExecutionTerminateResponse terminateExecution(
      String project, String domain, String name, String cause) {
    LOG.debug("terminateExecution {} {} {}", project, domain, name);

    final var request =
        ExecutionOuterClass.ExecutionTerminateRequest.newBuilder()
            .setId(
                IdentifierOuterClass.WorkflowExecutionIdentifier.newBuilder()
                    .setProject(project)
                    .setDomain(domain)
                    .setName(name)
                    .build())
            .setCause(cause)
            .build();

    return stub.terminateExecution(request);
  }

  public ExecutionOuterClass.ExecutionList listExecutions(
      String project,
      String domain,
      int limit,
      String token,
      String filters) {
    LOG.debug("listExecutions {} {} {} {}", project, domain, limit, token);

    final var request = Common.ResourceListRequest
        .newBuilder()
        .setId(Common.NamedEntityIdentifier
            .newBuilder()
            .setProject(project)
            .setDomain(domain)
            .build())
        .setLimit(limit)
        .setToken(nullToEmpty(token))
        .setFilters(nullToEmpty(filters))
        // TODO: .setSortBy()
        .build();

    return stub.listExecutions(request);
  }

  public ProjectOuterClass.Projects listProjects() {
    LOG.debug("listProjects");

    return stub.listProjects(ProjectOuterClass.ProjectListRequest.getDefaultInstance());
  }
}
