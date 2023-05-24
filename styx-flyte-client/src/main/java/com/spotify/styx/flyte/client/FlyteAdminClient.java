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
import static io.grpc.Status.Code.DEADLINE_EXCEEDED;
import static io.grpc.Status.Code.INTERNAL;
import static io.grpc.Status.Code.UNKNOWN;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import flyteidl.admin.Common;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.admin.LaunchPlanOuterClass;
import flyteidl.admin.ProjectOuterClass;
import flyteidl.core.IdentifierOuterClass;
import flyteidl.service.AdminServiceGrpc;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import io.grpc.ClientInterceptor;
import io.grpc.ManagedChannelBuilder;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlyteAdminClient {

  private static final String FLYTEADMIN_HOST = "host";
  private static final String FLYTEADMIN_PORT = "port";
  private static final String FLYTEADMIN_INSECURE = "insecure";
  private static final String FLYTEADMIN_GRPC_DEADLINE_SECONDS = "grpc.deadline-seconds";
  private static final String FLYTEADMIN_MAX_RETRY_ATTEMPTS = "grpc.max-retry-attempts";
  private static final String FLYTEADMIN_RETRY_INTERVAL_BACKOFF_MULTIPLIER = "grpc.retry-wait-duration-backoff-multiplier";
  private static final String FLYTEADMIN_RETRY_INTERVAL_INITIAL_MILLIS = "grpc.retry-initial-wait-duration";
  private static final int DEFAULT_FLYTEADMIN_RETRY_INTERVAL_INITIAL_MILLIS = 200;
  private static final double DEFAULT_FLYTEADMIN_RETRY_INTERVAL_BACKOFF_MULTIPLIER = 2;

  private static final Logger LOG = LoggerFactory.getLogger(FlyteAdminClient.class);
  private static final String TRIGGERING_PRINCIPAL = "styx";
  private static final int USER_TRIGGERED_EXECUTION_NESTING = 0;
  private final long grpcDeadlineSeconds;

  private final AdminServiceGrpc.AdminServiceBlockingStub stub;

  private static final Set<Status.Code> GRPC_STATUSES_TO_RETRY =
          Set.of(DEADLINE_EXCEEDED, INTERNAL, UNKNOWN);

  private final Retry retry;

  @VisibleForTesting
  FlyteAdminClient(AdminServiceGrpc.AdminServiceBlockingStub stub,
                   long grpcDeadlineSeconds,
                   RetryConfig retry
                   ) {
    this.stub = Objects.requireNonNull(stub, "stub");
    this.grpcDeadlineSeconds = grpcDeadlineSeconds;
    this.retry = Retry.of("flyteadmin-client", Objects.requireNonNull(retry, "retry"));
  }

  public static FlyteAdminClient create(
      Config config,
      List<ClientInterceptor> interceptors,
      final String serviceName) {

    final var target = config.getString(FLYTEADMIN_HOST) + ":" + config.getInt(FLYTEADMIN_PORT);
    final var insecure = config.getBoolean(FLYTEADMIN_INSECURE);
    final var grpcDeadlineSeconds = config.getLong(FLYTEADMIN_GRPC_DEADLINE_SECONDS);
    final var maxRetryAttempts = config.getInt(FLYTEADMIN_MAX_RETRY_ATTEMPTS);
    final var initialIntervalMillis = config.hasPath(FLYTEADMIN_RETRY_INTERVAL_INITIAL_MILLIS) ? config.getInt(FLYTEADMIN_RETRY_INTERVAL_INITIAL_MILLIS): DEFAULT_FLYTEADMIN_RETRY_INTERVAL_INITIAL_MILLIS;
    final var multiplier = config.hasPath(FLYTEADMIN_RETRY_INTERVAL_BACKOFF_MULTIPLIER)? config.getDouble(FLYTEADMIN_RETRY_INTERVAL_BACKOFF_MULTIPLIER): DEFAULT_FLYTEADMIN_RETRY_INTERVAL_BACKOFF_MULTIPLIER;

    final RetryConfig retryConfig = getRetryConfig(maxRetryAttempts, initialIntervalMillis, multiplier);

    var builder = ManagedChannelBuilder.forTarget(target);

    if (insecure) {
      builder.usePlaintext();
    }
    builder.intercept(GrpcClientMetadataInterceptor.create(serviceName));
    var channel =
        builder.intercept(interceptors).build();

    return new FlyteAdminClient(
        AdminServiceGrpc.newBlockingStub(channel), grpcDeadlineSeconds, retryConfig);
  }

  public static RetryConfig getRetryConfig(int maxRetryAttempts, int initialIntervalMillis, double multiplier) {
    return
            RetryConfig.custom()
                    .maxAttempts(maxRetryAttempts)
                    .intervalFunction(
                            IntervalFunction.ofExponentialBackoff(
                                    initialIntervalMillis,
                                    multiplier
                            )
                    )
                    .retryOnException(
                            e -> {
                              if (e instanceof StatusRuntimeException) {
                                var status = ((StatusRuntimeException) e).getStatus();
                                return GRPC_STATUSES_TO_RETRY.contains(status.getCode());
                              }
                              return true;
                            })
                    .build();
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
      Map<String, String> styxVariables,
      Map<String, String> triggerParams) {
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
            .setLabels(Common.Labels.newBuilder().putAllValues(labels).build())
            .setAnnotations(Common.Annotations.newBuilder().putAllValues(annotations).build())
            .build();

    var response = retry.executeSupplier(() -> stub.withDeadlineAfter(grpcDeadlineSeconds, TimeUnit.SECONDS).createExecution(
            ExecutionOuterClass.ExecutionCreateRequest.newBuilder()
                    .setDomain(domain)
                    .setProject(project)
                    .setName(name)
                    .setSpec(spec)
                    .setInputs(
                            fillParameterInInputs(inputs, userDefinedInputs, styxVariables, triggerParams))
                    .build()));

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
    var request = Common.ObjectGetRequest.newBuilder().setId(launchPlanId).build();
    return retry.executeSupplier(() -> stub.withDeadlineAfter(grpcDeadlineSeconds, TimeUnit.SECONDS).getLaunchPlan(request));
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
    return retry.executeSupplier(() -> stub.withDeadlineAfter(grpcDeadlineSeconds, TimeUnit.SECONDS).getExecution(request));
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

    return retry.executeSupplier(() -> stub.withDeadlineAfter(grpcDeadlineSeconds, TimeUnit.SECONDS).terminateExecution(request));
  }

  public ExecutionOuterClass.ExecutionList listExecutions(
      String project, String domain, int limit, String token, String filters) {
    LOG.debug("listExecutions {} {} {} {}", project, domain, limit, token);

    final var request =
        Common.ResourceListRequest.newBuilder()
            .setId(
                Common.NamedEntityIdentifier.newBuilder()
                    .setProject(project)
                    .setDomain(domain)
                    .build())
            .setLimit(limit)
            .setToken(nullToEmpty(token))
            .setFilters(nullToEmpty(filters))
            // TODO: .setSortBy()
            .build();

    return retry.executeSupplier(() -> stub.withDeadlineAfter(grpcDeadlineSeconds, TimeUnit.SECONDS).listExecutions(request));
  }

  public ProjectOuterClass.Projects listProjects() {
    LOG.debug("listProjects");

    return retry.executeSupplier(() -> stub.withDeadlineAfter(grpcDeadlineSeconds, TimeUnit.SECONDS).listProjects(ProjectOuterClass.ProjectListRequest.getDefaultInstance()));
  }
}
