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

import static com.google.common.base.Verify.verifyNotNull;

import com.google.common.annotations.VisibleForTesting;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.ManagedChannelBuilder;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlyteAdminClient {

  private static final Logger log = LoggerFactory.getLogger(FlyteAdminClient.class);
  private static final String TRIGGERING_PRINCIPAL = "styx";
  private static final int USER_TRIGGERED_EXECUTION_NESTING = 0;

  private final AdminServiceGrpc.AdminServiceBlockingStub stub;

  @VisibleForTesting
  FlyteAdminClient(AdminServiceGrpc.AdminServiceBlockingStub stub) {
    this.stub = Objects.requireNonNull(stub, "stub");
  }

  public static FlyteAdminClient create(String target, boolean insecure) {
    var builder = ManagedChannelBuilder.forTarget(target);

    if (insecure) {
      builder.usePlaintext();
    }

    var channel = builder.build();

    return new FlyteAdminClient(AdminServiceGrpc.newBlockingStub(channel));
  }

  public WorkflowExecutionIdentifier createExecution(String domain, String project,
                                              LaunchPlanIdentifier launchPlanId,
                                              ExecutionMode executionMode) {
    log.debug("createExecution {} {} {}", domain, project, launchPlanId);

    var metadata =
        ExecutionOuterClass.ExecutionMetadata.newBuilder()
            .setMode(executionMode.toProto())
            .setPrincipal(TRIGGERING_PRINCIPAL)
            .setNesting(USER_TRIGGERED_EXECUTION_NESTING)
            .build();

    var spec =
        ExecutionOuterClass.ExecutionSpec.newBuilder()
            .setLaunchPlan(launchPlanId.toProto())
            .setMetadata(metadata)
            .build();

    var response =
        stub.createExecution(
            ExecutionOuterClass.ExecutionCreateRequest.newBuilder()
                .setDomain(domain)
                .setProject(project)
                .setSpec(spec)
                .build());

    verifyNotNull(
        response,
        "Unexpected null response when creating execution %s on project %s domain %s",
        launchPlanId,
        project,
        domain);

    return WorkflowExecutionIdentifier.fromProto(response);
  }
}
