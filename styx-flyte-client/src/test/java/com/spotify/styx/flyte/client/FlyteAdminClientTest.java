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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import flyteidl.admin.ExecutionOuterClass;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class FlyteAdminClientTest {

  private static String DOMAIN = "testing";
  private static String PROJECT = "styx_flyte_test";
  private static String LP_NAME = "launch_plan_1";
  private static String LP_VERSION = "launch_plan_version_1";
  static String WF_EXECUTION_ID = "wf_execution_id_1";
  private FlyteAdminClient flyteAdminClient;
  private TestAdminService testAdminService;

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private static final LaunchPlanIdentifier LP_IDENTIFIER =
      LaunchPlanIdentifier.create(DOMAIN, PROJECT, LP_NAME, LP_VERSION);

  @Before
  public void setup() throws IOException {
    testAdminService = new TestAdminService();
    String serverName = InProcessServerBuilder.generateName();
    final Server server = InProcessServerBuilder
        .forName(serverName)
        .directExecutor()
        .addService(testAdminService)
        .build();
    final ManagedChannel channel = InProcessChannelBuilder
        .forName(serverName)
        .directExecutor()
        .build();

    flyteAdminClient =
        new FlyteAdminClient(AdminServiceGrpc.newBlockingStub(channel), channel);

    grpcCleanup.register(server.start());
    grpcCleanup.register(channel);
  }

  @After
  public void tearDown() {
  }

  @Test
  public void shouldPropagateCreateExecutionToStub() {
    final WorkflowExecutionIdentifier flyteExecution =
        flyteAdminClient.createExecution(DOMAIN, PROJECT, LP_IDENTIFIER,
            ExecutionOuterClass.ExecutionMetadata.ExecutionMode.SCHEDULED);
    assertThat(DOMAIN, equalTo(flyteExecution.domain()));
    assertThat(PROJECT, equalTo(flyteExecution.project()));
    assertThat(WF_EXECUTION_ID, equalTo(flyteExecution.name()));
  }
}
