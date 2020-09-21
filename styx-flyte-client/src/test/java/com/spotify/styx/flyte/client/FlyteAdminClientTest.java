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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

import flyteidl.admin.ExecutionOuterClass;
import flyteidl.core.IdentifierOuterClass;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class FlyteAdminClientTest {

  static final String PROJECT = "styx_flyte_test";
  static final String DOMAIN = "testing";
  static final String NAME = "execution_name";
  static final String LP_NAME = "launch_plan_1";
  static final String LP_VERSION = "launch_plan_version_1";
  private FlyteAdminClient flyteAdminClient;

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
  private static final IdentifierOuterClass.Identifier LP_IDENTIFIER =
      IdentifierOuterClass.Identifier.newBuilder()
          .setResourceType(IdentifierOuterClass.ResourceType.LAUNCH_PLAN)
          .setDomain(DOMAIN)
          .setProject(PROJECT)
          .setName(LP_NAME)
          .setVersion(LP_VERSION).build();

  @Before
  public void setUp() throws IOException {
    final TestAdminService testAdminService = new TestAdminService();
    var serverName = InProcessServerBuilder.generateName();
    var server = InProcessServerBuilder
        .forName(serverName)
        .directExecutor()
        .addService(testAdminService)
        .build();
    var channel = InProcessChannelBuilder
        .forName(serverName)
        .directExecutor()
        .build();

    flyteAdminClient =
        new FlyteAdminClient(AdminServiceGrpc.newBlockingStub(channel));

    grpcCleanup.register(server.start());
    grpcCleanup.register(channel);
  }

  @Test
  public void shouldPropagateCreateExecutionToStub() {
    var workflowExecution =
        flyteAdminClient.createExecution(PROJECT, DOMAIN, NAME, LP_IDENTIFIER,
            ExecutionOuterClass.ExecutionMetadata.ExecutionMode.SCHEDULED);
    assertThat(PROJECT, equalTo(workflowExecution.getId().getProject()));
    assertThat(DOMAIN, equalTo(workflowExecution.getId().getDomain()));
    assertThat(NAME, equalTo(workflowExecution.getId().getName()));
  }

  @Test
  public void shouldPropagateGetExecutionToStub() {
    var workflowExecution =
        flyteAdminClient.getExecution(PROJECT, DOMAIN, NAME);
    assertThat(PROJECT, equalTo(workflowExecution.getId().getProject()));
    assertThat(DOMAIN, equalTo(workflowExecution.getId().getDomain()));
    assertThat(NAME, equalTo(workflowExecution.getId().getName()));
  }

  @Test
  public void shouldPropagateTerminateExecutionToStub() {
    var terminationResponse =
        flyteAdminClient.terminateExecution(PROJECT, DOMAIN, LP_NAME, "Cause of termination");
    assertThat(terminationResponse, notNullValue());
  }

  @Test
  public void shouldPropagateListExecutionToStub() {
    var listExecutions =
        flyteAdminClient.listExecutions(PROJECT, DOMAIN, 100, "token", "filters");

    listExecutions.getExecutionsList().forEach(
        e -> {
          assertThat(PROJECT, equalTo(e.getId().getProject()));
          assertThat(DOMAIN, equalTo(e.getId().getDomain()));
        }
    );
    assertThat(2, equalTo(listExecutions.getExecutionsCount()));
  }

  @Test
  public void shouldPropagateListProjectsToStub() {
    var listProjectsResponse =
        flyteAdminClient.listProjects();

    assertThat(listProjectsResponse.getProjectsList(), hasSize(1));
    assertThat(listProjectsResponse.getProjectsList().get(0).getId(), equalTo(PROJECT));
    assertThat(listProjectsResponse.getProjectsList().get(0).getDomains(0).getId(), equalTo(DOMAIN));
  }
}
