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

import static com.spotify.styx.flyte.client.TestAdminService.EXEC_NAME_PREFIX;
import static com.spotify.styx.flyte.client.TestAdminService.PAGE_SIZE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.ImmutableMap;
import flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode;
import flyteidl.core.IdentifierOuterClass;
import flyteidl.service.AdminServiceGrpc;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.testing.GrpcCleanupRule;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class FlyteAdminClientTest {

  static final String PROJECT = "styx_flyte_test";
  static final String DOMAIN = "testing";
  static final String EXISTING_NAME = EXEC_NAME_PREFIX + 1;
  static final String NON_EXISTING_NAME = EXEC_NAME_PREFIX + 8;
  static final String LP_NAME = "launch_plan_name";
  static final String LP_VERSION = "launch_plan_version_1";
  static final IdentifierOuterClass.ResourceType LP_RESOURCE_TYPE = IdentifierOuterClass.ResourceType.LAUNCH_PLAN;
  static final Map<String, String> LABELS = ImmutableMap.of("label-key", "id");
  static final Map<String, String> ANNOTATIONS = ImmutableMap.of("annotation-key", "value");
  static final Map<String, String> EXTRA_DEFAULT_INPUTS = ImmutableMap.of(TestAdminService.EXTRA_PARAMETER_NAME, Instant.now().toString());
  private FlyteAdminClient flyteAdminClient;

  @Rule public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();

  private static final IdentifierOuterClass.Identifier LP_IDENTIFIER =
      IdentifierOuterClass.Identifier.newBuilder()
          .setResourceType(LP_RESOURCE_TYPE)
          .setDomain(DOMAIN)
          .setProject(PROJECT)
          .setName(LP_NAME)
          .setVersion(LP_VERSION).build();

  @Before
  public void setUp() throws IOException {
    var testAdminService = new TestAdminService();
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
        flyteAdminClient.createExecution(PROJECT, DOMAIN, NON_EXISTING_NAME, identifier(NON_EXISTING_NAME),
            ExecutionMode.SCHEDULED, Map.of(), Map.of(), Map.of(), EXTRA_DEFAULT_INPUTS);
    assertThat(workflowExecution.getId().getProject(), equalTo(PROJECT));
    assertThat(workflowExecution.getId().getDomain(), equalTo(DOMAIN));
    assertThat(workflowExecution.getId().getName(), equalTo(NON_EXISTING_NAME));
  }

  @Test
  public void shouldPropagateLabelsAndAnnotationsOnCreateExecutionToStub() {
    var workflowExecution =
        flyteAdminClient.createExecution(PROJECT, DOMAIN, NON_EXISTING_NAME, identifier(NON_EXISTING_NAME),
            ExecutionMode.SCHEDULED, LABELS, ANNOTATIONS, Map.of(), EXTRA_DEFAULT_INPUTS);
    assertThat(workflowExecution, notNullValue());

    var retrievedExecution = flyteAdminClient.getExecution(PROJECT, DOMAIN, NON_EXISTING_NAME);
    assertThat(retrievedExecution.getSpec().getLabels().getValuesMap(), equalTo(LABELS));
    assertThat(retrievedExecution.getSpec().getAnnotations().getValuesMap(), equalTo(ANNOTATIONS));
  }

  @Test
  public void shouldPropagateGetExecutionToStub() {
    var workflowExecution = flyteAdminClient.getExecution(PROJECT, DOMAIN, EXISTING_NAME);
    assertThat(workflowExecution.getId().getProject(), equalTo(PROJECT));
    assertThat(workflowExecution.getId().getDomain(), equalTo(DOMAIN));
    assertThat(workflowExecution.getId().getName(), equalTo(EXISTING_NAME));
  }

  @Test
  public void shouldPropagateGetLaunchPlanToStub() {
    var launchPlan =
        flyteAdminClient.getLaunchPlan(LP_IDENTIFIER);
    assertThat(LP_RESOURCE_TYPE, equalTo(launchPlan.getId().getResourceType()));
    assertThat(PROJECT, equalTo(launchPlan.getId().getProject()));
    assertThat(DOMAIN, equalTo(launchPlan.getId().getDomain()));
    assertThat(LP_NAME, equalTo(launchPlan.getId().getName()));
    assertThat(LP_VERSION, equalTo(launchPlan.getId().getVersion()));
  }

  @Test
  public void shouldPropagateTerminateExecutionToStub() {
    var terminationResponse =
        flyteAdminClient.terminateExecution(PROJECT, DOMAIN, EXISTING_NAME, "Cause of termination");
    assertThat(terminationResponse, notNullValue());
  }

  @Test
  public void shouldPropagateListExecutionToStub() {
    var listExecutions =
        flyteAdminClient.listExecutions(PROJECT, DOMAIN, 100, null, null);

    listExecutions.getExecutionsList().forEach(
        e -> {
          assertThat(e.getId().getProject(), equalTo(PROJECT));
          assertThat(e.getId().getDomain(), equalTo(DOMAIN));
        }
    );
    assertThat(2, equalTo(listExecutions.getExecutionsCount()));
  }

  @Test
  public void shouldReturnFirstPageWhenTokenIsNullOnListExecutions() {
    final var response = flyteAdminClient.listExecutions(PROJECT, DOMAIN, 100, null, null);

    final var executionNames = response.getExecutionsList().stream()
        .map(exec -> exec.getId().getName())
        .collect(Collectors.toUnmodifiableList());
    assertThat(executionNames, hasSize(PAGE_SIZE));
    assertThat(executionNames, hasItems(EXEC_NAME_PREFIX + 1, EXEC_NAME_PREFIX + 2));
  }

  @Test
  public void shouldNotReturnTheFirstPageWhenTokenIsNotNullOnListExecutions() {
    final var response = flyteAdminClient.listExecutions(PROJECT, DOMAIN, 100, "1", null);

    final var executionNames = response.getExecutionsList().stream()
        .map(exec -> exec.getId().getName())
        .collect(Collectors.toUnmodifiableList());
    assertThat(executionNames, hasSize(PAGE_SIZE));
    assertThat(executionNames, not(hasItems(EXEC_NAME_PREFIX + 1, EXEC_NAME_PREFIX + 2)));
  }

  @Test
  public void shouldReturnPageWithFilteredExecutionsWhenFilterIsNotNullOnListExecutions() {
    final var response = flyteAdminClient.listExecutions(PROJECT, DOMAIN, 100, null, "remove-ods");

    final var executionNames = response.getExecutionsList().stream()
        .map(exec -> exec.getId().getName())
        .collect(Collectors.toUnmodifiableList());
    assertThat(executionNames, hasItems(EXEC_NAME_PREFIX + 1, EXEC_NAME_PREFIX + 3));
  }

  @Test
  public void shouldPropagateListProjectsToStub() {
    var listProjectsResponse =
        flyteAdminClient.listProjects();

    assertThat(listProjectsResponse.getProjectsList(), hasSize(1));
    assertThat(listProjectsResponse.getProjectsList().get(0).getId(), equalTo(PROJECT));
    assertThat(listProjectsResponse.getProjectsList().get(0).getDomains(0).getId(), equalTo(DOMAIN));
  }

  private static IdentifierOuterClass.Identifier identifier(String name) {
    return IdentifierOuterClass.Identifier.newBuilder()
        .setResourceType(IdentifierOuterClass.ResourceType.LAUNCH_PLAN)
        .setDomain(DOMAIN)
        .setProject(PROJECT)
        .setName(name)
        .setVersion(LP_VERSION).build();
  }
}
