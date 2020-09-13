/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx.flyte;

import static com.spotify.styx.model.Schedule.HOURS;
import static com.spotify.styx.state.RunState.MISSING_DEPS_EXIT_CODE;
import static com.spotify.styx.state.RunState.SUCCESS_EXIT_CODE;
import static com.spotify.styx.state.RunState.UNKNOWN_ERROR_EXIT_CODE;
import static com.spotify.styx.state.RunState.UNRECOVERABLE_FAILURE_EXIT_CODE;
import static flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode.SCHEDULED;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.spotify.styx.flyte.client.FlyteAdminClient;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.model.FlyteIdentifier;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.util.IsClosedException;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.core.Execution;
import flyteidl.core.IdentifierOuterClass;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class FlyteAdminClientRunnerTest {

  private static final FlyteIdentifier LAUNCH_PLAN_IDENTIFIER = FlyteIdentifier.builder()
      .project("flyte-test")
      .domain("testing")
      .name("wf-name")
      .version("1234")
      .resourceType("lp")
      .build();
  private static final FlyteExecConf FLYTE_EXEC_CONF = FlyteExecConf.builder()
      .referenceId(LAUNCH_PLAN_IDENTIFIER)
      .build();

  private static final String RUNNER_ID = "flyte-runner";
  private static final WorkflowInstance WORKFLOW_INSTANCE = createWorkflowInstance();
  private static final FlyteExecutionId FLYTE_EXECUTION_ID =
      FlyteExecutionId.create("flyte-test", "testing", "execution-name");
  private static final RunState RUN_STATE = runState();

  @Mock private FlyteAdminClient flyteAdminClient;
  @Mock private StateManager stateManager;

  private FlyteAdminClientRunner flyteRunner;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    flyteRunner = new FlyteAdminClientRunner(RUNNER_ID, flyteAdminClient, stateManager);
  }

  @Test
  public void testIsEnabled() {
    assertTrue(flyteRunner.isEnabled());
  }

  @Test
  public void testCreateExecution() throws FlyteRunner.CreateExecutionException {
    final var execName = "test-create-execution";
    when(flyteAdminClient.createExecution(any(), any(), any(), any(),any())).thenReturn(
        ExecutionOuterClass.ExecutionCreateResponse
            .newBuilder()
            .setId(IdentifierOuterClass.WorkflowExecutionIdentifier
                .newBuilder()
                .setProject(LAUNCH_PLAN_IDENTIFIER.project())
                .setDomain(LAUNCH_PLAN_IDENTIFIER.domain())
                .setName(execName)
                .build())
            .build());

    var runnerId = flyteRunner.createExecution(RUN_STATE, execName, FLYTE_EXEC_CONF);

    assertThat(runnerId, is(RUNNER_ID));
    verify(flyteAdminClient).createExecution(
        LAUNCH_PLAN_IDENTIFIER.project(), LAUNCH_PLAN_IDENTIFIER.domain(), execName,
        toProto(LAUNCH_PLAN_IDENTIFIER), SCHEDULED);
  }

  private IdentifierOuterClass.Identifier toProto(FlyteIdentifier identifier) {
    return IdentifierOuterClass.Identifier.newBuilder()
        .setResourceType(IdentifierOuterClass.ResourceType.LAUNCH_PLAN)
        .setProject(identifier.project())
        .setDomain(identifier.domain())
        .setName(identifier.name())
        .setVersion(identifier.version())
        .build();
  }

  @Test
  public void testThrowsFlyteLaunchPlanNotFound() {
    doThrow(new StatusRuntimeException(Status.NOT_FOUND))
        .when(flyteAdminClient).createExecution(any(), any(), any(), any(),any());

    assertThrows(
        FlyteRunner.LaunchPlanNotFound.class,
        () -> flyteRunner.createExecution(RUN_STATE, "exec", FLYTE_EXEC_CONF));
  }

  @Test
  public void testThrowsCreateExecutionExceptionForOtherCode() {
    doThrow(new StatusRuntimeException(Status.INTERNAL))
        .when(flyteAdminClient).createExecution(any(), any(), any(), any(),any());

    assertThrows(
        FlyteRunner.CreateExecutionException.class,
        () -> flyteRunner.createExecution(RUN_STATE, "exec", FLYTE_EXEC_CONF));
  }

  @Test
  public void testThrowsCreateExecutionExceptionForUnknownException() {
    doThrow(new IllegalStateException("test"))
        .when(flyteAdminClient).createExecution(any(), any(), any(), any(),any());

    assertThrows(
        FlyteRunner.CreateExecutionException.class,
        () -> flyteRunner.createExecution(RUN_STATE, "exec", FLYTE_EXEC_CONF));
  }

  @Test
  public void testPollFlyteExecutionIdCannotBeNull() {
    assertThrows(
        NullPointerException.class,
        () -> flyteRunner.poll(null, RUN_STATE)
    );
  }

  @Test
  public void testPollRunStateCannotBeNull() {
    assertThrows(
        NullPointerException.class,
        () -> flyteRunner.poll(FLYTE_EXECUTION_ID, null)
    );
  }

  @Test
  public void testTransitionRunningToTerminateSuccessfulRun() throws Exception {
    when(flyteAdminClient.getExecution("flyte-test", "testing", "execution-name")).thenReturn(
        ExecutionOuterClass.Execution
            .newBuilder()
            .setClosure(ExecutionOuterClass.ExecutionClosure.newBuilder()
                    .setPhase(Execution.WorkflowExecution.Phase.SUCCEEDED).build())
            .build());

    flyteRunner.poll(FLYTE_EXECUTION_ID, RUN_STATE);
    verify(stateManager,  timeout(60_000)).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(0)));
  }

  @Test
  @Parameters(method = "parametersForTestTransitionRunningToTerminateExitCodes")
  public void testTransitionRunningToTerminateExitCodes(Execution.WorkflowExecution.Phase phase, String flyteExitCode, int exitCode) throws Exception {
    WorkflowInstance workflowInstance = createWorkflowInstance();

    when(flyteAdminClient.getExecution("flyte-test", "testing", "execution-name")).thenReturn(
        ExecutionOuterClass.Execution
            .newBuilder()
            .setClosure(ExecutionOuterClass.ExecutionClosure.newBuilder().setError(
                Execution.ExecutionError
                    .newBuilder()
                    .setCode(flyteExitCode)
                    .build())
                .setPhase(phase).build())
            .build());

    flyteRunner.poll(FLYTE_EXECUTION_ID, RUN_STATE);
    verify(stateManager,  timeout(60_000)).receive(Event.terminate(workflowInstance, Optional.of(exitCode)));
  }

  private Object[] parametersForTestTransitionRunningToTerminateExitCodes() {
    return new Object[] {
        new Object[] { Execution.WorkflowExecution.Phase.FAILED, "USER:NotReady", MISSING_DEPS_EXIT_CODE },
        new Object[] { Execution.WorkflowExecution.Phase.ABORTED, "USER:NotReady", MISSING_DEPS_EXIT_CODE },
        new Object[] { Execution.WorkflowExecution.Phase.TIMED_OUT, "USER:NotReady", MISSING_DEPS_EXIT_CODE },
        new Object[] { Execution.WorkflowExecution.Phase.FAILED, "USER:NotRetryable", UNRECOVERABLE_FAILURE_EXIT_CODE },
        new Object[] { Execution.WorkflowExecution.Phase.ABORTED, "USER:NotRetryable", UNRECOVERABLE_FAILURE_EXIT_CODE},
        new Object[] { Execution.WorkflowExecution.Phase.TIMED_OUT, "USER:NotRetryable", UNRECOVERABLE_FAILURE_EXIT_CODE},
        new Object[] { Execution.WorkflowExecution.Phase.FAILED, "USER:AnythingElse", UNKNOWN_ERROR_EXIT_CODE },
        new Object[] { Execution.WorkflowExecution.Phase.ABORTED, "USER:AnythingElse", UNKNOWN_ERROR_EXIT_CODE},
        new Object[] { Execution.WorkflowExecution.Phase.TIMED_OUT, "USER:AnythingElse", UNKNOWN_ERROR_EXIT_CODE},
    };
  }

  @Test
  public void testPollingExceptionFlyteAdminClientExecutionNotFound() {
    doThrow(new StatusRuntimeException(Status.NOT_FOUND))
        .when(flyteAdminClient).getExecution(anyString(), anyString(), anyString());

    assertThrows(
        FlyteRunner.ExecutionNotFoundException.class,
        () -> flyteRunner.poll(FLYTE_EXECUTION_ID, RUN_STATE));
  }

  @Test
  public void testPollingExceptionFlyteAdminClientExecution() {
    doThrow(new StatusRuntimeException(Status.INTERNAL))
        .when(flyteAdminClient).getExecution(anyString(), anyString(), anyString());

    assertThrows(
        FlyteRunner.PollingException.class,
        () -> flyteRunner.poll(FLYTE_EXECUTION_ID, RUN_STATE));
  }

  @Test
  public void testPollingException() {
    doThrow(new RuntimeException())
        .when(flyteAdminClient).getExecution(anyString(), anyString(), anyString());

    assertThrows(
        FlyteRunner.PollingException.class,
        () -> flyteRunner.poll(FLYTE_EXECUTION_ID, RUN_STATE));
  }

  @Test
  public void testEmitFlyteEventsExceptionHandling() throws IsClosedException {
    doThrow(new IsClosedException())
        .when(stateManager).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(SUCCESS_EXIT_CODE)));

    assertThrows(
        IsClosedException.class,
        () -> flyteRunner.emitFlyteEvents(ExecutionOuterClass.Execution.newBuilder().setClosure(
            ExecutionOuterClass.ExecutionClosure.newBuilder().setPhase(
                Execution.WorkflowExecution.Phase.SUCCEEDED).build()).build(), RUN_STATE));
  }

  @Test
  public void testUndefinedShouldNotInteractWithStateManager() throws Exception {
    when(flyteAdminClient.getExecution("flyte-test", "testing", "execution-name")).thenReturn(
        ExecutionOuterClass.Execution
            .newBuilder()
            .setClosure(ExecutionOuterClass.ExecutionClosure.newBuilder()
                .setPhase(Execution.WorkflowExecution.Phase.UNDEFINED).build())
            .build());

    flyteRunner.poll(FLYTE_EXECUTION_ID, RUN_STATE);

    verifyNoInteractions(stateManager);
  }

  private static RunState runState() {
    return RunState.create(
        WORKFLOW_INSTANCE,
        RunState.State.SUBMITTING,
        StateData.newBuilder()
            .trigger(Trigger.natural())
            .build()
    );
  }

  private static WorkflowInstance createWorkflowInstance() {
    Workflow workflow = Workflow.create("id", configuration());
    return WorkflowInstance.create(workflow.id(), "2016-03-14");
  }

  private static WorkflowConfiguration configuration(String... args) {
    return WorkflowConfiguration.builder()
        .id("styx.TestEndpoint")
        .schedule(HOURS)
        .flyteExecConf(FlyteExecConf
            .builder()
            .referenceId(FlyteIdentifier
                .builder()
                .resourceType("lp")
                .project("flyte-test")
                .domain("testing")
                .name("test-lp")
                .version("1")
                .build())
            .build()
        )
        .build();
  }
}
