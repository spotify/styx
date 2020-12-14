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

import static com.spotify.styx.flyte.FlyteAdminClientRunner.TERMINATE_CAUSE_PREFIX;
import static com.spotify.styx.model.Schedule.HOURS;
import static com.spotify.styx.state.RunState.MISSING_DEPS_EXIT_CODE;
import static com.spotify.styx.state.RunState.SUCCESS_EXIT_CODE;
import static com.spotify.styx.state.RunState.UNKNOWN_ERROR_EXIT_CODE;
import static com.spotify.styx.state.RunState.UNRECOVERABLE_FAILURE_EXIT_CODE;
import static flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode.SCHEDULED;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
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
import com.spotify.styx.state.StateTransitionConflictException;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.util.CounterCapacityException;
import com.spotify.styx.util.IsClosedException;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode;
import flyteidl.core.Execution;
import flyteidl.core.IdentifierOuterClass;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Map;
import java.util.Optional;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
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
  private static final RunState RUN_STATE_SUBMITTED = runState(RunState.State.SUBMITTED);
  private static final Map<String, String> ANNOTATIONS = Map.of(
      "hero", "Spiderman",
      "villain", "Green Goblin"
  );

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
    when(flyteAdminClient.createExecution(any(), any(), any(), any(), any(), any(), any())).thenReturn(
        ExecutionOuterClass.ExecutionCreateResponse
            .newBuilder()
            .setId(IdentifierOuterClass.WorkflowExecutionIdentifier
                .newBuilder()
                .setProject(LAUNCH_PLAN_IDENTIFIER.project())
                .setDomain(LAUNCH_PLAN_IDENTIFIER.domain())
                .setName(execName)
                .build())
            .build());

    var runnerId = flyteRunner.createExecution(RUN_STATE, execName, FLYTE_EXEC_CONF, ANNOTATIONS);

    assertThat(runnerId, is(RUNNER_ID));
    verify(flyteAdminClient).createExecution(
        LAUNCH_PLAN_IDENTIFIER.project(), LAUNCH_PLAN_IDENTIFIER.domain(), execName,
        toProto(LAUNCH_PLAN_IDENTIFIER), SCHEDULED, ANNOTATIONS, RUN_STATE.workflowInstance().parameter());
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
  @Parameters(method = "styxTriggerToFlyteExecMode")
  public void testStyxTriggerTranslateToFlyteExecutionMode(Trigger styxTrigger, ExecutionMode flyteExecMode)
      throws FlyteRunner.CreateExecutionException {
    final RunState runState = runState(styxTrigger);
    flyteRunner.createExecution(runState, "test-create-execution", FLYTE_EXEC_CONF, ANNOTATIONS);
    verify(flyteAdminClient).createExecution(any(), any(), any(), any(), Mockito.eq(flyteExecMode),
        any(), any());
  }

  private static Object[] styxTriggerToFlyteExecMode() {
    return new Object[] {
        new Object[] { Trigger.adhoc("id"), ExecutionMode.MANUAL },
        new Object[] { Trigger.backfill("id"), ExecutionMode.MANUAL },
        new Object[] { Trigger.natural(), SCHEDULED },
    };
  }

  @Test
  @Parameters(method = "invalidsStyxTriggerToFlyteExecMode")
  public void testCreateExecutionThrowsExceptionForInvalidExecutionMode(Trigger styxTrigger) {
    var exception = assertThrows(
        FlyteRunner.CreateExecutionException.class,
        () -> flyteRunner.createExecution(runState(styxTrigger), "test-create-execution", FLYTE_EXEC_CONF, ANNOTATIONS)
    );

    assertThat(exception.getMessage(), containsString("Missing trigger or unknown in StateData"));
  }

  private static Trigger[] invalidsStyxTriggerToFlyteExecMode() {
    return new Trigger[] { /* no trigger */ null, Trigger.unknown("id") };
  }

  @Test
  public void testThrowsFlyteLaunchPlanNotFound() {
    doThrow(new StatusRuntimeException(Status.NOT_FOUND))
        .when(flyteAdminClient).createExecution(any(), any(), any(), any(), any(), any(), any());
    assertThrows(
        FlyteRunner.LaunchPlanNotFound.class,
        () -> flyteRunner.createExecution(RUN_STATE, "exec", FLYTE_EXEC_CONF, ANNOTATIONS));
  }

  @Test
  public void testCreateExecutionForAlreadyExistsException() throws FlyteRunner.CreateExecutionException {
    doThrow(new StatusRuntimeException(Status.ALREADY_EXISTS))
        .when(flyteAdminClient).createExecution(any(), any(), any(), any(), any(), any(), any());
    var runnerId = flyteRunner.createExecution(RUN_STATE, "exec", FLYTE_EXEC_CONF, ANNOTATIONS);

    assertThat(runnerId, is(RUNNER_ID));
    verify(flyteAdminClient).createExecution(
        LAUNCH_PLAN_IDENTIFIER.project(), LAUNCH_PLAN_IDENTIFIER.domain(), "exec",
        toProto(LAUNCH_PLAN_IDENTIFIER), SCHEDULED, ANNOTATIONS, RUN_STATE.workflowInstance().parameter());
  }

  @Test
  public void testThrowsCreateExecutionExceptionForOtherCode() {
    doThrow(new StatusRuntimeException(Status.INTERNAL))
        .when(flyteAdminClient).createExecution(any(), any(), any(), any(), any(), any(), any());
    assertThrows(
        FlyteRunner.CreateExecutionException.class,
        () -> flyteRunner.createExecution(RUN_STATE, "exec", FLYTE_EXEC_CONF, ANNOTATIONS));
  }

  @Test
  public void testThrowsCreateExecutionExceptionForUnknownException() {
    doThrow(new IllegalStateException("test"))
        .when(flyteAdminClient).createExecution(any(), any(), any(), any(), any(), any(), any());

    assertThrows(
        FlyteRunner.CreateExecutionException.class,
        () -> flyteRunner.createExecution(RUN_STATE, "exec", FLYTE_EXEC_CONF, ANNOTATIONS));
  }

  @Test
  public void testTerminateExecution() {
    flyteRunner.terminateExecution(RUN_STATE, FLYTE_EXECUTION_ID);

    verify(flyteAdminClient).terminateExecution(
        eq(FLYTE_EXECUTION_ID.project()),
        eq(FLYTE_EXECUTION_ID.domain()),
        eq(FLYTE_EXECUTION_ID.name()),
        contains(TERMINATE_CAUSE_PREFIX));
  }

  @Test
  @Parameters(method = "terminateExecutionExceptions")
  public void testNoExceptionThrownFromTerminateExecution(Throwable ex) {
    doThrow(ex).when(flyteAdminClient).terminateExecution(any(), any(), any(), any());

    flyteRunner.terminateExecution(RUN_STATE, FLYTE_EXECUTION_ID);
  }

  private static Throwable[] terminateExecutionExceptions() {
    return new Throwable[] {
        new StatusRuntimeException(Status.NOT_FOUND),
        new StatusRuntimeException(Status.UNKNOWN),
        new RuntimeException("test")
    };
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
    verify(stateManager,  timeout(60_000)).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(0)), -1);
  }

  @Test
  @Parameters(method = "parametersForTestTransitionPollingToTerminateExitCodes")
  public void testTransitionRunningToTerminateExitCodes(Execution.WorkflowExecution.Phase phase, String flyteExitCode, int exitCode) throws Exception {
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
    verify(stateManager,  timeout(60_000)).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(exitCode)), -1);
  }

  @Test
  @Parameters(method = "parametersForTestTransitionPollingToTerminateExitCodes")
  public void testTransitionSubmittedToTerminateExitCodes(Execution.WorkflowExecution.Phase phase, String flyteExitCode,
                                                 int exitCode) throws Exception {
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

    flyteRunner.poll(FLYTE_EXECUTION_ID, RUN_STATE_SUBMITTED);
    verify(stateManager, timeout(60_000)).receive(Event.started(WORKFLOW_INSTANCE), -1);
    verify(stateManager,  timeout(60_000)).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(exitCode)), 0);
  }

  @Test
  public void testTransitionSubmittedToRunning() throws Exception {
    when(flyteAdminClient.getExecution("flyte-test", "testing", "execution-name")).thenReturn(
        ExecutionOuterClass.Execution
            .newBuilder()
            .setClosure(ExecutionOuterClass.ExecutionClosure.newBuilder()
                .setPhase(Execution.WorkflowExecution.Phase.RUNNING).build())
            .build());

    flyteRunner.poll(FLYTE_EXECUTION_ID, RUN_STATE_SUBMITTED);
    verify(stateManager,  timeout(60_000)).receive(Event.started(WORKFLOW_INSTANCE), -1);
  }

  @Test
  public void testTransitionSubmittedToTerminate() throws Exception {
    when(flyteAdminClient.getExecution("flyte-test", "testing", "execution-name")).thenReturn(
        ExecutionOuterClass.Execution
            .newBuilder()
            .setClosure(ExecutionOuterClass.ExecutionClosure.newBuilder()
                .setPhase(Execution.WorkflowExecution.Phase.SUCCEEDED).build())
            .build());

    flyteRunner.poll(FLYTE_EXECUTION_ID, RUN_STATE_SUBMITTED);
    verify(stateManager,  timeout(60_000)).receive(Event.started(WORKFLOW_INSTANCE), -1);
    verify(stateManager, timeout(60_000))
        .receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(SUCCESS_EXIT_CODE)), 0);
  }

  @Test
  public void testTransitionRunningToRunningNoEventEmit() throws Exception {
    when(flyteAdminClient.getExecution("flyte-test", "testing", "execution-name")).thenReturn(
        ExecutionOuterClass.Execution
            .newBuilder()
            .setClosure(ExecutionOuterClass.ExecutionClosure.newBuilder()
                .setPhase(Execution.WorkflowExecution.Phase.RUNNING).build())
            .build());
    RunState state = runState(RunState.State.RUNNING);

    flyteRunner.poll(FLYTE_EXECUTION_ID, state);
    verify(stateManager,  never()).receive(Event.started(WORKFLOW_INSTANCE));
  }

  private Object[] parametersForTestTransitionPollingToTerminateExitCodes() {
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
  @Parameters(method = "parametersForTestEmitFlyteEvents")
  public void testEmitFlyteEventsIgnoreStateTransitionConflictException(Exception e) throws IsClosedException {
    doThrow(e)
        .when(stateManager).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(SUCCESS_EXIT_CODE)));

    flyteRunner.emitFlyteEvents(ExecutionOuterClass.Execution.newBuilder().setClosure(
        ExecutionOuterClass.ExecutionClosure.newBuilder().setPhase(
            Execution.WorkflowExecution.Phase.SUCCEEDED).build()).build(), RUN_STATE);

    verify(stateManager).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(SUCCESS_EXIT_CODE)), -1);
  }

  private Object[] parametersForTestEmitFlyteEvents() {
    return new Object[]{
        new Object[]{ new StateTransitionConflictException("") },
        new Object[]{ new CounterCapacityException("") },
        new Object[]{ new IsClosedException() }
    };
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
    return runState(Trigger.natural());
  }

  private static RunState runState(Trigger trigger) {
    return RunState.create(
        WORKFLOW_INSTANCE,
        RunState.State.SUBMITTING,
        StateData.newBuilder()
            .trigger(Optional.ofNullable(trigger))
            .build()
    );
  }

  private static RunState runState(RunState.State state) {
    return RunState.create(
        WORKFLOW_INSTANCE,
        state,
        StateData.newBuilder()
            .trigger(Trigger.natural())
            .build()
    );
  }

  private static WorkflowInstance createWorkflowInstance() {
    Workflow workflow = Workflow.create("id", configuration());
    return WorkflowInstance.create(workflow.id(), "2016-03-14");
  }

  private static WorkflowConfiguration configuration() {
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
