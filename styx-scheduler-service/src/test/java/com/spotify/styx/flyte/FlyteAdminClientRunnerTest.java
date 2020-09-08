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
import static com.spotify.styx.state.RunState.UNKNOWN_ERROR_EXIT_CODE;
import static com.spotify.styx.state.RunState.UNRECOVERABLE_FAILURE_EXIT_CODE;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.styx.flyte.FlyteAdminClientRunner;
import com.spotify.styx.flyte.FlyteExecutionId;
import com.spotify.styx.flyte.FlyteRunner;
import com.spotify.styx.flyte.client.FlyteAdminClient;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.model.FlyteIdentifier;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
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

  private static final FlyteExecConf FLYTE_EXEC_CONF = FlyteExecConf.builder()
      .referenceId(FlyteIdentifier.builder()
          .project("flyte-test")
          .domain("testing")
          .name("wf-name")
          .version("1234")
          .resourceType("lp")
          .build())
      .build();
  @Mock private FlyteAdminClient flyteAdminClient;
  @Mock private StateManager stateManager;
  @Mock private RunState runState;

  private FlyteAdminClientRunner flyteRunner;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    flyteRunner = new FlyteAdminClientRunner(flyteAdminClient, stateManager);
  }

  @Test
  public void testIsEnabled() {
    assertTrue(flyteRunner.isEnabled());
  }

  @Test
  public void testCreateExecution() throws FlyteRunner.CreateExecutionException {
    when(flyteAdminClient.createExecution(any(), any(), any(), any(),any())).thenReturn(
        ExecutionOuterClass.ExecutionCreateResponse
            .newBuilder()
            .setId(IdentifierOuterClass.WorkflowExecutionIdentifier
                .newBuilder()
                .setProject("flyte-test")
                .setDomain("testing")
                .setName("test-create-execution")
                .build())
            .build());
    var flyteExecution = flyteRunner.createExecution("test-create-execution", FLYTE_EXEC_CONF);

    assertThat(flyteExecution.getProject(), is("flyte-test"));
    assertThat(flyteExecution.getDomain(), is("testing"));
    assertThat(flyteExecution.getName(), is("test-create-execution"));
    assertThat(flyteExecution.toUrn(), is("ex:flyte-test:testing:test-create-execution"));
  }

  @Test
  public void testThrowsFlyteLaunchPlanNotFound() {
    doThrow(new StatusRuntimeException(Status.NOT_FOUND))
        .when(flyteAdminClient).createExecution(any(), any(), any(), any(),any());

    assertThrows(
        FlyteRunner.LaunchPlanNotFound.class,
        () -> flyteRunner.createExecution("exec", FLYTE_EXEC_CONF));
  }

  @Test
  public void testThrowsCreateExecutionExceptionForOtherCode() {
    doThrow(new StatusRuntimeException(Status.INTERNAL))
        .when(flyteAdminClient).createExecution(any(), any(), any(), any(),any());

    assertThrows(
        FlyteRunner.CreateExecutionException.class,
        () -> flyteRunner.createExecution("exec", FLYTE_EXEC_CONF));
  }

  @Test
  public void testThrowsCreateExecutionExceptionForUnknownException() {
    doThrow(new IllegalStateException("test"))
        .when(flyteAdminClient).createExecution(any(), any(), any(), any(),any());

    assertThrows(
        FlyteRunner.CreateExecutionException.class,
        () -> flyteRunner.createExecution("exec", FLYTE_EXEC_CONF));
  }

  @Test
  public void testPollFlyteExecutionIdCannotBeNull() {
    assertThrows(
        NullPointerException.class,
        () ->    flyteRunner.poll(null, RunState.create(null, null))
    );
  }

  @Test
  public void testPollRunStateCannotBeNull() {
    assertThrows(
        NullPointerException.class,
        () ->    flyteRunner.poll(FlyteExecutionId.create("flyte-test", "testing", "test-run-state-cannot-be-null"), null)
    );
  }

  @Test
  public void testTransititionRunningToTerminateSuccessfulRun() throws Exception {
    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");

    when(flyteAdminClient.getExecution("flyte-test", "testing", "execution-name")).thenReturn(
        ExecutionOuterClass.Execution
            .newBuilder()
            .setClosure(ExecutionOuterClass.ExecutionClosure.newBuilder()
                    .setPhase(Execution.WorkflowExecution.Phase.SUCCEEDED).build())
            .build());
    when(runState.workflowInstance()).thenReturn(workflowInstance);

    final FlyteExecutionId flyteExecutionId =
        FlyteExecutionId.create("flyte-test", "testing", "execution-name");
    flyteRunner.poll(flyteExecutionId, runState);
    verify(stateManager,  timeout(60_000)).receive(Event.terminate(workflowInstance, Optional.of(0)));
  }

  @Test
  @Parameters(method = "parametersForTestTransititionRunningToTerminateExitCodes")
  public void testTransititionRunningToTerminateExitCodes(Execution.WorkflowExecution.Phase phase, String flyteExitCode, int exitCode) throws Exception {
    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");

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
    when(runState.workflowInstance()).thenReturn(workflowInstance);

    final FlyteExecutionId flyteExecutionId =
        FlyteExecutionId.create("flyte-test", "testing", "execution-name");
    flyteRunner.poll(flyteExecutionId, runState);
    verify(stateManager,  timeout(60_000)).receive(Event.terminate(workflowInstance, Optional.of(exitCode)));
  }

  private Object[] parametersForTestTransititionRunningToTerminateExitCodes() {
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

    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    when(runState.workflowInstance()).thenReturn(workflowInstance);

    assertThrows(
        FlyteRunner.ExecutionNotFoundException.class,
        () -> flyteRunner.poll(
            FlyteExecutionId.create("flyte-test", "testing", "test-flyte-admin-client-exception-during-poll"), runState));
  }

  @Test
  public void testPollingExceptionFlyteAdminClientExecution() {
    doThrow(new StatusRuntimeException(Status.INTERNAL))
        .when(flyteAdminClient).getExecution(anyString(), anyString(), anyString());

    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    when(runState.workflowInstance()).thenReturn(workflowInstance);

    assertThrows(
        FlyteRunner.PollingException.class,
        () -> flyteRunner.poll(
            FlyteExecutionId.create("flyte-test", "testing", "test-flyte-admin-client-exception-during-poll"), runState));
  }

  @Test
  public void testPollingException() {
    doThrow(new Exception())
        .when(flyteAdminClient).getExecution(anyString(), anyString(), anyString());

    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    when(runState.workflowInstance()).thenReturn(workflowInstance);

    assertThrows(
        FlyteRunner.PollingException.class,
        () -> flyteRunner.poll(
            FlyteExecutionId.create("flyte-test", "testing", "test-flyte-admin-client-exception-during-poll"), runState));
  }

  @Test
  public void testEmitFlyteEventsExceptionHandling() throws IsClosedException {
    doThrow(new IsClosedException())
        .when(stateManager).receive(any(), any());

    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    when(runState.workflowInstance()).thenReturn(workflowInstance);

    assertThrows(
        IsClosedException.class,
        () -> flyteRunner.emitFlyteEvents(ExecutionOuterClass.Execution.newBuilder().setClosure(
            ExecutionOuterClass.ExecutionClosure.newBuilder().setPhase(
                Execution.WorkflowExecution.Phase.SUCCEEDED).build()).build(), runState));
  }

  private WorkflowConfiguration configuration(String... args) {
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
