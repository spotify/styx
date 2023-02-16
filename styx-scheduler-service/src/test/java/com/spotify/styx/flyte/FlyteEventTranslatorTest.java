/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2020 Spotify AB
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

import static com.spotify.styx.flyte.FlyteEventTranslator.translate;
import static com.spotify.styx.state.RunState.MISSING_DEPS_EXIT_CODE;
import static com.spotify.styx.state.RunState.SUCCESS_EXIT_CODE;
import static com.spotify.styx.state.RunState.UNKNOWN_ERROR_EXIT_CODE;
import static com.spotify.styx.state.RunState.UNRECOVERABLE_FAILURE_EXIT_CODE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.testdata.TestData;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.core.Execution;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class FlyteEventTranslatorTest {
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  private static final WorkflowInstance WFI =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "foo");

  @Mock private RunState runState;

  private ExecutionOuterClass.Execution createExecution(Execution.WorkflowExecution.Phase phase) {
    return ExecutionOuterClass.Execution.newBuilder().setClosure(
        ExecutionOuterClass.ExecutionClosure.newBuilder()
            .setPhase(phase)
            .build())
        .build();
  }

  private ExecutionOuterClass.Execution createExecutionExitCodes(Execution.WorkflowExecution.Phase phase,
                                                                 String flyteExitCode) {
    return ExecutionOuterClass.Execution
        .newBuilder()
        .setClosure(ExecutionOuterClass.ExecutionClosure.newBuilder().setError(
            Execution.ExecutionError
                .newBuilder()
                .setCode(flyteExitCode)
                .build())
            .setPhase(phase).build())
        .build();
  }

  @Test
  public void submittedToTerminate(){
    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED,
        createExecution(Execution.WorkflowExecution.Phase.SUCCEEDED),
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(SUCCESS_EXIT_CODE))
    );
  }

  @Test
  public void submittedToSubmitted(){
    assertNoEvent(
        RunState.State.SUBMITTED,
        createExecution(Execution.WorkflowExecution.Phase.QUEUED)
    );
  }

  @Test
  public void runningToRunning(){
    assertNoEvent(
        RunState.State.RUNNING,
        createExecution(Execution.WorkflowExecution.Phase.RUNNING)
    );
  }

  @Test
  public void submittedToSubmittedWithUndefinedPhase(){
    assertNoEvent(
        RunState.State.SUBMITTED,
        createExecution(Execution.WorkflowExecution.Phase.UNDEFINED)
    );
  }

  @Test
  public void submittedToRunning(){
    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED,
        createExecution(Execution.WorkflowExecution.Phase.RUNNING),
        Event.started(WFI)
        );
  }

  private Object[] parametersForTestTransitionPollingToTerminateExitCodes() {
    return new Object[] {
        new Object[] { Execution.WorkflowExecution.Phase.FAILED, "USER:NotReady", MISSING_DEPS_EXIT_CODE },
        new Object[] { Execution.WorkflowExecution.Phase.ABORTED, "USER:NotReady", MISSING_DEPS_EXIT_CODE },
        new Object[] { Execution.WorkflowExecution.Phase.TIMED_OUT, "USER:NotReady", MISSING_DEPS_EXIT_CODE },
        new Object[] { Execution.WorkflowExecution.Phase.FAILED, "USER:NotRetryable", UNRECOVERABLE_FAILURE_EXIT_CODE },
        new Object[] { Execution.WorkflowExecution.Phase.ABORTED, "USER:NotRetryable", UNRECOVERABLE_FAILURE_EXIT_CODE},
        new Object[] { Execution.WorkflowExecution.Phase.TIMED_OUT, "USER:NotRetryable", UNRECOVERABLE_FAILURE_EXIT_CODE},
        new Object[] { Execution.WorkflowExecution.Phase.FAILED, "USER:Persisted", SUCCESS_EXIT_CODE },
        new Object[] { Execution.WorkflowExecution.Phase.ABORTED, "USER:Persisted", SUCCESS_EXIT_CODE},
        new Object[] { Execution.WorkflowExecution.Phase.TIMED_OUT, "USER:Persisted", SUCCESS_EXIT_CODE},
        new Object[] { Execution.WorkflowExecution.Phase.FAILED, "USER:AnythingElse", UNKNOWN_ERROR_EXIT_CODE },
        new Object[] { Execution.WorkflowExecution.Phase.ABORTED, "USER:AnythingElse", UNKNOWN_ERROR_EXIT_CODE},
        new Object[] { Execution.WorkflowExecution.Phase.TIMED_OUT, "USER:AnythingElse", UNKNOWN_ERROR_EXIT_CODE},
        };
  }

  @Test
  @Parameters(method = "parametersForTestTransitionPollingToTerminateExitCodes")
  public void submittedToTerminateNoSuccess(Execution.WorkflowExecution.Phase phase, String flyteExitCode, int exitCode){
    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED,
        createExecutionExitCodes(phase, flyteExitCode),
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(exitCode))
    );
  }

  @Test
  @Parameters(method = "parametersForTestTransitionPollingToTerminateExitCodes")
  public void runningToTerminateExitCodes(Execution.WorkflowExecution.Phase phase, String flyteExitCode, int exitCode){

    var execution = createExecutionExitCodes(phase, flyteExitCode);

    when(runState.state()).thenReturn(RunState.State.RUNNING);
    when(runState.workflowInstance()).thenReturn(WFI);

    var events = translate(execution, runState);

    var expectedEvents = List.of(Event.terminate(WFI, Optional.of(exitCode)));

    assertEquals(events, expectedEvents);
  }

  private void assertGeneratesEventsAndTransitions(
      RunState.State initialState,
      ExecutionOuterClass.Execution execution,
      Event... expectedEvents) {

    RunState state = RunState.create(WFI, initialState);
    List<Event> events = translate(execution, state);
    assertThat(events, contains(expectedEvents));

    // ensure no exceptions are thrown when transitioning
    for (Event event : events) {
      state = state.transition(event, Instant::now);
    }
  }

  private void assertNoEvent(
      RunState.State initialState,
      ExecutionOuterClass.Execution execution) {

    RunState state = RunState.create(WFI, initialState);
    List<Event> events = translate(execution, state);

    assertThat(events, empty());
  }
}
