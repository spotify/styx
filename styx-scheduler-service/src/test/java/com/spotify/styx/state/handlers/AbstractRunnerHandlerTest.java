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

package com.spotify.styx.state.handlers;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.testdata.TestData;
import java.util.Arrays;
import java.util.stream.Stream;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class AbstractRunnerHandlerTest {
  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-04-04T08");

  private static final StateData VALID_STATE_DATA = StateData.newBuilder()
      .executionId("wf1")
      .executionDescription(ExecutionDescription.forImage("busybox"))
      .build();
  private static final StateData NO_EXEC_ID_STATE_DATA = StateData.newBuilder()
      .executionDescription(ExecutionDescription.forImage("busybox"))
      .build();
  private static final StateData NO_EXEC_DESC_STATE_DATA = StateData.newBuilder()
      .executionId("wf1")
      .build();

  private final EventRouter router = mock(EventRouter.class);
  private final TestRunnerHandler handler = new TestRunnerHandler(true);

  @Test
  @Parameters(method = "relevantRunnerHandlerStates")
  public void shouldCallSafeTransitionForValidState(RunState.State state) {
    var runState = runState(state, VALID_STATE_DATA);

    handler.transitionInto(runState, router);

    verifyNoInteractions(router);
    assertTrue(handler.reached);
  }

  @Test
  @Parameters(method = "execRunnerHandlerStates")
  public void shouldRejectStateWithNoExecutionId(RunState.State state) {
    var runState = runState(state, NO_EXEC_ID_STATE_DATA);

    handler.transitionInto(runState, router);

    verify(router).receiveIgnoreClosed(Event.halt(WORKFLOW_INSTANCE), runState.counter());
    assertFalse(handler.reached);
  }

  @Test
  @Parameters(method = "execRunnerHandlerStates")
  public void shouldRejectStateWithNoExecutionDescription(RunState.State state) {
    var runState = runState(state, NO_EXEC_DESC_STATE_DATA);

    handler.transitionInto(runState, router);

    verify(router).receiveIgnoreClosed(Event.halt(WORKFLOW_INSTANCE), runState.counter());
    assertFalse(handler.reached);
  }

  @Test
  @Parameters(method = "relevantRunnerHandlerStates")
  public void shouldSkipNonApplicableStateWithExecutionIdAndExecutionDescription(RunState.State state) {
    var nonApplicableHandler = new TestRunnerHandler(false);
    var runState = runState(state, VALID_STATE_DATA);

    nonApplicableHandler.transitionInto(runState, router);

    verifyNoInteractions(router);
    assertFalse(nonApplicableHandler.reached);
  }

  @Test
  @Parameters(method = "irrelevantRunnerHandlerStates")
  public void shouldCallSafeTransitionForIrrelevantState(RunState.State state) {
    var runState = runState(state, VALID_STATE_DATA);

    handler.transitionInto(runState, router);

    verifyNoInteractions(router);
    assertFalse(handler.reached);
  }

  @SuppressWarnings("unused")
  private static RunState.State[] execRunnerHandlerStates() {
    return new RunState.State[] {
        RunState.State.SUBMITTING,
        RunState.State.SUBMITTED,
        RunState.State.RUNNING,
    };
  }

  @SuppressWarnings("unused")
  private static RunState.State[] relevantRunnerHandlerStates() {
    return Stream.concat(
        Arrays.stream(execRunnerHandlerStates()),
        Stream.of(RunState.State.FAILED, RunState.State.ERROR)
    ).toArray(RunState.State[]::new);
  }

  @SuppressWarnings("unused")
  private static RunState.State[] irrelevantRunnerHandlerStates() {
    return new RunState.State[] {
        RunState.State.NEW,
        RunState.State.QUEUED,
        RunState.State.PREPARE,
        RunState.State.TERMINATED,
        RunState.State.DONE
    };
  }

  private static RunState runState(RunState.State state, StateData stateData) {
    return RunState.create(WORKFLOW_INSTANCE, state, stateData);
  }

  private static class TestRunnerHandler extends AbstractRunnerHandler {
    private boolean reached;

    protected TestRunnerHandler(boolean applicable) {
      super(__ -> applicable);
    }

    @Override
    protected void safeTransitionInto(RunState state, EventRouter eventRouter) {
      this.reached = true;
    }
  }
}
