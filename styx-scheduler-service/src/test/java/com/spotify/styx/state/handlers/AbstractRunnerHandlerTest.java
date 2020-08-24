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
import com.spotify.styx.state.StateDataBuilder;
import com.spotify.styx.testdata.TestData;
import java.util.function.Consumer;
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
  private static final RunState VALID_RUNSTATE = runState(RunState.State.SUBMITTING, VALID_STATE_DATA);

  private final EventRouter router = mock(EventRouter.class);
  private final TestRunnerHandler handler = new TestRunnerHandler(true);

  @Test
  public void shouldRejectStateWithNoExecutionId() {
    var state = runState(builder -> builder.executionDescription(ExecutionDescription.forImage("busybox")));

    handler.transitionInto(state, router);

    verify(router).receiveIgnoreClosed(Event.halt(WORKFLOW_INSTANCE), state.counter());
    assertFalse(handler.reached);
  }

  @Test
  public void shouldRejectStateWithNoExecutionDescription() {
    var state = runState(builder -> builder.executionId("wf1"));

    handler.transitionInto(state, router);

    verify(router).receiveIgnoreClosed(Event.halt(WORKFLOW_INSTANCE), state.counter());
    assertFalse(handler.reached);
  }

  @Test
  public void shouldSkipNonApplicableStateWithExecutionIdAndExecutionDescription() {
    var nonApplicableHandler = new TestRunnerHandler(false);

    nonApplicableHandler.transitionInto(VALID_RUNSTATE, router);

    verifyNoInteractions(router);
    assertFalse(nonApplicableHandler.reached);
  }

  @Test
  @Parameters(method = "relevantRunnerHandlerStates")
  public void shouldCallSafeTransitionForRelevantState(RunState runState) {
    handler.transitionInto(runState, router);

    verifyNoInteractions(router);
    assertTrue(handler.reached);
  }

  @SuppressWarnings("unused")
  private static RunState[] relevantRunnerHandlerStates() {
    return new RunState[] {
        runState(RunState.State.SUBMITTING, VALID_STATE_DATA),
        runState(RunState.State.SUBMITTED, VALID_STATE_DATA),
        runState(RunState.State.RUNNING, VALID_STATE_DATA)
    };
  }

  @Test
  @Parameters(method = "irrelevantRunnerHandlerStates")
  public void shouldCallSafeTransitionForIrrelevantState(RunState runState) {
    handler.transitionInto(runState, router);

    verifyNoInteractions(router);
    assertFalse(handler.reached);
  }

  @SuppressWarnings("unused")
  private static RunState[] irrelevantRunnerHandlerStates() {
    return new RunState[] {
        runState(RunState.State.NEW, VALID_STATE_DATA),
        runState(RunState.State.QUEUED, VALID_STATE_DATA),
        runState(RunState.State.PREPARE, VALID_STATE_DATA),
        runState(RunState.State.TERMINATED, VALID_STATE_DATA),
        runState(RunState.State.FAILED, VALID_STATE_DATA),
        runState(RunState.State.ERROR, VALID_STATE_DATA),
        runState(RunState.State.DONE, VALID_STATE_DATA)
    };
  }

  private static RunState runState(Consumer<StateDataBuilder> dataBuilder) {
    var stateDataBuilder = new StateDataBuilder();
    dataBuilder.accept(stateDataBuilder);
    return runState(RunState.State.SUBMITTING, stateDataBuilder.build());
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