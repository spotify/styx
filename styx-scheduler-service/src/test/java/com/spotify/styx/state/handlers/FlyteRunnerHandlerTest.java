/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 Spotify AB
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

import static com.spotify.styx.state.handlers.FlyteRunnerHandler.STATIC_EXIT_CODE;
import static com.spotify.styx.state.handlers.FlyteRunnerHandler.STATIC_RUNNER_ID;
import static com.spotify.styx.testdata.TestData.EXECUTION_ID;
import static com.spotify.styx.testdata.TestData.FLYTE_EXECUTION_DESCRIPTION;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static org.mockito.Mockito.verify;

import com.spotify.styx.model.Event;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import java.io.IOException;
import java.util.Optional;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class FlyteRunnerHandlerTest {

  private FlyteRunnerHandler flyteRunnerHandler;

  @Mock EventRouter eventRouter;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    flyteRunnerHandler = new FlyteRunnerHandler();
  }

  @Test
  public void shouldTransitionIntoSubmitted() throws Exception {
    RunnerHandlerTestUtil
        .shouldTransitionIntoSubmitted(FLYTE_EXECUTION_DESCRIPTION, eventRouter, flyteRunnerHandler,
        STATIC_RUNNER_ID);
  }

  @Test
  public void shouldTransitionIntoRunning() throws Exception {
    RunState runState = RunState.create(WORKFLOW_INSTANCE, State.SUBMITTED, StateData.newBuilder()
        .executionId(EXECUTION_ID)
        .executionDescription(FLYTE_EXECUTION_DESCRIPTION)
        .build());

    flyteRunnerHandler.transitionInto(runState, eventRouter);

    verify(eventRouter).receive(Event.started(WORKFLOW_INSTANCE),
        runState.counter());
  }

  @Test
  public void shouldTransitionIntoTerminated() throws Exception {
    RunState runState = RunState.create(WORKFLOW_INSTANCE, State.RUNNING, StateData.newBuilder()
        .executionId(EXECUTION_ID)
        .executionDescription(FLYTE_EXECUTION_DESCRIPTION)
        .build());

    flyteRunnerHandler.transitionInto(runState, eventRouter);

    verify(eventRouter).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(STATIC_EXIT_CODE)),
        runState.counter());
  }

  @Parameters({"SUBMITTING", "SUBMITTED", "RUNNING"})
  @Test
  public void shouldHaltIfMissingExecutionDescription(State state) {
    RunnerHandlerTestUtil.shouldHaltIfMissingExecutionDescription(state, eventRouter,
        flyteRunnerHandler);
  }

  @Parameters({"SUBMITTING", "SUBMITTED", "RUNNING"})
  @Test
  public void shouldHaltIfMissingExecutionId(State state) {
    RunnerHandlerTestUtil
        .shouldHaltIfMissingExecutionId(state, FLYTE_EXECUTION_DESCRIPTION, eventRouter,
        flyteRunnerHandler);
  }
}
