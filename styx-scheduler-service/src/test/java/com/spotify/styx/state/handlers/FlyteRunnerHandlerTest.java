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

import static com.spotify.styx.state.handlers.FlyteRunnerHandler.STYX_EXECUTION_ID_ANNOTATION;
import static com.spotify.styx.state.handlers.FlyteRunnerHandler.STYX_WORKFLOW_INSTANCE_ANNOTATION;
import static com.spotify.styx.testdata.TestData.EXECUTION_DESCRIPTION;
import static com.spotify.styx.testdata.TestData.EXECUTION_ID;
import static com.spotify.styx.testdata.TestData.FLYTE_EXECUTION_DESCRIPTION;
import static com.spotify.styx.testdata.TestData.FLYTE_EXEC_CONF;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.spotify.styx.flyte.FlyteExecutionId;
import com.spotify.styx.flyte.FlyteRunner;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.FlyteIdentifier;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.IsClosedException;
import java.util.List;
import java.util.Map;
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
  @Mock FlyteRunner flyteRunner;

  private static final FlyteExecutionId FLYTE_EXECUTION_ID = getFlyteExecutionId(FLYTE_EXECUTION_DESCRIPTION,
      TestData.FLYTE_EXECUTION_ID);
  private static final Map<String, String> ANNOTATIONS = Map.of(
      STYX_EXECUTION_ID_ANNOTATION, EXECUTION_ID,
      STYX_WORKFLOW_INSTANCE_ANNOTATION, WORKFLOW_INSTANCE.toKey());

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(flyteRunner.isEnabled()).thenReturn(true);
    flyteRunnerHandler = new FlyteRunnerHandler(flyteRunner);
  }

  @Test
  public void shouldTransitionIntoSubmitted() throws Exception {
    when(flyteRunner.createExecution(any(), any(), any())).thenReturn("runnerId");
    RunState runState = RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTING, StateData.newBuilder()
        .executionId(EXECUTION_ID)
        .executionDescription(FLYTE_EXECUTION_DESCRIPTION)
        .build());

    flyteRunnerHandler.transitionInto(runState, eventRouter);
    verify(flyteRunner).createExecution(runState, TestData.FLYTE_EXECUTION_ID, FLYTE_EXEC_CONF);
    verify(eventRouter,  timeout(60_000)).receiveIgnoreClosed(Event.submitted(WORKFLOW_INSTANCE, EXECUTION_ID, "runnerId"),
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

  @Test
  public void shouldReportRunErrorIfCatchCreateExecutionException() throws Exception {
    doThrow(new FlyteRunner.CreateExecutionException("Houston we have a problem", null))
        .when(flyteRunner).createExecution(any(), any(), any());
    RunState runState = RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTING, StateData.newBuilder()
        .executionId(EXECUTION_ID)
        .executionDescription(FLYTE_EXECUTION_DESCRIPTION)
        .build());

    flyteRunnerHandler.transitionInto(runState, eventRouter);

    verify(flyteRunner).createExecution(runState, TestData.FLYTE_EXECUTION_ID, FLYTE_EXEC_CONF);
    verify(eventRouter,  timeout(60_000)).receiveIgnoreClosed(Event.runError(WORKFLOW_INSTANCE, "Houston we have a problem"),
        runState.counter());
  }

  @Test
  @Parameters({"SUBMITTING", "SUBMITTED", "RUNNING"})
  public void shouldHaltTransitionsWhenFlyteRunnerIsNotEnabled(State state) throws Exception {
    when(flyteRunner.isEnabled()).thenReturn(false);
    RunState runState = RunState.create(WORKFLOW_INSTANCE, state, StateData.newBuilder()
        .executionId(EXECUTION_ID)
        .executionDescription(FLYTE_EXECUTION_DESCRIPTION)
        .build());

    flyteRunnerHandler.transitionInto(runState, eventRouter);

    verify(flyteRunner, never()).createExecution(any(), any(), any());
    verify(eventRouter,  timeout(60_000)).receiveIgnoreClosed(Event.halt(WORKFLOW_INSTANCE), runState.counter());
  }

  @Test
  public void shouldNotHaltTransitionsWhenFlyteRunnerIsNotEnabledAndErrorState() throws Exception {
    when(flyteRunner.isEnabled()).thenReturn(false);
    RunState runState = RunState.create(WORKFLOW_INSTANCE, State.ERROR, StateData.newBuilder()
        .executionId(EXECUTION_ID)
        .executionDescription(FLYTE_EXECUTION_DESCRIPTION)
        .build());

    flyteRunnerHandler.transitionInto(runState, eventRouter);

    verify(flyteRunner, never()).createExecution(any(), any(), any());
    verifyNoInteractions(eventRouter);
  }


  @Test()
  @Parameters({"SUBMITTING", "SUBMITTED", "RUNNING"})
  public void shouldNotThrowExceptionIfEventRouterIsClosed(State state) throws Exception {
    doThrow(IsClosedException.class).when(eventRouter).receive(any(), anyLong());
    RunState runState = RunState.create(WORKFLOW_INSTANCE, state, StateData.newBuilder()
        .executionId(EXECUTION_ID)
        .executionDescription(FLYTE_EXECUTION_DESCRIPTION)
        .build());

    flyteRunnerHandler.transitionInto(runState, eventRouter);
  }

  @Test
  public void shouldPollInRunning() {
    RunState runState = RunState.create(WORKFLOW_INSTANCE, State.RUNNING, StateData.newBuilder()
        .executionId(EXECUTION_ID)
        .executionDescription(FLYTE_EXECUTION_DESCRIPTION)
        .build());

    flyteRunnerHandler.transitionInto(runState, eventRouter);

    verify(flyteRunner).poll(FLYTE_EXECUTION_ID, runState);
  }

  @Test
  public void shouldPollInSubmitted() {
    RunState runState = RunState.create(WORKFLOW_INSTANCE, State.SUBMITTED, StateData.newBuilder()
        .executionId(EXECUTION_ID)
        .executionDescription(FLYTE_EXECUTION_DESCRIPTION)
        .build());

    flyteRunnerHandler.transitionInto(runState, eventRouter);

    verify(flyteRunner).poll(getFlyteExecutionId(FLYTE_EXECUTION_DESCRIPTION,
        TestData.FLYTE_EXECUTION_ID), runState);
  }

  @Test
  @Parameters({"FAILED", "ERROR"})
  public void shouldTerminateExecutionsOnFailedandErrorState(State state) {
    RunState runState = RunState.create(WORKFLOW_INSTANCE, state, StateData.newBuilder()
        .executionId(EXECUTION_ID)
        .executionDescription(FLYTE_EXECUTION_DESCRIPTION)
        .build());

    flyteRunnerHandler.transitionInto(runState, eventRouter);

    verify(flyteRunner).terminateExecution(runState, FLYTE_EXECUTION_ID);
    verifyNoInteractions(eventRouter);
  }

  @Test
  @Parameters(method = "stateDataWithNoFlyteConfiguration")
  public void shouldDoNothingOnFailedAndErrorStateWithNoFlyteRuntimeConfiguration(StateData stateData, State state) {
    RunState runState = RunState.create(WORKFLOW_INSTANCE, state, stateData);

    flyteRunnerHandler.transitionInto(runState, eventRouter);

    verify(flyteRunner, never()).terminateExecution(any(), any());
    verifyNoInteractions(eventRouter);
  }

  private static Object[] stateDataWithNoFlyteConfiguration() {
    var stateData = List.of(
        StateData.newBuilder().build(),
        StateData.newBuilder()
            .executionId(EXECUTION_ID)
            .build(),
        StateData.newBuilder()
            .executionId(EXECUTION_ID)
            .executionDescription(EXECUTION_DESCRIPTION) //docker
            .build()
    );
    var states = List.of(State.FAILED, State.ERROR);
    //cartesian product
    return stateData.stream()
        .flatMap(data -> states.stream().map(state -> new Object[]{ data, state }))
        .toArray();
  }

  private static FlyteExecutionId getFlyteExecutionId(ExecutionDescription executionDescription, String executionId) {
    final FlyteIdentifier flyteIdentifier =
        executionDescription.flyteExecConf().orElseThrow().referenceId();
    return FlyteExecutionId.create(flyteIdentifier.project(), flyteIdentifier.domain(), executionId);
  }
}
