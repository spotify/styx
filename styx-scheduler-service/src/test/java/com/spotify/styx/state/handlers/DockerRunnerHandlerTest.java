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

import static com.spotify.styx.model.Schedule.HOURS;
import static com.spotify.styx.testdata.TestData.EXECUTION_ID;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.docker.InvalidExecutionException;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.util.IsClosedException;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class DockerRunnerHandlerTest {

  private static final Instant NOW = Instant.now();
  private static final long COUNTER = 17;

  private DockerRunnerHandler dockerRunnerHandler;

  private static final String TEST_EXECUTION_ID = "execution_1";
  private static final String TEST_RUNNER_ID = "test";
  private static final String TEST_DOCKER_IMAGE = "busybox:1.1";
  private static final ExecutionDescription EXECUTION_DESCRIPTION = ExecutionDescription.builder()
      .dockerImage(TEST_DOCKER_IMAGE)
      .dockerArgs(List.of("--date", "{}", "--bar"))
      .build();

  @Mock DockerRunner dockerRunner;
  @Mock EventRouter eventRouter;

  @Captor ArgumentCaptor<RunState> runStateCaptor;
  @Captor ArgumentCaptor<RunSpec> runSpecCaptor;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    when(dockerRunner.start(any(RunState.class), any(RunSpec.class))).thenReturn(TEST_RUNNER_ID);
    dockerRunnerHandler = new DockerRunnerHandler(dockerRunner);
  }

  @Test
  public void shouldPassInArguments() throws Exception {
    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    ExecutionDescription desc = ExecutionDescription.forImage(TEST_DOCKER_IMAGE);
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING,
        StateData.newBuilder()
            .executionDescription(desc)
            .executionId(TEST_EXECUTION_ID)
            .build());

    dockerRunnerHandler.transitionInto(runState, eventRouter);

    verify(dockerRunner, timeout(60_000)).start(runStateCaptor.capture(), runSpecCaptor.capture());

    assertThat(runStateCaptor.getValue(), is(runState));
    assertThat(runSpecCaptor.getValue().imageName(), is(TEST_DOCKER_IMAGE));
    assertThat(runSpecCaptor.getValue().executionId(), is(TEST_EXECUTION_ID));
  }

  @Test
  public void shouldInterpolateScheduleArgument() throws Exception {
    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING,
        StateData.newBuilder()
            .executionId(TEST_EXECUTION_ID)
            .executionDescription(EXECUTION_DESCRIPTION)
            .build());

    dockerRunnerHandler.transitionInto(runState, eventRouter);

    verify(dockerRunner, timeout(60_000)).start(runStateCaptor.capture(), runSpecCaptor.capture());

    assertThat(runSpecCaptor.getValue().args(), contains("--date", "2016-03-14T15" , "--bar"));
  }

  @Test
  public void shouldTransitionIntoSubmitted() throws Exception {
    RunState runState = RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTING, StateData.newBuilder()
        .executionId(EXECUTION_ID)
        .executionDescription(EXECUTION_DESCRIPTION)
        .build());

    dockerRunnerHandler.transitionInto(runState, eventRouter);

    verify(eventRouter,  timeout(60_000)).receive(Event.submitted(WORKFLOW_INSTANCE, EXECUTION_ID, TEST_RUNNER_ID),
        runState.counter());
  }

  @Test
  public void shouldFailIfDockerRunnerRaisesException() throws Exception {
    shouldFailIfDockerRunnerRaisesException0(new IOException("Testing exception."));
  }

  @Test
  public void shouldFailIfDockerRunnerRaisesUserErrorInvalidExecutionException() throws Exception {
    shouldFailIfDockerRunnerRaisesException0(new InvalidExecutionException("PEBKAC"));
  }

  private void shouldFailIfDockerRunnerRaisesException0(Throwable throwable)
      throws IOException, IsClosedException {
    doThrow(throwable).when(dockerRunner)
        .start(any(RunState.class), any(RunSpec.class));

    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING,
        StateData.newBuilder()
            .executionId(TEST_EXECUTION_ID)
            .executionDescription(EXECUTION_DESCRIPTION)
            .build(), NOW, COUNTER);

    dockerRunnerHandler.transitionInto(runState, eventRouter);

    verify(eventRouter).receive(Event.runError(workflowInstance, throwable.getMessage()), COUNTER);
    verifyNoMoreInteractions(eventRouter);
  }

  @Parameters({"SUBMITTING", "SUBMITTED", "RUNNING"})
  @Test
  public void shouldHaltIfMissingExecutionDescription(State state) {
    RunnerHandlerTestUtil
        .shouldHaltIfMissingExecutionDescription(state, eventRouter, dockerRunnerHandler);
  }

  @Parameters({"SUBMITTING", "SUBMITTED", "RUNNING"})
  @Test
  public void shouldHaltIfMissingExecutionId(State state) {
    RunnerHandlerTestUtil.shouldHaltIfMissingExecutionId(state, EXECUTION_DESCRIPTION, eventRouter, dockerRunnerHandler);
  }

  @Parameters({"SUBMITTED", "RUNNING"})
  @Test
  public void shouldPollWhenStarted(State state) {
    WorkflowConfiguration workflowConfiguration = configuration("--date", "{}", "--bar");
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, state,
        StateData.newBuilder().executionDescription(ExecutionDescription.forImage("image")).executionId(TEST_EXECUTION_ID).build());

    dockerRunnerHandler.transitionInto(runState, eventRouter);

    verify(dockerRunner).poll(runState);
  }

  private WorkflowConfiguration configuration(String... args) {
    return WorkflowConfiguration.builder()
        .id("styx.TestEndpoint")
        .schedule(HOURS)
        .dockerImage(TEST_DOCKER_IMAGE)
        .dockerArgs(ImmutableList.copyOf(args))
        .build();
  }
}
