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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.docker.InvalidExecutionException;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.util.IsClosedException;
import java.io.IOException;
import java.time.Instant;
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
  private static final String TEST_DOCKER_IMAGE = "busybox:1.1";
  private static final ExecutionDescription EXECUTION_DESCRIPTION = ExecutionDescription.builder()
      .dockerImage(TEST_DOCKER_IMAGE)
      .dockerArgs("--date", "{}", "--bar")
      .build();

  @Mock DockerRunner dockerRunner;
  @Mock StateManager stateManager;

  @Captor ArgumentCaptor<WorkflowInstance> instanceCaptor;
  @Captor ArgumentCaptor<RunSpec> runSpecCaptor;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    dockerRunnerHandler = new DockerRunnerHandler(dockerRunner, stateManager);
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

    dockerRunnerHandler.transitionInto(runState);

    verify(dockerRunner, timeout(60_000)).start(instanceCaptor.capture(), runSpecCaptor.capture());

    assertThat(instanceCaptor.getValue(), is(workflowInstance));
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

    dockerRunnerHandler.transitionInto(runState);

    verify(dockerRunner, timeout(60_000)).start(instanceCaptor.capture(), runSpecCaptor.capture());

    assertThat(runSpecCaptor.getValue().args(), contains("--date", "2016-03-14T15" , "--bar"));
  }

  @Test
  public void shouldTransitionIntoSubmitted() throws Exception {
    WorkflowConfiguration workflowConfiguration = configuration("--date", "{}", "--bar");
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, State.SUBMITTING, StateData.newBuilder()
        .executionId(TEST_EXECUTION_ID)
        .executionDescription(EXECUTION_DESCRIPTION)
        .build());

    dockerRunnerHandler.transitionInto(runState);

    verify(stateManager, timeout(60_000)).receive(Event.submitted(workflowInstance, TEST_EXECUTION_ID),
        runState.counter());
  }

  @Test
  public void shouldFailIfDockerRunnerRaisesException() throws Exception {
    shouldFailIfDockerRunnerRaisesException(new IOException("Testing exception."));
  }

  @Test
  public void shouldFailIfDockerRunnerRaisesUserErrorInvalidExecutionException() throws Exception {
    shouldFailIfDockerRunnerRaisesException(new InvalidExecutionException("PEBKAC"));
  }

  private void shouldFailIfDockerRunnerRaisesException(Throwable throwable)
      throws IOException, IsClosedException {
    doThrow(throwable).when(dockerRunner)
        .start(any(WorkflowInstance.class), any(RunSpec.class));

    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING,
        StateData.newBuilder()
            .executionId(TEST_EXECUTION_ID)
            .executionDescription(EXECUTION_DESCRIPTION)
            .build(), NOW, COUNTER);

    dockerRunnerHandler.transitionInto(runState);

    verify(stateManager).receive(Event.runError(workflowInstance, throwable.getMessage()), COUNTER);
    verifyNoMoreInteractions(stateManager);
  }

  @Test
  public void shouldHaltIfMissingExecutionDescription() {
    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, State.SUBMITTING, StateData.newBuilder()
        .executionId(TEST_EXECUTION_ID)
        .build(), NOW, COUNTER);

    dockerRunnerHandler.transitionInto(runState);

    verify(stateManager).receiveIgnoreClosed(Event.halt(workflowInstance), COUNTER);
    verifyNoMoreInteractions(stateManager);
  }

  @Test
  public void shouldHaltIfMissingExecutionId() {
    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, State.SUBMITTING, StateData.newBuilder()
        .executionDescription(EXECUTION_DESCRIPTION)
        .build(), NOW, COUNTER);

    dockerRunnerHandler.transitionInto(runState);

    verify(stateManager).receiveIgnoreClosed(Event.halt(workflowInstance), COUNTER);
    verifyNoMoreInteractions(stateManager);
  }

  @Test
  public void shouldPerformCleanupOnFailed() {
    WorkflowConfiguration workflowConfiguration = configuration("--date", "{}", "--bar");
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.FAILED,
        StateData.newBuilder().executionId(TEST_EXECUTION_ID).build());

    dockerRunnerHandler.transitionInto(runState);

    verify(dockerRunner, timeout(60_000)).cleanup(workflowInstance, TEST_EXECUTION_ID);
  }

  @Test
  public void shouldPerformCleanupOnFailedThroughTransitions() {
    WorkflowConfiguration workflowConfiguration = configuration("--date", "{}", "--bar");
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, State.FAILED, StateData.newBuilder()
        .executionId(TEST_EXECUTION_ID)
        .build());

    dockerRunnerHandler.transitionInto(runState);

    verify(dockerRunner).cleanup(workflowInstance, TEST_EXECUTION_ID);
  }

  @Parameters({"SUBMITTED", "RUNNING"})
  @Test
  public void shouldPollWhenStarted(State state) {
    WorkflowConfiguration workflowConfiguration = configuration("--date", "{}", "--bar");
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, state,
        StateData.newBuilder().executionId(TEST_EXECUTION_ID).build());

    dockerRunnerHandler.transitionInto(runState);

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
