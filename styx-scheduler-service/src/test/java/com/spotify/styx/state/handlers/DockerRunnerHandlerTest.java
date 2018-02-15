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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.docker.InvalidExecutionException;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventVisitor;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DockerRunnerHandlerTest {

  private DockerRunnerHandler dockerRunnerHandler;

  private static final String TEST_EXECUTION_ID = "execution_1";
  private static final String TEST_DOCKER_IMAGE = "busybox:1.1";
  private static final ExecutionDescription EXECUTION_DESCRIPTION = ExecutionDescription.builder()
      .dockerImage(TEST_DOCKER_IMAGE)
      .dockerArgs("--date", "{}", "--bar")
      .build();

  @Mock DockerRunner dockerRunner;
  @Mock StateManager stateManager;
  @Mock EventVisitor<Void> eventVisitor;

  @Captor ArgumentCaptor<WorkflowInstance> instanceCaptor;
  @Captor ArgumentCaptor<RunSpec> runSpecCaptor;
  @Captor ArgumentCaptor<Event> eventCaptor;

  @Before
  public void setUp() throws Exception {
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
        StateData.newBuilder().executionDescription(EXECUTION_DESCRIPTION).build());

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

    verify(stateManager, timeout(60_000)).receive(Event.submitted(workflowInstance, TEST_EXECUTION_ID));
  }

  @Test
  public void shouldFailIfDockerRunnerRaisesException() throws Exception {
    shouldFailIfDockerRunnerRaisesException(new IOException("Testing exception."));
  }

  @Test
  public void shouldFailIfDockerRunnerRaisesUserErrorInvalidExecutionException() throws Exception {
    shouldFailIfDockerRunnerRaisesException(new InvalidExecutionException("PEBKAC"));
  }

  void shouldFailIfDockerRunnerRaisesException(Throwable throwable)
      throws IOException, IsClosedException {
    doThrow(throwable).when(dockerRunner)
        .start(any(WorkflowInstance.class), any(RunSpec.class));

    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING,
        StateData.newBuilder()
            .executionId(TEST_EXECUTION_ID)
            .executionDescription(EXECUTION_DESCRIPTION)
            .build());

    dockerRunnerHandler.transitionInto(runState);

    // Verify that the state manager receives two events:
    // 1. submitted
    // 2. runError
    verify(stateManager, timeout(60_000).times(2)).receive(eventCaptor.capture());

    Event event1 = eventCaptor.getAllValues().get(0);
    Event event2 = eventCaptor.getAllValues().get(1);

    event1.accept(eventVisitor);
    verify(eventVisitor).submitted(workflowInstance, TEST_EXECUTION_ID);

    event2.accept(eventVisitor);
    verify(eventVisitor).runError(workflowInstance, throwable.getMessage());
  }

  @Test
  public void shouldHaltIfMissingExecutionDescription() throws Exception {
    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING);

    dockerRunnerHandler.transitionInto(runState);

    verify(stateManager).receiveIgnoreClosed(Event.halt(workflowInstance));
  }

  @Test
  public void shouldPerformCleanupOnFailed() throws Exception {
    WorkflowConfiguration workflowConfiguration = configuration("--date", "{}", "--bar");
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.FAILED,
        StateData.newBuilder().executionId(TEST_EXECUTION_ID).build());

    dockerRunnerHandler.transitionInto(runState);

    verify(dockerRunner, timeout(60_000)).cleanup(workflowInstance, TEST_EXECUTION_ID);
  }

  @Test
  public void shouldPerformCleanupOnFailedThroughTransitions() throws Exception {
    WorkflowConfiguration workflowConfiguration = configuration("--date", "{}", "--bar");
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, State.FAILED, StateData.newBuilder()
        .executionId(TEST_EXECUTION_ID)
        .build());

    dockerRunnerHandler.transitionInto(runState);

    verify(dockerRunner).cleanup(workflowInstance, TEST_EXECUTION_ID);
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
