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

import static com.spotify.styx.model.Partitioning.HOURS;
import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.testdata.TestData;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DockerRunnerHandlerTest {

  private Storage storage = Mockito.mock(Storage.class);
  private RateLimiter rateLimiter = Mockito.mock(RateLimiter.class);
  private ExecutorService executor = Executors.newCachedThreadPool();

  private StateManager stateManager = Mockito.spy(new SyncStateManager());
  private DockerRunner dockerRunner = Mockito.mock(DockerRunner.class);
  private DockerRunnerHandler dockerRunnerHandler = new DockerRunnerHandler(
      dockerRunner, stateManager, storage, rateLimiter, executor);

  private static final String TEST_EXECUTION_ID = "execution_1";
  private static final String TEST_DOCKER_IMAGE = "busybox:1.1";
  private static final ExecutionDescription EXECUTION_DESCRIPTION = ExecutionDescription.create(
      TEST_DOCKER_IMAGE, Arrays.asList("--date", "{}", "--bar"), false, empty(), empty());
  private static final Trigger TRIGGER = Trigger.unknown("trig");

  @Captor ArgumentCaptor<WorkflowInstance> instanceCaptor;
  @Captor ArgumentCaptor<RunSpec> runSpecCaptor;

  @Before
  public void setUp() throws Exception {
    when(dockerRunner.start(any(WorkflowInstance.class), any(RunSpec.class)))
        .thenReturn(TEST_EXECUTION_ID);
    when(rateLimiter.acquire()).thenReturn(0.0);
  }

  @Test
  public void shouldBeRateLimiting() throws Exception {

    // Set up rate limiting to be under our control
    Semaphore semaphore = new Semaphore(0);
    when(rateLimiter.acquire()).then(a -> {
      semaphore.acquire();
      return 17.0;
    });

    // Submit an instance and verify that it blocks on the rate limiter
    WorkflowInstance instance1 = startWorkflowInstance("2016-03-14");
    verify(rateLimiter, timeout(60_000)).acquire();
    verifyZeroInteractions(dockerRunner);

    // Let the submission proceed and verify it does so
    semaphore.release();
    verify(dockerRunner, timeout(60_000).times(1)).start(eq(instance1), any(RunSpec.class));

    verifyNoMoreInteractions(rateLimiter);

    // Submit multiple instances and verify that each submission is rate limited
    WorkflowInstance instance2 = startWorkflowInstance("2016-03-15");
    WorkflowInstance instance3 = startWorkflowInstance("2016-03-16");
    WorkflowInstance instance4 = startWorkflowInstance("2016-03-17");

    verify(rateLimiter, timeout(60_000).times(4)).acquire();

    verifyNoMoreInteractions(dockerRunner);

    semaphore.release();
    verify(dockerRunner, timeout(60_000).times(2)).start(any(WorkflowInstance.class), any(RunSpec.class));
    verifyNoMoreInteractions(rateLimiter);

    semaphore.release();
    semaphore.release();

    verify(dockerRunner, timeout(60_000).times(4)).start(instanceCaptor.capture(), any(RunSpec.class));
    verifyNoMoreInteractions(rateLimiter);

    assertThat(instanceCaptor.getAllValues(), containsInAnyOrder(instance1, instance2, instance3, instance4));
  }

  private WorkflowInstance startWorkflowInstance(String parameter) throws Exception {
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, schedule());
    ExecutionDescription desc = ExecutionDescription.forImage(TEST_DOCKER_IMAGE);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), parameter);
    StateData stateData = StateData.newBuilder().executionDescription(desc).build();
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING, stateData);
    stateManager.initialize(runState);
    dockerRunnerHandler.transitionInto(runState);
    return workflowInstance;
  }

  @Test
  public void shouldPassInArguments() throws Exception {
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, schedule());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    ExecutionDescription desc = ExecutionDescription.forImage(TEST_DOCKER_IMAGE);
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING,
        StateData.newBuilder().executionDescription(desc).build());

    stateManager.initialize(runState);
    dockerRunnerHandler.transitionInto(runState);

    verify(dockerRunner, timeout(60_000)).start(instanceCaptor.capture(), runSpecCaptor.capture());

    assertThat(instanceCaptor.getValue(), is(workflowInstance));
    assertThat(runSpecCaptor.getValue().imageName(), is(TEST_DOCKER_IMAGE));
  }

  @Test
  public void shouldInterpolatePartitioningArgument() throws Exception {
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, schedule());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING,
        StateData.newBuilder().executionDescription(EXECUTION_DESCRIPTION).build());

    stateManager.initialize(runState);
    dockerRunnerHandler.transitionInto(runState);

    verify(dockerRunner, timeout(60_000)).start(instanceCaptor.capture(), runSpecCaptor.capture());

    assertThat(runSpecCaptor.getValue().args(), contains("--date", "2016-03-14T15" , "--bar"));
  }

  @Test
  public void shouldFailIfDockerRunnerRaisesException() throws Exception {
    when(dockerRunner.start(any(WorkflowInstance.class), any(RunSpec.class)))
        .thenThrow(new IOException("Testing exception."));

    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, schedule());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING,
        StateData.newBuilder().executionDescription(EXECUTION_DESCRIPTION).build());

    stateManager.initialize(runState);
    dockerRunnerHandler.transitionInto(runState);

    verify(stateManager, timeout(60_000)).receive(any(Event.class));

    assertThat(stateManager.get(workflowInstance).state(), is(RunState.State.FAILED));
  }

  @Test
  public void shouldHaltIfMissingExecutionDescription() throws Exception {
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, schedule());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING);

    stateManager.initialize(runState);
    dockerRunnerHandler.transitionInto(runState);

    verify(stateManager, timeout(60_000)).receive(any(Event.class));

    assertThat(stateManager.get(workflowInstance), is(nullValue()));
  }

  @Test
  public void shouldPerformCleanupOnFailed() throws Exception {
    WorkflowConfiguration workflowConfiguration = schedule("--date", "{}", "--bar");
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, workflowConfiguration);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.FAILED,
        StateData.newBuilder().executionId(TEST_EXECUTION_ID).build());

    stateManager.initialize(runState);
    dockerRunnerHandler.transitionInto(runState);

    verify(dockerRunner, timeout(60_000)).cleanup(TEST_EXECUTION_ID);
  }

  @Test
  public void shouldPerformCleanupOnFailedThroughTransitions() throws Exception {
    WorkflowConfiguration workflowConfiguration = schedule("--date", "{}", "--bar");
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, workflowConfiguration);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.NEW, dockerRunnerHandler);

    stateManager.initialize(runState);
    stateManager.receive(Event.triggerExecution(workflowInstance, TRIGGER));
    stateManager.receive(Event.dequeue(workflowInstance));
    stateManager.receive(Event.submit(workflowInstance, EXECUTION_DESCRIPTION));
    verify(stateManager, timeout(60_000)).receive(Event.submitted(workflowInstance, TEST_EXECUTION_ID));
    stateManager.receive(Event.runError(workflowInstance, ""));
    verify(dockerRunner).cleanup(TEST_EXECUTION_ID);
  }

  private WorkflowConfiguration schedule(String... args) {
    return WorkflowConfiguration.create(
        "styx.TestEndpoint",
        HOURS,
        Optional.of(TEST_DOCKER_IMAGE),
        Optional.of(Lists.newArrayList(args)),
        empty(),
        empty(),
        emptyList());
  }
}
