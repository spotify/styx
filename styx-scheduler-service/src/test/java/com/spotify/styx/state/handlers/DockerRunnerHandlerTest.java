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
import static java.util.Optional.empty;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.model.DataEndpoint;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.testdata.TestData;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class DockerRunnerHandlerTest {

  private WorkflowInstance startWorkflowInstance;
  private String startImageName;
  private List<String> startArgs;
  private String cleanupExecutionId;

  private StateManager stateManager = new SyncStateManager();
  private DockerRunnerHandler dockerRunnerHandler =
      new DockerRunnerHandler(new FakeDockerRunner(), stateManager);

  private static final String TEST_EXECUTION_ID = "execution_1";
  private static final String TEST_DOCKER_IMAGE = "busybox:1.1";
  private static final ExecutionDescription EXECUTION_DESCRIPTION = ExecutionDescription.create(
      TEST_DOCKER_IMAGE, Arrays.asList("--date", "{}", "--bar"), empty(), empty());

  @Test
  public void shouldPassInArguments() throws Exception {
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, dataEndpoint());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    ExecutionDescription desc = ExecutionDescription.forImage(TEST_DOCKER_IMAGE);
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING,
        StateData.builder().executionDescription(desc).build());

    stateManager.initialize(runState);
    dockerRunnerHandler.transitionInto(runState);

    assertThat(startWorkflowInstance, is(workflowInstance));
    assertThat(startImageName, is(TEST_DOCKER_IMAGE));
  }

  @Test
  public void shouldInterpolatePartitioningArgument() throws Exception {
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, dataEndpoint());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING,
        StateData.builder().executionDescription(EXECUTION_DESCRIPTION).build());

    stateManager.initialize(runState);
    dockerRunnerHandler.transitionInto(runState);

    assertThat(startArgs, contains("--date", "2016-03-14T15" , "--bar"));
  }

  @Test
  public void shouldFailIfDockerRunnerRaisesException() throws Exception {
    DockerRunnerHandler dockerRunnerHandler =
        new DockerRunnerHandler(new ExceptionalDockerRunner(), stateManager);

    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, dataEndpoint());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING,
        StateData.builder().executionDescription(EXECUTION_DESCRIPTION).build());

    stateManager.initialize(runState);
    dockerRunnerHandler.transitionInto(runState);

    assertThat(stateManager.get(workflowInstance).state(), is(RunState.State.FAILED));
  }

  @Test
  public void shouldHaltIfMissingExecutionDescription() throws Exception {
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, dataEndpoint());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.SUBMITTING);

    stateManager.initialize(runState);
    dockerRunnerHandler.transitionInto(runState);

    assertThat(stateManager.get(workflowInstance).state(), is(RunState.State.ERROR));
  }

  @Test
  public void shouldPerformCleanupOnFailed() throws Exception {
    DataEndpoint endpoint = dataEndpoint("--date", "{}", "--bar");
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, endpoint);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.FAILED,
        StateData.builder().executionId(TEST_EXECUTION_ID).build());

    stateManager.initialize(runState);
    dockerRunnerHandler.transitionInto(runState);

    assertThat(cleanupExecutionId, is(TEST_EXECUTION_ID));
  }

  @Test
  public void shouldPerformCleanupOnFailedThroughTransitions() throws Exception {
    DataEndpoint endpoint = dataEndpoint("--date", "{}", "--bar");
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, endpoint);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.NEW, dockerRunnerHandler);

    stateManager.initialize(runState);
    stateManager.receive(Event.triggerExecution(workflowInstance, "trig"));
    stateManager.receive(Event.submit(workflowInstance, EXECUTION_DESCRIPTION));
    stateManager.receive(Event.runError(workflowInstance, ""));

    assertThat(cleanupExecutionId, is(TEST_EXECUTION_ID));
  }

  private DataEndpoint dataEndpoint(String... args) {
    return DataEndpoint.create(
        "styx.TestEndpoint",
        HOURS,
        Optional.of(TEST_DOCKER_IMAGE),
        Optional.of(Lists.newArrayList(args)),
        empty());
  }

  private class FakeDockerRunner implements DockerRunner {

    @Override
    public String start(WorkflowInstance workflowInstance, RunSpec runSpec) {
      startWorkflowInstance = workflowInstance;
      startImageName = runSpec.imageName();
      startArgs = runSpec.args();
      return TEST_EXECUTION_ID;
    }

    @Override
    public void cleanup(String executionId) {
      cleanupExecutionId = executionId;
    }

    @Override
    public void close() throws IOException {
    }
  }

  private class ExceptionalDockerRunner implements DockerRunner {

    @Override
    public String start(WorkflowInstance workflowInstance, RunSpec runSpec) throws IOException {
      throw new IOException("Testing exception.");
    }

    @Override
    public void cleanup(String executionId) {
      cleanupExecutionId = executionId;
    }

    @Override
    public void close() throws IOException {
    }
  }
}
