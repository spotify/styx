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

import static com.github.npathai.hamcrestopt.OptionalMatchers.hasValue;
import static com.spotify.styx.model.Partitioning.HOURS;
import static com.spotify.styx.state.RunState.State.SUBMITTING;
import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.InMemStorage;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.testdata.TestData;
import java.io.IOException;
import java.util.Optional;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class ExecutionDescriptionHandlerTest {

  private static final String DOCKER_IMAGE = "my_docker_image";
  private static final String COMMIT_SHA = "71d70fca99e29812e81d1ed0a5c9d3559f4118e9";
  private static final Trigger TRIGGER = Trigger.unknown("trig");

  private Storage storage;
  private StateManager stateManager;
  private ExecutionDescriptionHandler toTest;

  @Before
  public void setUp() throws Exception {
    storage = new InMemStorage();
    stateManager = new SyncStateManager();
    toTest = new ExecutionDescriptionHandler(storage, stateManager);
  }

  @Test
  public void shouldTransitionIntoSubmitting() throws Exception {
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, schedule("--date", "{}", "--bar"));
    WorkflowState workflowState = WorkflowState.builder()
        .enabled(true)
        .dockerImage(DOCKER_IMAGE)
        .commitSha(COMMIT_SHA)
        .build();
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    RunState runState = RunState.fresh(workflowInstance, toTest);

    storage.storeWorkflow(workflow);
    storage.patchState(workflow.id(), workflowState);
    stateManager.initialize(runState);
    stateManager.receive(Event.triggerExecution(workflowInstance, TRIGGER));
    stateManager.receive(Event.dequeue(workflowInstance));

    RunState currentState = stateManager.get(workflowInstance);
    StateData data = currentState.data();

    assertThat(currentState.state(), is(SUBMITTING));
    assertTrue(data.executionDescription().isPresent());
    assertThat(data.executionDescription().get().dockerImage(), is(DOCKER_IMAGE));
    assertThat(data.executionDescription().get().dockerArgs(), contains("--date", "{}", "--bar"));
    assertThat(data.executionDescription().get().commitSha(), hasValue(COMMIT_SHA));
  }

  @Test
  public void shouldTransitionIntoFailedIfStorageError() throws Exception {
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, schedule("--date", "{}", "--bar"));
    WorkflowState workflowState = WorkflowState.builder()
        .enabled(true)
        .dockerImage(DOCKER_IMAGE)
        .commitSha(COMMIT_SHA)
        .build();
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");

    Storage storageSpy = Mockito.spy(storage);
    Mockito.when(storageSpy.workflowState(workflowInstance.workflowId()))
        .thenThrow(new IOException("TEST"));
    storageSpy.storeWorkflow(workflow);
    storageSpy.patchState(workflow.id(), workflowState);

    toTest = new ExecutionDescriptionHandler(storageSpy, stateManager);

    RunState runState = RunState.fresh(workflowInstance, toTest);

    stateManager.initialize(runState);
    stateManager.receive(Event.triggerExecution(workflowInstance, TRIGGER));
    stateManager.receive(Event.dequeue(workflowInstance));

    RunState failed = stateManager.get(workflowInstance);
    assertThat(failed.state(), Matchers.is(RunState.State.FAILED));
  }

  @Test
  public void shouldHaltIfMissingWorkflow() throws Exception {
    WorkflowInstance workflowInstance = WorkflowInstance.create(WorkflowId.create("c", "e"), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.PREPARE);

    stateManager.initialize(runState);
    toTest.transitionInto(runState);

    RunState halted = stateManager.get(workflowInstance);
    assertThat(halted, Matchers.is(nullValue()));
  }

  @Test
  public void shouldHaltIfMissingDockerArgs() throws Exception {
    Schedule schedule = TestData.DAILY_SCHEDULE;
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, schedule);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.PREPARE);

    storage.storeWorkflow(workflow);
    stateManager.initialize(runState);
    toTest.transitionInto(runState);

    RunState halted = stateManager.get(workflowInstance);
    assertThat(halted, Matchers.is(nullValue()));
  }

  @Test
  public void shouldHaltIfMissingDockerImage() throws Exception {
    Schedule schedule = schedule("foo", "bar");
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, schedule);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.PREPARE);

    storage.storeWorkflow(workflow);
    stateManager.initialize(runState);
    toTest.transitionInto(runState);

    RunState halted = stateManager.get(workflowInstance);
    assertThat(halted, Matchers.is(nullValue()));
  }

  @Test
  public void shouldFallbackToDockerImageInScheduleDefinition() throws Exception {
    Schedule schedule = Schedule.create(
        "styx.TestEndpoint",
        HOURS,
        Optional.of("legacy-docker-image"),
        Optional.of(Lists.newArrayList("foo", "bar")),
        empty(),
        empty(),
        emptyList());
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, schedule);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, RunState.State.PREPARE);

    storage.storeWorkflow(workflow);
    stateManager.initialize(runState);
    toTest.transitionInto(runState);

    RunState currentState = stateManager.get(workflowInstance);
    StateData data = currentState.data();

    assertThat(currentState.state(), is(SUBMITTING));
    assertTrue(data.executionDescription().isPresent());
    assertThat(data.executionDescription().get().dockerImage(), is("legacy-docker-image"));
  }

  private Schedule schedule(String... args) {
    return Schedule.create(
        "styx.TestEndpoint",
        HOURS,
        Optional.empty(),
        Optional.of(Lists.newArrayList(args)),
        empty(),
        empty(),
        emptyList());
  }
}
