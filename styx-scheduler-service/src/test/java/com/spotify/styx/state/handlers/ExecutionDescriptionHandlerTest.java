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
import static com.spotify.styx.model.Schedule.HOURS;
import static com.spotify.styx.state.RunState.State.PREPARE;
import static com.spotify.styx.state.RunState.State.SUBMITTING;
import static com.spotify.styx.testdata.TestData.FULL_WORKFLOW_CONFIGURATION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfigurationBuilder;
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
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.WorkflowValidator;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExecutionDescriptionHandlerTest {

  private static final String DOCKER_IMAGE = "my_docker_image";
  private static final String COMMIT_SHA = "71d70fca99e29812e81d1ed0a5c9d3559f4118e9";
  private static final Trigger TRIGGER = Trigger.unknown("trig");

  private Storage storage;
  private StateManager stateManager;
  private ExecutionDescriptionHandler toTest;

  @Mock WorkflowValidator workflowValidator;

  @Before
  public void setUp() throws Exception {
    when(workflowValidator.validateWorkflow(any())).thenReturn(Collections.emptyList());
    storage = spy(new InMemStorage());
    stateManager = spy(new SyncStateManager());
    toTest = new ExecutionDescriptionHandler(storage, stateManager, workflowValidator);
  }

  @Test
  public void shouldTransitionIntoSubmittingIfMissingDockerArgs() throws Exception {
    Workflow workflow = Workflow.create("id", schedule());
    WorkflowState workflowState = WorkflowState.patchEnabled(true);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    RunState runState = RunState.fresh(workflowInstance, toTest);

    storage.storeWorkflow(workflow);
    storage.patchState(workflow.id(), workflowState);
    stateManager.trigger(runState, trigger);
    stateManager.receive(Event.triggerExecution(workflowInstance, TRIGGER));
    stateManager.receive(Event.dequeue(workflowInstance));

    RunState currentState = stateManager.get(workflowInstance);
    StateData data = currentState.data();

    assertThat(currentState.state(), is(SUBMITTING));
    assertTrue(data.executionId().isPresent());
    assertThat(data.executionId().get(), startsWith("styx-run-"));
    assertTrue(data.executionDescription().isPresent());
    assertThat(data.executionDescription().get().dockerImage(), is(DOCKER_IMAGE));
    assertThat(data.executionDescription().get().dockerArgs(), hasSize(0));
    assertThat(data.executionDescription().get().commitSha(), hasValue(COMMIT_SHA));
  }

  @Test
  public void shouldTransitionIntoSubmitting() throws Exception {
    Workflow workflow = Workflow.create("id", schedule("--date", "{}", "--bar"));
    WorkflowState workflowState = WorkflowState.patchEnabled(true);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    RunState runState = RunState.fresh(workflowInstance, toTest);

    storage.storeWorkflow(workflow);
    storage.patchState(workflow.id(), workflowState);
    stateManager.trigger(runState, trigger);
    stateManager.receive(Event.triggerExecution(workflowInstance, TRIGGER));
    stateManager.receive(Event.dequeue(workflowInstance));

    RunState currentState = stateManager.get(workflowInstance);
    StateData data = currentState.data();

    assertThat(currentState.state(), is(SUBMITTING));
    assertTrue(data.executionId().isPresent());
    assertThat(data.executionId().get(), startsWith("styx-run-"));
    assertTrue(data.executionDescription().isPresent());
    assertThat(data.executionDescription().get().dockerImage(), is(DOCKER_IMAGE));
    assertThat(data.executionDescription().get().dockerArgs(), contains("--date", "{}", "--bar"));
    assertThat(data.executionDescription().get().commitSha(), hasValue(COMMIT_SHA));
  }

  @Test
  public void shouldTransitionIntoFailedIfStorageError() throws Exception {
    Workflow workflow = Workflow.create("id", schedule("--date", "{}", "--bar"));
    WorkflowState workflowState = WorkflowState.patchEnabled(true);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");

    when(storage.workflow(workflow.id()))
        .thenThrow(new IOException("TEST"));

    storage.storeWorkflow(workflow);
    storage.patchState(workflow.id(), workflowState);

    RunState runState = RunState.fresh(workflowInstance, toTest);
    stateManager.trigger(runState, trigger);
    stateManager.receive(Event.triggerExecution(workflowInstance, TRIGGER));
    stateManager.receive(Event.dequeue(workflowInstance));

    RunState failed = stateManager.get(workflowInstance);
    assertThat(failed.state(), Matchers.is(RunState.State.FAILED));
  }

  @Test
  public void shouldHaltIfMissingWorkflow() throws Exception {
    WorkflowInstance workflowInstance = WorkflowInstance.create(WorkflowId.create("c", "e"), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, PREPARE);

    stateManager.trigger(runState, trigger);
    toTest.transitionInto(runState);

    RunState halted = stateManager.get(workflowInstance);
    assertThat(halted, Matchers.is(nullValue()));
  }

  @Test
  public void shouldHaltIfMissingDockerImage() throws Exception {
    WorkflowConfiguration workflowConfiguration =
        WorkflowConfigurationBuilder.from(schedule("foo", "bar"))
            .dockerImage(Optional.empty())
            .build();
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, PREPARE);

    storage.storeWorkflow(workflow);
    stateManager.trigger(runState, trigger);
    toTest.transitionInto(runState);

    RunState halted = stateManager.get(workflowInstance);
    assertThat(halted, Matchers.is(nullValue()));
  }

  @Test
  public void shouldHaltIfInvalidConfiguration() throws Exception {
    when(workflowValidator.validateWorkflow(any())).thenReturn(ImmutableList.of("foo", "bar"));

    Workflow workflow = Workflow.create("id", FULL_WORKFLOW_CONFIGURATION);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    RunState runState = RunState.create(workflowInstance, PREPARE);

    storage.storeWorkflow(workflow);
    stateManager.trigger(runState, trigger);
    toTest.transitionInto(runState);

    RunState halted = stateManager.get(workflowInstance);
    assertThat(halted, Matchers.is(nullValue()));
  }

  @Test
  public void shouldNotTransitIfStateManagerIsClosed() throws Exception {
    Workflow workflow = Workflow.create("id", schedule("--date", "{}", "--bar"));
    WorkflowState workflowState = WorkflowState.patchEnabled(true);
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    RunState runState = RunState.fresh(workflowInstance, toTest);

    doCallRealMethod()
        .doCallRealMethod()
        .doThrow(IsClosedException.class)
        .when(stateManager).receive(any(Event.class));

    storage.storeWorkflow(workflow);
    storage.patchState(workflow.id(), workflowState);
    stateManager.trigger(runState, trigger);
    stateManager.receive(Event.triggerExecution(workflowInstance, TRIGGER));
    stateManager.receive(Event.dequeue(workflowInstance));

    RunState currentState = stateManager.get(workflowInstance);
    StateData data = currentState.data();

    assertThat(currentState.state(), is(PREPARE));
    assertFalse(data.executionDescription().isPresent());
  }

  private WorkflowConfiguration schedule(String... args) {
    return WorkflowConfiguration.builder()
        .id("styx.TestEndpoint")
        .schedule(HOURS)
        .commitSha(COMMIT_SHA)
        .dockerImage(DOCKER_IMAGE)
        .dockerArgs(Arrays.asList(args))
        .build();
  }
}
