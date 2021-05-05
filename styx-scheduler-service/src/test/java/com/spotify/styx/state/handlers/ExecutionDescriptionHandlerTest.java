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

import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresentAndIs;
import static com.spotify.styx.docker.LabelValue.isNormalized;
import static com.spotify.styx.model.Schedule.HOURS;
import static com.spotify.styx.state.RunState.State.PREPARE;
import static com.spotify.styx.testdata.TestData.FLYTE_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.FULL_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.MINIMAL_WORKFLOW_CONFIGURATION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventVisitor;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfigurationBuilder;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.WorkflowWithState;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.WorkflowValidator;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ExecutionDescriptionHandlerTest {

  private static final String DOCKER_IMAGE = "my_docker_image";
  private static final String COMMIT_SHA = "71d70fca99e29812e81d1ed0a5c9d3559f4118e9";
  private static final Instant NOW = Instant.now();
  private static final long COUNTER = 17;

  private ExecutionDescriptionHandler toTest;

  @Mock Storage storage;
  @Mock EventRouter eventRouter;
  @Mock EventVisitor<Void> eventVisitor;

  @Captor ArgumentCaptor<WorkflowInstance> workflowInstanceCaptor;
  @Captor ArgumentCaptor<ExecutionDescription> executionDescriptionCaptor;
  @Captor ArgumentCaptor<String> executionIdCaptor;
  @Captor ArgumentCaptor<Event> eventCaptor;

  @Mock WorkflowValidator workflowValidator;

  @Before
  public void setUp() {
    when(workflowValidator.validateWorkflow(any())).thenReturn(Collections.emptyList());

    toTest = new ExecutionDescriptionHandler(storage, workflowValidator);
  }

  @Test
  public void shouldTransitionIntoSubmittingIfMissingDockerArgs() throws Exception {
    var workflow = Workflow.create("id", workflowConfiguration());
    var workflowState = WorkflowState.builder().enabled(true).build();
    var workflowWithState = WorkflowWithState.create(workflow, workflowState);
    var workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    var runState = RunState.create(workflowInstance, PREPARE, NOW, COUNTER);

    when(storage.workflowWithState(workflow.id())).thenReturn(Optional.of(workflowWithState));

    toTest.transitionInto(runState, eventRouter);

    verify(eventRouter).receive(eventCaptor.capture(), eq(COUNTER));

    var event = eventCaptor.getValue();
    event.accept(eventVisitor);
    verify(eventVisitor)
        .submit(workflowInstanceCaptor.capture(), executionDescriptionCaptor.capture(), executionIdCaptor.capture());

    assertThat(executionIdCaptor.getValue(), startsWith("styx-run-"));
    assertThat(executionDescriptionCaptor.getValue().dockerImage().orElseThrow(), is(DOCKER_IMAGE));
    assertThat(executionDescriptionCaptor.getValue().dockerArgs(), hasSize(0));
    assertThat(executionDescriptionCaptor.getValue().commitSha(), isPresentAndIs(COMMIT_SHA));
  }

  @Test
  public void shouldTransitionDockerWorkflowIntoSubmitting() throws Exception {
    var workflow = Workflow.create("id", workflowConfiguration("--date", "{}", "--bar"));
    var workflowState = WorkflowState.builder().enabled(true).build();
    var workflowWithState = WorkflowWithState.create(workflow, workflowState);
    var workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    var runState = RunState.create(workflowInstance,
        PREPARE,
        StateData.newBuilder().trigger(Trigger.natural()).build(),
        NOW,
        COUNTER);

    when(storage.workflowWithState(workflow.id())).thenReturn(Optional.of(workflowWithState));

    toTest.transitionInto(runState, eventRouter);

    verify(eventRouter).receive(eventCaptor.capture(), eq(COUNTER));

    var event = eventCaptor.getValue();
    event.accept(eventVisitor);

    verify(eventVisitor)
        .submit(workflowInstanceCaptor.capture(), executionDescriptionCaptor.capture(), executionIdCaptor.capture());

    assertThat(executionIdCaptor.getValue(), startsWith("styx-run-"));
    assertThat(executionDescriptionCaptor.getValue().dockerImage().orElseThrow(), is(DOCKER_IMAGE));
    assertThat(executionDescriptionCaptor.getValue().commitSha(), isPresentAndIs(COMMIT_SHA));
    assertThat(
        executionDescriptionCaptor.getValue().dockerArgs(),
        contains("--date", "2016-03-14", "--bar")
    );
    assertThat(executionDescriptionCaptor.getValue().env(), is(Map.of("foo", "bar")));
    assertThat(executionDescriptionCaptor.getValue().runningTimeout(), is(Optional.of(Duration.ZERO)));
    assertThat(executionDescriptionCaptor.getValue().retryCondition(), is(Optional.of("#tries<2")));
  }

  @Test
  public void shouldTransitionFlyteWorkflowIntoSubmitting() throws Exception {
    var workflow = Workflow.create("id", FLYTE_WORKFLOW_CONFIGURATION);
    var workflowState = WorkflowState.builder().enabled(true).build();
    var workflowWithState = WorkflowWithState.create(workflow, workflowState);
    var workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    var runState = RunState.create(workflowInstance,
        PREPARE,
        StateData.newBuilder().trigger(Trigger.natural()).build(),
        NOW,
        COUNTER);

    when(storage.workflowWithState(workflow.id())).thenReturn(Optional.of(workflowWithState));

    toTest.transitionInto(runState, eventRouter);

    verify(eventRouter).receive(eventCaptor.capture(), eq(COUNTER));

    var event = eventCaptor.getValue();
    event.accept(eventVisitor);

    verify(eventVisitor)
        .submit(workflowInstanceCaptor.capture(), executionDescriptionCaptor.capture(), executionIdCaptor.capture());

    assertThat(executionIdCaptor.getValue(), startsWith("styx-run-"));
    assertThat(executionDescriptionCaptor.getValue().flyteExecConf(),
        is(FLYTE_WORKFLOW_CONFIGURATION.flyteExecConf()));
    assertThat(executionDescriptionCaptor.getValue().commitSha(), isPresentAndIs(FLYTE_WORKFLOW_CONFIGURATION.commitSha().get()));
    assertThat(executionDescriptionCaptor.getValue().env(), is(FLYTE_WORKFLOW_CONFIGURATION.env()));
    assertThat(executionDescriptionCaptor.getValue().runningTimeout(), is(FLYTE_WORKFLOW_CONFIGURATION.runningTimeout()));
    assertThat(executionDescriptionCaptor.getValue().retryCondition(), is(FLYTE_WORKFLOW_CONFIGURATION.retryCondition()));
  }

  @Test
  public void shouldTransitionIntoFailedIfStorageError() throws Exception {
    var workflow = Workflow.create("id", workflowConfiguration("--date", "{}", "--bar"));
    var workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");

    var exception = new IOException("TEST");
    when(storage.workflowWithState(workflow.id())).thenThrow(exception);

    RunState runState = RunState.create(workflowInstance, PREPARE, NOW, COUNTER);

    toTest.transitionInto(runState, eventRouter);

    verify(eventRouter).receive(Event.runError(workflowInstance, exception.getMessage()), COUNTER);
  }

  @Test
  public void shouldHaltIfMissingWorkflow() throws Exception {
    var workflowInstance = WorkflowInstance.create(WorkflowId.create("c", "e"), "2016-03-14T15");
    var runState = RunState.create(workflowInstance, PREPARE, NOW, COUNTER);

    when(storage.workflowWithState(any())).thenReturn(Optional.empty());

    toTest.transitionInto(runState, eventRouter);

    verify(eventRouter).receiveIgnoreClosed(Event.halt(workflowInstance), COUNTER);
  }

  @Test
  public void shouldHaltIfMissingDockerImageAndFlyteExecDescription() throws Exception {
    var workflowConfiguration =
        WorkflowConfigurationBuilder.from(MINIMAL_WORKFLOW_CONFIGURATION)
            .dockerImage(Optional.empty())
            .flyteExecConf(Optional.empty())
            .build();
    var workflow = Workflow.create("id", workflowConfiguration);
    var workflowState = WorkflowState.builder().enabled(true).build();
    var workflowWithState = WorkflowWithState.create(workflow, workflowState);
    var workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    var runState = RunState.create(workflowInstance, PREPARE, NOW, COUNTER);

    when(storage.workflowWithState(workflow.id())).thenReturn(Optional.of(workflowWithState));

    toTest.transitionInto(runState, eventRouter);

    verify(eventRouter).receiveIgnoreClosed(Event.halt(workflowInstance), COUNTER);
  }

  @Test
  public void shouldHaltIfDisabledAndNaturallyTriggered() throws Exception {
    var workflow = Workflow.create("id", workflowConfiguration("--date", "{}", "--bar"));
    var workflowState = WorkflowState.builder().enabled(false).build();
    var workflowWithState = WorkflowWithState.create(workflow, workflowState);
    var workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    var runState = RunState.create(workflowInstance,
        PREPARE,
        StateData.newBuilder().trigger(Trigger.natural()).build(),
        NOW,
        COUNTER);

    when(storage.workflowWithState(workflow.id())).thenReturn(Optional.of(workflowWithState));

    toTest.transitionInto(runState, eventRouter);

    verify(eventRouter).receiveIgnoreClosed(Event.halt(workflowInstance), COUNTER);
  }

  @Test
  public void shouldHaltIfInvalidConfiguration() throws Exception {
    when(workflowValidator.validateWorkflow(any())).thenReturn(List.of("foo", "bar"));

    var workflow = Workflow.create("id", FULL_WORKFLOW_CONFIGURATION);
    var workflowState = WorkflowState.builder().enabled(true).build();
    var workflowWithState = WorkflowWithState.create(workflow, workflowState);
    var workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14T15");
    var runState = RunState.create(workflowInstance, PREPARE, NOW, COUNTER);

    when(storage.workflowWithState(workflow.id())).thenReturn(Optional.of(workflowWithState));

    toTest.transitionInto(runState, eventRouter);

    verify(eventRouter).receiveIgnoreClosed(Event.halt(workflowInstance), COUNTER);
  }

  @Test
  public void executionIdShouldConformToKubernetesLabelFormat() throws Exception {
    var workflow = Workflow.create("id", workflowConfiguration());
    var workflowState = WorkflowState.builder().enabled(true).build();
    var workflowWithState = WorkflowWithState.create(workflow, workflowState);
    var workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");
    var runState = RunState.create(workflowInstance, PREPARE, NOW, COUNTER);

    when(storage.workflowWithState(workflow.id())).thenReturn(Optional.of(workflowWithState));
    toTest.transitionInto(runState, eventRouter);
    verify(eventRouter).receive(eventCaptor.capture(), eq(COUNTER));

    var event = eventCaptor.getValue();
    event.accept(eventVisitor);
    verify(eventVisitor)
        .submit(workflowInstanceCaptor.capture(), executionDescriptionCaptor.capture(), executionIdCaptor.capture());

    assertTrue(isNormalized(executionIdCaptor.getValue()));
  }


  private WorkflowConfiguration workflowConfiguration(String... args) {
    return WorkflowConfiguration.builder()
        .id("styx.TestEndpoint")
        .schedule(HOURS)
        .commitSha(COMMIT_SHA)
        .dockerImage(DOCKER_IMAGE)
        .dockerArgs(Arrays.asList(args))
        .env("foo", "bar")
        .runningTimeout(Duration.ZERO)
        .retryCondition("#tries<2")
        .build();
  }
}
