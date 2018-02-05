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

package com.spotify.styx.api;

import static com.github.npathai.hamcrestopt.OptionalMatchers.hasValue;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresent;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withCode;
import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static com.spotify.styx.serialization.Json.serialize;
import static com.spotify.styx.testdata.TestData.FULL_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.INVALID_SHA;
import static com.spotify.styx.util.FutureUtil.exceptionallyCompletedFuture;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.test.ServiceHelper;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfigurationBuilder;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.InMemStorage;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.TriggerUtil;
import com.spotify.styx.util.WorkflowValidator;
import com.spotify.styx.workflow.WorkflowInitializationException;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import okio.ByteString;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

/**
 * API endpoints for interacting directly with the scheduler
 */
@RunWith(MockitoJUnitRunner.class)
public class SchedulerResourceTest {

  private final InMemStorage storage = new InMemStorage();
  private final StateManager stateManager = Mockito.spy(new SyncStateManager());

  private static final WorkflowInstance WFI = WorkflowInstance
      .create(TestData.WORKFLOW_ID, "12345");

  private final Workflow HOURLY_WORKFLOW = Workflow.create("styx",
                                                           TestData.HOURLY_WORKFLOW_CONFIGURATION);
  private final Workflow HOURLY_WORKFLOW_WITH_INVALID_OFFSET =
      Workflow.create("styx", TestData.HOURLY_WORKFLOW_CONFIGURATION_WITH_INVALID_OFFSET);
  private final Workflow DAILY_WORKFLOW = Workflow.create("styx",
                                                          TestData.DAILY_WORKFLOW_CONFIGURATION);
  private final Workflow FULL_DAILY_WORKFLOW = Workflow.create("styx",
                                                               FULL_WORKFLOW_CONFIGURATION);
  private final Workflow WEEKLY_WORKFLOW = Workflow.create("styx",
                                                           TestData.WEEKLY_WORKFLOW_CONFIGURATION);
  private final Workflow MONTHLY_WORKFLOW = Workflow.create("styx",
                                                            TestData.MONTHLY_WORKFLOW_CONFIGURATION);
  private Optional<Workflow> triggeredWorkflow = Optional.empty();
  private Optional<Trigger> trigger = Optional.empty();
  private Optional<Instant> triggeredInstant = Optional.empty();

  @Mock WorkflowValidator workflowValidator;

  @Rule
  public ServiceHelper serviceHelper = getServiceHelper(stateManager, storage);

  private void workflowChangeListener(Workflow workflow) {
    if (workflow.equals(HOURLY_WORKFLOW_WITH_INVALID_OFFSET)) {
      throw new WorkflowInitializationException(new RuntimeException());
    }

    try {
      storage.storeWorkflow(workflow);
    } catch (IOException e) {
    }
  }

  private void workflowRemoveListener(Workflow workflow) {
    try {
      storage.delete(workflow.id());
    } catch (IOException e) {
    }
  }

  @Before
  public void setUp() throws Exception {
    when(workflowValidator.validateWorkflow(any())).thenReturn(Collections.emptyList());
    when(workflowValidator.validateWorkflowConfiguration(any())).thenReturn(Collections.emptyList());
  }

  private ServiceHelper getServiceHelper(StateManager stateManager, Storage storage) {
    return ServiceHelper.create((environment) -> {
      final SchedulerResource schedulerResource = new SchedulerResource(
          stateManager,
          (workflow, trigger1, instant) -> {
            this.triggeredWorkflow = Optional.of(workflow);
            this.trigger = Optional.of(trigger1);
            this.triggeredInstant = Optional.of(instant);
            return CompletableFuture.completedFuture(null);
          },
          this::workflowChangeListener, this::workflowRemoveListener, storage,
          () -> Instant.parse("2015-12-31T23:59:10.000Z"),
          workflowValidator);

      environment.routingEngine()
          .registerRoutes(schedulerResource.routes());
    }, "styx");
  }

  private Response<ByteString> requestAndWaitTriggerWorkflowInstance(WorkflowInstance toTrigger)
      throws Exception {

    ByteString eventPayload = ByteString.of(OBJECT_MAPPER.writeValueAsBytes(toTrigger));
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/trigger", eventPayload);

    return post.toCompletableFuture().get();
  }

  @Test
  public void testRetry() throws Exception {
    RunState initialState = RunState.create(
        WFI, RunState.State.QUEUED, StateData.newBuilder().retryDelayMillis(1000L).build());
    stateManager.initialize(initialState);

    ByteString wfiPayload = serialize(WFI);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/retry", wfiPayload);

    post.toCompletableFuture().get(); // block until done

    verify(stateManager, times(1)).receive(any());
    verify(stateManager, times(1))
        .receive(Event.retryAfter(WFI, 0L));
    RunState finalState = stateManager.get(WFI);
    assertThat(finalState.data().retryDelayMillis(), is(Optional.of(0L)));
    assertThat(finalState.state(), is(RunState.State.QUEUED));
  }

  @Test
  public void testRetryWithDelayParameter() throws Exception {
    RunState initialState = RunState.create(
        WFI, RunState.State.QUEUED, StateData.newBuilder().retryDelayMillis(1000L).build());
    stateManager.initialize(initialState);

    ByteString wfiPayload = serialize(WFI);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/retry?delay=500", wfiPayload);

    post.toCompletableFuture().get(); // block until done

    verify(stateManager, times(1)).receive(any());
    verify(stateManager, times(1))
        .receive(Event.retryAfter(WFI, 500L));
    RunState finalState = stateManager.get(WFI);
    assertThat(finalState.data().retryDelayMillis(), is(Optional.of(500L)));
    assertThat(finalState.state(), is(RunState.State.QUEUED));
  }

  @Test
  public void testRetryWithWrongDelayParameter() throws Exception {
    RunState initialState = RunState.create(
        WFI, RunState.State.QUEUED, StateData.newBuilder().retryDelayMillis(1000L).build());
    stateManager.initialize(initialState);

    ByteString wfiPayload = serialize(WFI);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/retry?delay=abc", wfiPayload);

    final Response<ByteString> response = post.toCompletableFuture().get();

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST
        .withReasonPhrase("Delay parameter could not be parsed"))));
    verify(stateManager, never()).receive(any());
    RunState finalState = stateManager.get(WFI);
    assertThat(finalState.data().retryDelayMillis(), is(Optional.of(1000L)));
    assertThat(finalState.state(), is(RunState.State.QUEUED));
  }

  @Test
  public void testHalt() throws Exception {
    RunState initialState = RunState.create(WFI, RunState.State.RUNNING);
    stateManager.initialize(initialState);

    ByteString wfiPayload = serialize(WFI);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/halt", wfiPayload);

    post.toCompletableFuture().get(); // block until done

    verify(stateManager, times(1)).receive(any());
    verify(stateManager, times(1))
        .receive(Event.halt(WFI));
    assertThat(stateManager.get(WFI), is(nullValue()));
  }

  @Test
  public void testInjectDequeueEvent() throws Exception {
    RunState initialState = RunState.create(
        WFI, RunState.State.QUEUED, StateData.newBuilder().retryDelayMillis(1000L).build());
    stateManager.initialize(initialState);

    Event injectedEvent = Event.dequeue(WFI);
    ByteString eventPayload = serialize(injectedEvent);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/events", eventPayload);

    post.toCompletableFuture().get(); // block until done

    verify(stateManager, times(1)).receive(any());
    verify(stateManager, times(1))
        .receive(Event.retryAfter(WFI, 0L));
    RunState finalState = stateManager.get(WFI);
    assertThat(finalState.data().retryDelayMillis(), is(Optional.of(0L)));
    assertThat(finalState.state(), is(RunState.State.QUEUED));
  }

  @Test
  public void testInjectHaltEvent() throws Exception {
    RunState initialState = RunState.create(WFI, RunState.State.RUNNING);
    stateManager.initialize(initialState);

    Event injectedEvent = Event.halt(WFI);
    ByteString eventPayload = serialize(injectedEvent);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/events", eventPayload);

    post.toCompletableFuture().get(); // block until done

    verify(stateManager, times(1)).receive(any());
    verify(stateManager, times(1))
        .receive(Event.halt(WFI));
    assertThat(stateManager.get(WFI), is(nullValue()));
  }

  @Test
  public void shouldFailOnInjectRetryEvent() throws Exception {
    RunState initialState = RunState.create(
        WFI, RunState.State.QUEUED, StateData.newBuilder().retryDelayMillis(1000L).build());
    stateManager.initialize(initialState);

    Event injectedEvent = Event.retry(WFI);
    ByteString eventPayload = serialize(injectedEvent);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/events", eventPayload);

    final Response<ByteString> response = post.toCompletableFuture().get();
    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
  }

  @Test
  public void shouldFailOnForbiddenTransitionForEvent() throws Exception {
    RunState initialState = RunState.create(WFI, RunState.State.RUNNING);
    stateManager.initialize(initialState);

    ByteString eventPayload = serialize(WFI);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/retry", eventPayload);

    final Response<ByteString> response = post.toCompletableFuture().get();
    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
  }

  @Test
  public void injectEventShouldReturnServerErrorIfExceptionalFuture() throws Exception {
    StateManager failingStateManager = mock(SyncStateManager.class);
    when(failingStateManager.receive(any())).thenReturn(
        exceptionallyCompletedFuture(new RuntimeException("test")));

    ServiceHelper serviceHelper = getServiceHelper(failingStateManager, storage);
    serviceHelper.start();

    ByteString eventPayload = serialize(WFI);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/retry", eventPayload);

    final Response<ByteString> response = post.toCompletableFuture().get();
    assertThat(response, hasStatus(withCode(Status.INTERNAL_SERVER_ERROR)));
    System.out.println(response);
  }

  @Test
  public void injectEventShouldReturnServerErrorIfRuntimeException() throws Exception {
    StateManager failingStateManager = mock(SyncStateManager.class);
    when(failingStateManager.receive(any())).thenThrow(new RuntimeException("test"));

    ServiceHelper serviceHelper = getServiceHelper(failingStateManager, storage);
    serviceHelper.start();

    ByteString eventPayload = serialize(WFI);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/retry", eventPayload);

    final Response<ByteString> response = post.toCompletableFuture().get();
    assertThat(response, hasStatus(withCode(Status.INTERNAL_SERVER_ERROR)));
    System.out.println(response);
  }

  @Test
  public void shouldFailOnInjectEventForUnknownWorkflowInstance() throws Exception {
    ByteString eventPayload = serialize(WFI);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/retry", eventPayload);

    final Response<ByteString> response = post.toCompletableFuture().get();
    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
  }

  @Test
  public void testCreateWorkflow() throws Exception {
    final Response<ByteString> r = serviceHelper.request(
        "POST", SchedulerResource.BASE + "/workflows/styx",
        serialize(FULL_DAILY_WORKFLOW.configuration())).toCompletableFuture().get();
    assertThat(r, hasStatus(withCode(Status.OK)));
    assertThat(storage.workflow(FULL_DAILY_WORKFLOW.id()), isPresent());
    verify(workflowValidator).validateWorkflowConfiguration(FULL_DAILY_WORKFLOW.configuration());
    storage.delete(FULL_DAILY_WORKFLOW.id());
  }

  @Test
  public void testCreateWorkflowWithoutDockerImage() throws Exception {
    final WorkflowConfiguration workflowConfiguration =
        WorkflowConfigurationBuilder.from(FULL_DAILY_WORKFLOW.configuration())
            .dockerImage(Optional.empty()).build();
    final Response<ByteString> r = serviceHelper.request(
        "POST", SchedulerResource.BASE + "/workflows/styx",
        serialize(workflowConfiguration)).toCompletableFuture().get();
    assertThat(r, hasStatus(withCode(Status.BAD_REQUEST)));
    assertThat(storage.workflow(FULL_DAILY_WORKFLOW.id()), isEmpty());
  }

  @Test
  public void testCreateWorkflowInvalidConfiguration() throws Exception {
    when(workflowValidator.validateWorkflowConfiguration(any())).thenReturn(ImmutableList.of("bad", "f00d"));
    final Response<ByteString> r = serviceHelper.request(
        "POST", SchedulerResource.BASE + "/workflows/styx",
        serialize(FULL_DAILY_WORKFLOW.configuration())).toCompletableFuture().get();
    assertThat(r.status(),
        is(Status.BAD_REQUEST.withReasonPhrase("Invalid workflow configuration: bad, f00d")));
    verify(workflowValidator).validateWorkflowConfiguration(FULL_DAILY_WORKFLOW.configuration());
  }

  @Test
  public void testUpdateWorkflow() throws Exception {
    ByteString workflowPayload = serialize(HOURLY_WORKFLOW.configuration());
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/workflows/styx", workflowPayload);

    assertThat(post.toCompletableFuture().get(), hasStatus(withCode(Status.OK)));
    assertThat(storage.workflow(HOURLY_WORKFLOW.id()), isPresent());

    CompletionStage<Response<ByteString>> post2 =
        serviceHelper.request("POST", SchedulerResource.BASE + "/workflows/styx", workflowPayload);

    assertThat(post2.toCompletableFuture().get(), hasStatus(withCode(Status.OK)));
    assertThat(storage.workflow(HOURLY_WORKFLOW.id()), isPresent());
    storage.delete(HOURLY_WORKFLOW.id());
  }

  @Test
  public void testUpdateWorkflowWithInvalidOffset() throws Exception {
    ByteString workflowPayload = serialize(HOURLY_WORKFLOW.configuration());
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/workflows/styx", workflowPayload);

    assertThat(post.toCompletableFuture().get(), hasStatus(withCode(Status.OK)));
    assertThat(storage.workflow(HOURLY_WORKFLOW.id()), isPresent());

    ByteString workflowWithInvalidOffset =
        serialize(HOURLY_WORKFLOW_WITH_INVALID_OFFSET.configuration());
    CompletionStage<Response<ByteString>> post2 =
        serviceHelper.request("POST", SchedulerResource.BASE + "/workflows/styx",
                              workflowWithInvalidOffset);

    assertThat(post2.toCompletableFuture().get(), hasStatus(withCode(Status.BAD_REQUEST)));
    assertThat(storage.workflow(HOURLY_WORKFLOW.id()), isPresent());
    storage.delete(HOURLY_WORKFLOW.id());
  }

  @Test
  public void testWorkflowWithInvalidShaFails() throws Exception {
    final WorkflowConfiguration workflowConfiguration =
        WorkflowConfigurationBuilder.from(HOURLY_WORKFLOW.configuration())
            .commitSha(INVALID_SHA)
            .build();
    ByteString workflowPayload = serialize(workflowConfiguration);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/workflows/styx", workflowPayload);

    assertThat(post.toCompletableFuture().get(), hasStatus(withCode(Status.BAD_REQUEST)));
  }

  @Test
  public void testDeleteWorkflowWhenPresent() throws Exception {
    storage.storeWorkflow(HOURLY_WORKFLOW);
    Response<ByteString> response = serviceHelper.request("DELETE", String
        .join("/", SchedulerResource.BASE, "workflows", HOURLY_WORKFLOW.componentId(),
              HOURLY_WORKFLOW.workflowId())).toCompletableFuture().get();
    assertThat(response, hasStatus(withCode(Status.NO_CONTENT)));
    assertThat(storage.workflow(HOURLY_WORKFLOW.id()), isEmpty());
  }

  @Test
  public void testDeleteWorkflowWhenNotPresent() throws Exception {
    Response<ByteString> response = serviceHelper.request("DELETE", String
        .join("/", SchedulerResource.BASE, "workflows", HOURLY_WORKFLOW.componentId(),
              HOURLY_WORKFLOW.workflowId())).toCompletableFuture().get();
    assertThat(response, hasStatus(withCode(Status.NOT_FOUND)));
    assertThat(storage.workflow(HOURLY_WORKFLOW.id()), isEmpty());
  }

  @Test
  public void testRejectUnknownWorkflowInstance() throws Exception {
    ByteString eventPayload = serialize(WFI);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/retry", eventPayload);

    final Response<ByteString> response = post.toCompletableFuture().get();

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
  }

  @Test
  public void testTriggeredWorkflowGeneratesTrigger() throws Exception {
    storage.storeWorkflow(HOURLY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2014-12-31T23");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));
    assertThat(trigger, isPresent());
    assertThat(TriggerUtil.triggerType(trigger.get()), is("adhoc"));
    assertThat(TriggerUtil.triggerId(trigger.get()), startsWith("ad-hoc-cli-"));
  }

  @Test
  public void testTriggerWorkflowInstanceHourly() throws Exception {
    storage.storeWorkflow(HOURLY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2014-12-31T23");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));
    assertThat(triggeredInstant, hasValue(Instant.parse("2014-12-31T23:00:00.000Z")));
    assertThat(triggeredWorkflow, hasValue(HOURLY_WORKFLOW));
  }

  @Test
  public void testTriggerWorkflowInstanceDaily() throws Exception {
    storage.storeWorkflow(DAILY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(DAILY_WORKFLOW.id(), "2014-12-31");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));
    assertThat(triggeredInstant, hasValue(Instant.parse("2014-12-31T00:00:00.000Z")));
    assertThat(triggeredWorkflow, hasValue(DAILY_WORKFLOW));
  }

  @Test
  public void testTriggerWorkflowInstanceWeekly() throws Exception {
    storage.storeWorkflow(WEEKLY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(WEEKLY_WORKFLOW.id(), "2016-01-03");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));
    assertThat(triggeredInstant, hasValue(Instant.parse("2015-12-28T00:00:00.000Z")));
    assertThat(triggeredWorkflow, hasValue(WEEKLY_WORKFLOW));
  }

  @Test
  public void testTriggerWorkflowInstanceMissingStorage() throws Exception {
    WorkflowInstance toTrigger = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-12-31T23");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("The specified workflow is not"
                                                      + " found in the scheduler")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }

  @Test
  public void testTriggerWorkflowInstanceFailingStorage() throws Exception {
    Storage failingStorage = mock(Storage.class);
    when(failingStorage.workflow(any(WorkflowId.class))).thenThrow(new IOException("Error"));

    ServiceHelper serviceHelper = getServiceHelper(stateManager, failingStorage);
    serviceHelper.start();

    WorkflowInstance toTrigger = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-12-31T23");
    ByteString eventPayload = ByteString.of(OBJECT_MAPPER.writeValueAsBytes(toTrigger));
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/trigger", eventPayload);
    Response<ByteString> response = post.toCompletableFuture().get();

    assertThat(response.status(),
               is(Status.INTERNAL_SERVER_ERROR
                      .withReasonPhrase("An error occurred while retrieving "
                                        + "workflow specifications")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }

  @Test
  public void testTriggerWorkflowInstanceUnsupportedSchedule() throws Exception {
    storage.storeWorkflow(MONTHLY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(MONTHLY_WORKFLOW.id(), "2014-12-01");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));
    assertThat(triggeredInstant, hasValue(Instant.parse("2014-12-01T00:00:00.000Z")));
    assertThat(triggeredWorkflow, hasValue(MONTHLY_WORKFLOW));
  }

  @Test
  public void testTriggerWorkflowInstanceFuture() throws Exception {
    storage.storeWorkflow(HOURLY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-12-31T23");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("Cannot trigger an instance of the future")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }

  @Test
  public void testTriggerWorkflowInstanceParseDayForHourly() throws Exception {
    storage.storeWorkflow(HOURLY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2015-12-31");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));
    assertThat(triggeredInstant, hasValue(Instant.parse("2015-12-31T00:00:00.000Z")));
    assertThat(triggeredWorkflow, hasValue(HOURLY_WORKFLOW));
  }

  @Test
  public void testTriggerWorkflowInstanceParseFailure() throws Exception {
    storage.storeWorkflow(HOURLY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(DAILY_WORKFLOW.id(), "2015");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("Cannot parse time parameter 2015 - "
                                                      + "Text '2015' could not be parsed at index 4")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }

  @Test
  public void testTriggerAlreadyActiveWorkflowInstance() throws Exception {
    storage.storeWorkflow(DAILY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(DAILY_WORKFLOW.id(), "2015-12-31");
    stateManager.initialize(RunState.fresh(toTrigger));

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("The specified instance is already "
                                                      + "active in the scheduler")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }

  @Test
  public void testTriggerMissingImage() throws Exception {
    final WorkflowConfiguration workflowConfiguration =
        WorkflowConfigurationBuilder.from(FULL_DAILY_WORKFLOW.configuration())
            .dockerImage(Optional.empty()).build();
    final Workflow workflow = Workflow.create("styx", workflowConfiguration);
    storage.storeWorkflow(workflow);
    WorkflowInstance toTrigger = WorkflowInstance.create(workflow.id(), "2015-12-31");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("Workflow is missing docker image")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }

  @Test
  public void testTriggerInvalidWorkflowConfiguration() throws Exception {
    when(workflowValidator.validateWorkflow(DAILY_WORKFLOW)).thenReturn(ImmutableList.of("bad", "f00d"));

    storage.storeWorkflow(DAILY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(DAILY_WORKFLOW.id(), "2015-12-31");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("Invalid workflow configuration: bad, f00d")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }
}
