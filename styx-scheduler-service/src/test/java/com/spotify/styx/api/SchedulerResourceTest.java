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
import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withCode;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withReasonPhrase;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.test.ServiceHelper;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventSerializer;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.storage.InMemStorage;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.testdata.TestData;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import okio.ByteString;
import org.junit.Rule;
import org.junit.Test;

/**
 * API endpoints for interacting directly with the scheduler
 */
public class SchedulerResourceTest {

  private final InMemStorage storage = new InMemStorage();
  private final EventSerializer eventSerializer = new EventSerializer();
  private final StateManager stateManager = new SyncStateManager();

  private static final WorkflowInstance WFI = WorkflowInstance.create(TestData.WORKFLOW_ID, "12345");
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
      .registerModule(new Jdk8Module());
  private final Workflow HOURLY_WORKFLOW = Workflow.create("styx",
                                                    TestData.WORKFLOW_URI,
                                                    TestData.HOURLY_DATA_ENDPOINT);
  private final Workflow DAILY_WORKFLOW = Workflow.create("styx",
                                                           TestData.WORKFLOW_URI,
                                                           TestData.DAILY_DATA_ENDPOINT);
  private final Workflow WEEKLY_WORKFLOW = Workflow.create("styx",
                                                          TestData.WORKFLOW_URI,
                                                          TestData.WEEKLY_DATA_ENDPOINT);
  private final Workflow MONTHLY_WORKFLOW = Workflow.create("styx",
                                                            TestData.WORKFLOW_URI,
                                                            TestData.MONTHLY_DATA_ENDPOINT);
  private Optional<Workflow> triggeredWorkflow = Optional.empty();
  private Optional<String> triggerId = Optional.empty();
  private Optional<Instant> triggeredInstant = Optional.empty();

  @Rule
  public ServiceHelper serviceHelper = ServiceHelper.create(this::init, "styx");

  void init(Environment environment) {
    final SchedulerResource schedulerResource = new SchedulerResource(
        stateManager,
        (workflow, triggerId, instant) -> {
          this.triggeredWorkflow = Optional.of(workflow);
          this.triggerId = Optional.of(triggerId);
          this.triggeredInstant = Optional.of(instant);
        },
        storage,
        () -> Instant.parse("2015-12-31T23:59:10.000Z"));

    environment.routingEngine()
        .registerRoutes(schedulerResource.routes());
  }

  private Response<ByteString> requestAndWaitTriggerWorkflowInstance(WorkflowInstance toTrigger)
      throws Exception {

    ByteString eventPayload = ByteString.of(MAPPER.writeValueAsBytes(toTrigger));
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/trigger", eventPayload);

    return post.toCompletableFuture().get();
  }

  @Test
  public void testInjectEvent() throws Exception {
    RunState initialState = RunState.create(WFI, RunState.State.RUNNING);
    stateManager.initialize(initialState);

    Event injectedEvent = Event.timeout(WFI);
    ByteString eventPayload = eventSerializer.serialize(injectedEvent);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/events", eventPayload);

    post.toCompletableFuture().get(); // block until done

    RunState finalState = stateManager.get(WFI);
    assertThat(finalState.state(), is(RunState.State.FAILED));
  }

  @Test
  public void testRejectUnknownWorkflowInstance() throws Exception {
    Event injectedEvent = Event.timeout(WFI);
    ByteString eventPayload = eventSerializer.serialize(injectedEvent);
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/events", eventPayload);

    final Response<ByteString> response =
        post.toCompletableFuture().get();// block until done

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
    assertThat(response, hasStatus(withReasonPhrase(containsString("not found"))));
  }

  @Test
  public void testTriggeredWorkflowGeneratesTriggerId() throws Exception {
    storage.store(HOURLY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2014-12-31T23");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));
    assertThat(triggerId, hasValue(startsWith("ad-hoc-cli-")));
  }

  @Test
  public void testTriggerWorkflowInstanceHourly() throws Exception {
    storage.store(HOURLY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2014-12-31T23");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));
    assertThat(triggeredInstant, hasValue(Instant.parse("2014-12-31T23:00:00.000Z")));
    assertThat(triggeredWorkflow, hasValue(HOURLY_WORKFLOW));
  }

  @Test
  public void testTriggerWorkflowInstanceDaily() throws Exception {
    storage.store(DAILY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(DAILY_WORKFLOW.id(), "2014-12-31");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(), is(Status.OK));
    assertThat(triggeredInstant, hasValue(Instant.parse("2014-12-31T00:00:00.000Z")));
    assertThat(triggeredWorkflow, hasValue(DAILY_WORKFLOW));
  }

  @Test
  public void testTriggerWorkflowInstanceWeekly() throws Exception {
    storage.store(WEEKLY_WORKFLOW);
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

    ServiceHelper serviceHelper = ServiceHelper.create((environment) -> {
      final SchedulerResource schedulerResource = new SchedulerResource(
          stateManager,
          (workflow, triggerId, instant) -> {
            this.triggeredWorkflow = Optional.of(workflow);
            this.triggerId = Optional.of(triggerId);
            this.triggeredInstant = Optional.of(instant);
          },
          failingStorage,
          () -> Instant.parse("2015-12-31T23:59:10.000Z"));

      environment.routingEngine()
          .registerRoutes(schedulerResource.routes());
    }, "styx");
    serviceHelper.start();

    WorkflowInstance toTrigger = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-12-31T23");
    ByteString eventPayload = ByteString.of(MAPPER.writeValueAsBytes(toTrigger));
    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", SchedulerResource.BASE + "/trigger", eventPayload);
    Response<ByteString> response = post.toCompletableFuture().get();

    assertThat(response.status(),
               is(Status.INTERNAL_SERVER_ERROR.withReasonPhrase("An error occurred while retrieving "
                                                                + "workflow specifications")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }

  @Test
  public void testTriggerWorkflowInstanceUnsupportedPartitioning() throws Exception {
    storage.store(MONTHLY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(MONTHLY_WORKFLOW.id(), "2014-12");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("Partitioning not supported: MONTHS")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }

  @Test
  public void testTriggerWorkflowInstanceFuture() throws Exception {
    storage.store(HOURLY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-12-31T23");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("Cannot trigger an instance of the future")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }

  @Test
  public void testTriggerWorkflowInstanceParseDayforHourly() throws Exception {
    storage.store(HOURLY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2015-12-31");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("Cannot parse time parameter. "
                                                      + "Expected partitioning is HOURS: yyyy-MM-dd'T'HH")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }

  @Test
  public void testTriggerWorkflowInstanceParseHourforDaily() throws Exception {
    storage.store(DAILY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(DAILY_WORKFLOW.id(), "2015-12-31T00");

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("Cannot parse time parameter. "
                                                      + "Expected partitioning is DAYS: yyyy-MM-dd")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }

  @Test
  public void testTriggerAlreadyActiveWorkflowInstance() throws Exception {
    storage.store(DAILY_WORKFLOW);
    WorkflowInstance toTrigger = WorkflowInstance.create(DAILY_WORKFLOW.id(), "2015-12-31");
    stateManager.initialize(RunState.fresh(toTrigger));

    Response<ByteString> response = requestAndWaitTriggerWorkflowInstance(toTrigger);

    assertThat(response.status(),
               is(Status.BAD_REQUEST.withReasonPhrase("The specified instance is already "
                                                      + "active in the scheduler")));
    assertThat(triggeredWorkflow, isEmpty());
    assertThat(triggeredInstant, isEmpty());
  }
}
