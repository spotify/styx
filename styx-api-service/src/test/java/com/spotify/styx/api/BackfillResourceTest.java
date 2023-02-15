/*-
 * -\-\-
 * Spotify Styx API Service
 * --
 * Copyright (C) 2017 Spotify AB
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

import static com.spotify.apollo.Status.FORBIDDEN;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.belongsToFamily;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withCode;
import static com.spotify.styx.api.JsonMatchers.assertJson;
import static com.spotify.styx.api.JsonMatchers.assertJsonNullValue;
import static com.spotify.styx.api.JsonMatchers.assertNoJson;
import static com.spotify.styx.testdata.TestData.EXECUTION_DESCRIPTION;
import static com.spotify.styx.testdata.TestData.FLYTE_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.RESOURCE_IDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.spotify.apollo.Environment;
import com.spotify.apollo.Request;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.StatusType;
import com.spotify.apollo.test.StubClient;
import com.spotify.apollo.test.response.ResponseWithDelay;
import com.spotify.apollo.test.response.Responses;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.EditableBackfillInput;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.BigtableMocker;
import com.spotify.styx.storage.BigtableStorage;
import com.spotify.styx.storage.DatastoreEmulator;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.WorkflowValidator;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import okio.ByteString;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class BackfillResourceTest extends VersionedApiTest {

  @ClassRule public static final DatastoreEmulator datastoreEmulator = new DatastoreEmulator();

  private static final TriggerParameters TRIGGER_PARAMETERS = TriggerParameters.builder()
      .env("FOO", "foo",
          "BAR", "bar")
      .build();

  private static final String SCHEDULER_BASE = "http://localhost:12345";

  private static final String SERVICE_ACCOUNT = "foo@bar.iam.gserviceaccount.com";

  private static final WorkflowId WORKFLOW_ID_1 = WorkflowId.create("component", "workflow1");
  private static final WorkflowId WORKFLOW_ID_2 = WorkflowId.create("component", "workflow2");

  private static final Instant INSTANT = Instant.parse("2018-10-17T00:00:00.000Z");

  private static final Backfill BACKFILL_1 = Backfill.newBuilder()
      .id("backfill-1")
      .start(Instant.parse("2017-01-01T00:00:00Z"))
      .end(Instant.parse("2017-01-02T00:00:00Z"))
      .workflowId(WORKFLOW_ID_1)
      .concurrency(1)
      .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
      .schedule(Schedule.HOURS)
      .created(INSTANT)
      .lastModified(INSTANT)
      .build();

  private static final Backfill BACKFILL_2 = Backfill.newBuilder()
      .id("backfill-2")
      .start(Instant.parse("2017-01-01T00:00:00Z"))
      .end(Instant.parse("2017-01-02T00:00:00Z"))
      .reverse(true)
      .workflowId(WORKFLOW_ID_1)
      .concurrency(1)
      .nextTrigger(Instant.parse("2017-01-01T23:00:00Z"))
      .schedule(Schedule.HOURS)
      .build();

  private static final Backfill BACKFILL_3 = Backfill.newBuilder()
      .id("backfill-3")
      .start(Instant.parse("2017-01-01T00:00:00Z"))
      .end(Instant.parse("2017-01-02T00:00:00Z"))
      .workflowId(WORKFLOW_ID_2)
      .concurrency(2)
      .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
      .schedule(Schedule.HOURS)
      .build();

  private static final Backfill BACKFILL_4 = Backfill.newBuilder()
      .id("backfill-4")
      .start(Instant.parse("2017-01-01T00:00:00Z"))
      .end(Instant.parse("2017-01-02T00:00:00Z"))
      .workflowId(WorkflowId.create("other_component", "other_workflow"))
      .concurrency(2)
      .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
      .schedule(Schedule.HOURS)
      .build();

  private static final Backfill BACKFILL_5 = Backfill.newBuilder()
      .id("backfill-5")
      .start(Instant.parse("2017-01-01T00:00:00Z"))
      .end(Instant.parse("2017-01-01T06:00:00Z"))
      .reverse(true)
      .workflowId(WorkflowId.create("component", "workflow1"))
      .concurrency(1)
      .nextTrigger(Instant.parse("2017-01-01T05:00:00Z"))
      .schedule(Schedule.HOURS)
      .build();

  private static final Backfill BACKFILL_6 = Backfill.newBuilder()
      .id("backfill-6")
      .start(Instant.parse("2021-01-01T00:00:00Z"))
      .end(Instant.parse("2021-01-01T06:00:00Z"))
      .reverse(true)
      .workflowId(WorkflowId.create("other_component", "other_workflow"))
      .concurrency(1)
      .nextTrigger(Instant.parse("2021-01-01T05:00:00Z"))
      .schedule(Schedule.HOURS)
      .build();

  private Connection bigtable = setupBigTableMockTable();

  private Storage storage;

  @Mock private WorkflowActionAuthorizer workflowActionAuthorizer;
  @Mock private GoogleIdToken idToken;
  @Mock private RequestAuthenticator requestAuthenticator;

  private final WorkflowValidator workflowValidator = mock(WorkflowValidator.class);

  private Instant currentTime = INSTANT;

  public BackfillResourceTest(Api.Version version) {
    super(BackfillResource.BASE, version, "backfill-test", spy(StubClient.class));
    MockitoAnnotations.initMocks(this);
  }

  @Override
  protected void init(Environment environment) {
    storage = spy(new AggregateStorage(bigtable, datastoreEmulator.client()));

    when(requestAuthenticator.authenticate(any())).thenReturn(() -> Optional.of(idToken));
    final BackfillResource backfillResource = closer.register(
        new BackfillResource(SCHEDULER_BASE, storage, workflowValidator,
            () -> this.currentTime, workflowActionAuthorizer));
    environment.routingEngine()
        .registerRoutes(backfillResource.routes(requestAuthenticator).map(r ->
            r.withMiddleware(Middlewares.exceptionAndRequestIdHandler())));
  }

  @Before
  public void setUp() throws Exception {
    when(workflowValidator.validateWorkflow(any())).thenReturn(Collections.emptyList());
    storage.storeWorkflow(Workflow.create(
        BACKFILL_1.workflowId().componentId(),
        WorkflowConfiguration.builder()
            .id(BACKFILL_1.workflowId().id())
            .schedule(Schedule.HOURS)
            .dockerImage("foobar")
            .serviceAccount(SERVICE_ACCOUNT)
            .build()));
    storage.storeWorkflow(Workflow.create(
        BACKFILL_3.workflowId().componentId(),
        WorkflowConfiguration.builder()
            .id(BACKFILL_3.workflowId().id())
            .schedule(Schedule.HOURS)
            .dockerImage("foobar")
            .serviceAccount(SERVICE_ACCOUNT)
            .build()));
    storage.storeWorkflow(Workflow.create(
        BACKFILL_4.workflowId().componentId(),
        WorkflowConfiguration.builder()
            .id(BACKFILL_4.workflowId().id())
            .schedule(Schedule.HOURS)
            .serviceAccount(SERVICE_ACCOUNT)
            .build()));
    storage.storeBackfill(BACKFILL_1);
  }

  @After
  public void tearDown() {
    datastoreEmulator.reset();
  }

  @Test
  public void shouldListBackfillsNoStatus() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills[0].backfill.id", equalTo(BACKFILL_1.id()));
    assertNoJson(response, "backfills[0].statuses.active_states");
  }

  @Test
  public void shouldListBackfillsWithStatus() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("?status=true")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills[0].backfill.id", equalTo(BACKFILL_1.id()));
    assertJson(response, "backfills[0].statuses.active_states", hasSize(24));
  }

  @Test
  public void shouldFilterBackfillsOnComponentEvenWhenInactive() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_4.builder().allTriggered(true).build());

    final String uri = path(String.format("?showAll=true&component=%s",
                                          BACKFILL_4.workflowId().componentId()));
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", uri));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(1));
  }

  @Test
  public void shouldFilterBackfillsOnComponent() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_4);

    final String uri = path(String.format("?component=%s",
                                          BACKFILL_1.workflowId().componentId()));
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", uri));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(1));
  }

  @Test
  public void shouldFilterBackfillsOnWorkflow() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_3);
    storage.storeBackfill(BACKFILL_4);

    final String uri = path(String.format("?workflow=%s",
                                          BACKFILL_1.workflowId().id()));
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", uri));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(1));
    assertJson(response, "backfills[0].backfill.id", equalTo(BACKFILL_1.id()));
  }

  @Test
  public void shouldFilterBackfillsOnComponentWorkflow() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_3);
    storage.storeBackfill(BACKFILL_4);

    final String uri = path(String.format("?component=%s&workflow=%s",
                                          BACKFILL_1.workflowId().componentId(),
                                          BACKFILL_1.workflowId().id()));
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", uri));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(1));
    assertJson(response, "backfills[0].backfill.id", equalTo(BACKFILL_1.id()));
  }

  @Test
  public void shouldListActiveBackfillsByDefault() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_3.builder().allTriggered(true).build());
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(1));
  }

  @Test
  public void shouldListAllBackfillsWithFlag() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_3.builder().halted(true).build());
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("?showAll=true")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(2));
  }
   @Test
  public void shouldListMultipleBackfills() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_3);
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(2));
  }

  @Test
  public void shouldGetBackfillStatusWithStatusByDefault() throws Exception {
    sinceVersion(Api.Version.V3);

    WorkflowInstance wfi = WorkflowInstance.create(BACKFILL_1.workflowId(), "2017-01-01T01");
    storage.storeBackfill(BACKFILL_1.builder().nextTrigger(Instant.parse("2017-01-01T02:00:00Z")).build());
    storage.writeEvent(SequenceEvent.create(
        Event.triggerExecution(wfi, Trigger.backfill("backfill-1"), TRIGGER_PARAMETERS),        1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi, RESOURCE_IDS),                   2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi, EXECUTION_DESCRIPTION, "exec-1"), 3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi, "exec-1", "test"),             4L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi),                                 5L, 5L));
    storage.writeActiveState(wfi, RunState.create(wfi, State.RUNNING,
        StateData.newBuilder()
            .trigger(Trigger.backfill(BACKFILL_1.id()))
            .executionId("exec-1")
            .executionDescription(EXECUTION_DESCRIPTION)
            .tries(0)
            .resourceIds(RESOURCE_IDS)
            .build(), Instant.now(), 5L));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/" + BACKFILL_1.id())));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfill.id", equalTo(BACKFILL_1.id()));
    assertJson(response, "statuses.active_states[0].state", equalTo("UNKNOWN"));
    assertJson(response, "statuses.active_states[1].state", equalTo("RUNNING"));
    assertJson(response, "statuses.active_states[2].state", equalTo("WAITING"));
    assertJson(response, "statuses.active_states[23].state", equalTo("WAITING"));
    assertJson(response, "statuses.active_states", hasSize(24));

    verify(storage, never()).readEvents(wfi);
  }

  @Test
  public void shouldGetBackfillStatusWithStatusByDefaultReversed() throws Exception {
    sinceVersion(Api.Version.V3);

    WorkflowInstance wfi = WorkflowInstance.create(BACKFILL_2.workflowId(), "2017-01-01T22");
    storage.storeBackfill(BACKFILL_2.builder().nextTrigger(Instant.parse("2017-01-01T21:00:00Z")).build());
    storage.writeEvent(SequenceEvent.create(
        Event.triggerExecution(wfi, Trigger.backfill("backfill-2"), TRIGGER_PARAMETERS),        1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi, RESOURCE_IDS),                   2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi, EXECUTION_DESCRIPTION, "exec-1"), 3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi),                                 5L, 5L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi, "exec-1", "test"),             4L, 4L));
    storage.writeActiveState(wfi, RunState.create(wfi, State.RUNNING,
        StateData.newBuilder()
            .trigger(Trigger.backfill(BACKFILL_2.id()))
            .executionId("exec-1")
            .executionDescription(EXECUTION_DESCRIPTION)
            .tries(0)
            .resourceIds(RESOURCE_IDS)
            .build(), Instant.now(), 5L));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/" + BACKFILL_2.id())));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfill.id", equalTo(BACKFILL_2.id()));
    assertJson(response, "statuses.active_states[0].state", equalTo("WAITING"));
    assertJson(response, "statuses.active_states[21].state", equalTo("WAITING"));
    assertJson(response, "statuses.active_states[22].state", equalTo("RUNNING"));
    assertJson(response, "statuses.active_states[23].state", equalTo("UNKNOWN"));
    assertJson(response, "statuses.active_states", hasSize(24));

    verify(storage, never()).readEvents(wfi);
  }

  @Test
  public void shouldReplayIfNotMatchingTriggerId() throws Exception {
    sinceVersion(Api.Version.V3);

    WorkflowInstance wfi = WorkflowInstance.create(BACKFILL_2.workflowId(), "2017-01-01T22");
    storage.storeBackfill(BACKFILL_2.builder().nextTrigger(Instant.parse("2017-01-01T21:00:00Z")).build());
    storage.writeEvent(SequenceEvent.create(
        Event.triggerExecution(wfi, Trigger.backfill("backfill-2"), TRIGGER_PARAMETERS),        1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi, RESOURCE_IDS),                   2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi, EXECUTION_DESCRIPTION, "exec-1"), 3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi, "exec-1", "test"),             4L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi),                                 5L, 5L));
    storage.writeActiveState(wfi, RunState.create(wfi, State.RUNNING,
        StateData.newBuilder()
            .trigger(Trigger.backfill(BACKFILL_1.id())) // BACKFILL_1 has active state, not BACKFILL_2
            .executionId("exec-1")
            .executionDescription(EXECUTION_DESCRIPTION)
            .tries(0)
            .resourceIds(RESOURCE_IDS)
            .build(), Instant.now(), 5L));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/" + BACKFILL_2.id())));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfill.id", equalTo(BACKFILL_2.id()));
    assertJson(response, "statuses.active_states[0].state", equalTo("WAITING"));
    assertJson(response, "statuses.active_states[21].state", equalTo("WAITING"));
    assertJson(response, "statuses.active_states[22].state", equalTo("RUNNING"));
    assertJson(response, "statuses.active_states[23].state", equalTo("UNKNOWN"));
    assertJson(response, "statuses.active_states", hasSize(24));

    // verify we replay for instance belonging to BACKFILL_2
    verify(storage).readEvents(wfi);
  }

  @Test
  public void shouldGetFinishedBackfillReversed() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_5.builder().nextTrigger(Instant.parse("2016-12-31T23:00:00Z")).allTriggered(true).build());

    storeSucessfulInstance(WorkflowInstance.create(BACKFILL_5.workflowId(), "2017-01-01T05"), BACKFILL_5.id());
    storeSucessfulInstance(WorkflowInstance.create(BACKFILL_5.workflowId(), "2017-01-01T04"), BACKFILL_5.id());
    storeSucessfulInstance(WorkflowInstance.create(BACKFILL_5.workflowId(), "2017-01-01T03"), BACKFILL_5.id());
    storeSucessfulInstance(WorkflowInstance.create(BACKFILL_5.workflowId(), "2017-01-01T02"), BACKFILL_5.id());
    storeSucessfulInstance(WorkflowInstance.create(BACKFILL_5.workflowId(), "2017-01-01T01"), BACKFILL_5.id());
    storeSucessfulInstance(WorkflowInstance.create(BACKFILL_5.workflowId(), "2017-01-01T00"), BACKFILL_5.id());

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/" + BACKFILL_5.id())));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfill.id", equalTo(BACKFILL_5.id()));
    assertJson(response, "statuses.active_states[0].state", equalTo("DONE"));
    assertJson(response, "statuses.active_states[0].initial_timestamp", is(1));
    assertJson(response, "statuses.active_states[0].latest_timestamp", is(7));
    assertJson(response, "statuses.active_states[1].state", equalTo("DONE"));
    assertJson(response, "statuses.active_states[1].initial_timestamp", is(1));
    assertJson(response, "statuses.active_states[1].latest_timestamp", is(7));
    assertJson(response, "statuses.active_states[2].state", equalTo("DONE"));
    assertJson(response, "statuses.active_states[2].initial_timestamp", is(1));
    assertJson(response, "statuses.active_states[2].latest_timestamp", is(7));
    assertJson(response, "statuses.active_states[3].state", equalTo("DONE"));
    assertJson(response, "statuses.active_states[3].initial_timestamp", is(1));
    assertJson(response, "statuses.active_states[3].latest_timestamp", is(7));
    assertJson(response, "statuses.active_states[4].state", equalTo("DONE"));
    assertJson(response, "statuses.active_states[4].initial_timestamp", is(1));
    assertJson(response, "statuses.active_states[4].latest_timestamp", is(7));
    assertJson(response, "statuses.active_states[5].state", equalTo("DONE"));
    assertJson(response, "statuses.active_states[5].initial_timestamp", is(1));
    assertJson(response, "statuses.active_states[5].latest_timestamp", is(7));
    assertJson(response, "statuses.active_states", hasSize(6));
  }

  private void storeSucessfulInstance(WorkflowInstance wfi, String backfillId) throws IOException {
    storage.writeEvent(SequenceEvent.create(
        Event.triggerExecution(wfi, Trigger.backfill(backfillId), TRIGGER_PARAMETERS),          1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi, RESOURCE_IDS),                   2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi, EXECUTION_DESCRIPTION, "exec-1"), 3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi, "exec-1", "test"),             4L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi),                                 5L, 5L));
    storage.writeEvent(SequenceEvent.create(Event.terminate(wfi, Optional.of(0)),               6L, 6L));
    storage.writeEvent(SequenceEvent.create(Event.success(wfi),                                 7L, 7L));
  }

  @Test
  public void shouldGetBackfillWithoutStatus() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_1.builder().nextTrigger(Instant.parse("2017-01-01T02:00:00Z")).build());

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/" + BACKFILL_1.id() + "?status=false")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfill.id", equalTo(BACKFILL_1.id()));
    assertJsonNullValue(response, "statuses");
  }

  @Test
  public void shouldHandleGetBackfillFailure() throws Exception {
    sinceVersion(Api.Version.V3);
    doThrow(new IOException("error!")).when(storage).backfill(BACKFILL_1.id());
    final Response<ByteString> response = awaitResponse(serviceHelper.request("GET", path("/" + BACKFILL_1.id())));
    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SERVER_ERROR)));
  }

  @Test
  public void shouldHandleListBackfillsFailure() throws Exception {
    sinceVersion(Api.Version.V3);
    doThrow(new IOException("error!")).when(storage).backfills(anyBoolean());
    final Response<ByteString> response = awaitResponse(serviceHelper.request("GET", path("")));
    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SERVER_ERROR)));
  }

  @Test
  public void shouldReturnNotFoundForMissingBackfill() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/missing-id?status=false")));

    assertThat(response.status(), is(Status.NOT_FOUND));
  }

  @Test
  public void shouldPostBackfill() throws Exception {
    sinceVersion(Api.Version.V3);

    final String json = "{\"start\":\"2017-01-01T00:00:00Z\"," +
                        "\"end\":\"2017-02-01T00:00:00Z\"," +
                        "\"component\":\"component\"," +
                        "\"workflow\":\"workflow2\","+
                        "\"concurrency\":1}";

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""), ByteString.encodeUtf8(json)));

    assertThat(response.status().reasonPhrase(),
               response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    Backfill postedBackfill = Json.OBJECT_MAPPER.readValue(
        response.payload().orElseThrow().toByteArray(), Backfill.class);
    assertThat(postedBackfill.id().matches("backfill-[\\d-]+"), is(true));
    assertThat(postedBackfill.start(), equalTo(Instant.parse("2017-01-01T00:00:00Z")));
    assertThat(postedBackfill.end(), equalTo(Instant.parse("2017-02-01T00:00:00Z")));
    assertThat(postedBackfill.workflowId(), equalTo(WorkflowId.create("component", "workflow2")));
    assertThat(postedBackfill.concurrency(), equalTo(1));
    assertThat(postedBackfill.description(), equalTo(Optional.empty()));
    assertThat(postedBackfill.nextTrigger(), equalTo(Instant.parse("2017-01-01T00:00:00Z")));
    assertThat(postedBackfill.schedule(), equalTo(Schedule.HOURS));
    assertThat(postedBackfill.allTriggered(), equalTo(false));
    assertThat(postedBackfill.halted(), equalTo(false));
    assertThat(postedBackfill.reverse(), equalTo(false));
  }

  @Test
  public void shouldFailPostBackfillIfNotAuthorized() throws Exception {
    sinceVersion(Api.Version.V3);

    final int backfillsBefore = storage.backfillsForWorkflowId(true, WORKFLOW_ID_2, Optional.empty()).size();

    final BackfillInput input = BackfillInput.newBuilder()
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-02-01T00:00:00Z"))
        .component(WORKFLOW_ID_2.componentId())
        .workflow(WORKFLOW_ID_2.id())
        .concurrency(1)
        .description("")
        .build();

    doThrow(new ResponseException(Response.forStatus(FORBIDDEN)))
        .when(workflowActionAuthorizer).authorizeWorkflowAction(any(), any(Workflow.class));

    reset(storage);

    final Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""), Json.serialize(input)));

    assertThat(response, hasStatus(withCode(FORBIDDEN)));

    final int backfillsAfter = storage.backfillsForWorkflowId(true, WORKFLOW_ID_2, Optional.empty()).size();
    assertThat(backfillsBefore, is(backfillsAfter));
  }

  @Test
  public void shouldBackfillForFlyteWorkflow() throws Exception {
    reset(storage);

    storage.storeWorkflow(Workflow.create(
        "flytewf",
        FLYTE_WORKFLOW_CONFIGURATION));

    final String json = "{\"start\":\"2017-01-01T00:00:00Z\"," +
                        "\"end\":\"2017-02-01T00:00:00Z\"," +
                        "\"component\":\"flytewf\"," +
                        "\"workflow\":\"" + FLYTE_WORKFLOW_CONFIGURATION.id() + "\","+
                        "\"concurrency\":1}";

    final Response<ByteString> response = awaitResponse(
        serviceHelper.request("POST", path(""), ByteString.encodeUtf8(json)));

    assertThat(response.status(), is(Status.OK));

    verify(storage, never()).storeBackfill(any());
  }

  @Test
  public void shouldFailHaltBackfillIfNotAuthorized() throws Exception {
    sinceVersion(Api.Version.V3);

    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.ACCEPTED))
        .to(SCHEDULER_BASE + "/api/v0/events");

    storage.storeBackfill(BACKFILL_1);

    doThrow(new ResponseException(Response.forStatus(FORBIDDEN)))
        .when(workflowActionAuthorizer).authorizeWorkflowAction(any(), any(WorkflowId.class));

    reset(storage);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("DELETE", path("/" + BACKFILL_1.id())));

    assertThat(response, hasStatus(withCode(FORBIDDEN)));

    verify(storage, never()).storeBackfill(any());
  }

  @Test
  public void shouldFailUpdateBackfillIfNotAuthorized() throws Exception {
    sinceVersion(Api.Version.V3);

    final EditableBackfillInput backfillInput = EditableBackfillInput.newBuilder()
        .id(BACKFILL_1.id())
        .description("updated")
        .build();

    doThrow(new ResponseException(Response.forStatus(FORBIDDEN)))
        .when(workflowActionAuthorizer).authorizeWorkflowAction(any(), any(WorkflowId.class));

    reset(storage);

    final Response<ByteString> response =
        awaitResponse(serviceHelper.request("PUT", path("/" + BACKFILL_1.id()), Json.serialize(backfillInput)));

    assertThat(response, hasStatus(withCode(FORBIDDEN)));
    assertThat(storage.backfill(BACKFILL_1.id()).orElseThrow().description(), is(not(Optional.of("updated"))));
  }


  @Test
  public void shouldPostBackfillReversed() throws Exception {
    sinceVersion(Api.Version.V3);

    final String json = "{\"start\":\"2017-01-01T00:00:00Z\"," +
                        "\"end\":\"2017-02-01T00:00:00Z\"," +
                        "\"component\":\"component\"," +
                        "\"workflow\":\"workflow2\","+
                        "\"concurrency\":1," +
                        "\"reverse\":true}";

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""), ByteString.encodeUtf8(json)));

    assertThat(response.status().reasonPhrase(),
        response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    Backfill postedBackfill = Json.OBJECT_MAPPER.readValue(
        response.payload().orElseThrow().toByteArray(), Backfill.class);
    assertThat(postedBackfill.id().matches("backfill-[\\d-]+"), is(true));
    assertThat(postedBackfill.start(), equalTo(Instant.parse("2017-01-01T00:00:00Z")));
    assertThat(postedBackfill.end(), equalTo(Instant.parse("2017-02-01T00:00:00Z")));
    assertThat(postedBackfill.workflowId(), equalTo(WorkflowId.create("component", "workflow2")));
    assertThat(postedBackfill.concurrency(), equalTo(1));
    assertThat(postedBackfill.description(), equalTo(Optional.empty()));
    assertThat(postedBackfill.nextTrigger(), equalTo(Instant.parse("2017-01-31T23:00:00Z")));
    assertThat(postedBackfill.schedule(), equalTo(Schedule.HOURS));
    assertThat(postedBackfill.allTriggered(), equalTo(false));
    assertThat(postedBackfill.halted(), equalTo(false));
    assertThat(postedBackfill.reverse(), equalTo(true));
  }

  @Test
  public void shouldPostBackfillWithDescription() throws Exception {
    sinceVersion(Api.Version.V3);

    final String json = "{\"start\":\"2017-01-01T00:00:00Z\"," +
                        "\"end\":\"2017-02-01T00:00:00Z\"," +
                        "\"component\":\"component\"," +
                        "\"workflow\":\"workflow2\"," +
                        "\"concurrency\":1," +
                        "\"description\":\"Description\"}";

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""), ByteString.encodeUtf8(json)));

    assertThat(response.status().reasonPhrase(),
               response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    Backfill postedBackfill = Json.OBJECT_MAPPER.readValue(
        response.payload().orElseThrow().toByteArray(), Backfill.class);
    assertThat(postedBackfill.id().matches("backfill-[\\d-]+"), is(true));
    assertThat(postedBackfill.start(), equalTo(Instant.parse("2017-01-01T00:00:00Z")));
    assertThat(postedBackfill.end(), equalTo(Instant.parse("2017-02-01T00:00:00Z")));
    assertThat(postedBackfill.workflowId(), equalTo(WorkflowId.create("component", "workflow2")));
    assertThat(postedBackfill.concurrency(), equalTo(1));
    assertThat(postedBackfill.description(), equalTo(Optional.of("Description")));
    assertThat(postedBackfill.nextTrigger(), equalTo(Instant.parse("2017-01-01T00:00:00Z")));
    assertThat(postedBackfill.schedule(), equalTo(Schedule.HOURS));
    assertThat(postedBackfill.allTriggered(), equalTo(false));
    assertThat(postedBackfill.halted(), equalTo(false));
  }

  @Test
  public void shouldFailOnStartNotBeforeEnd() throws Exception {
    sinceVersion(Api.Version.V3);

    final String json = "{\"start\":\"2017-02-01T00:00:00Z\"," +
                        "\"end\":\"2017-01-01T00:00:00Z\"," +
                        "\"component\":\"component\"," +
                        "\"workflow\":\"workflow2\","+
                        "\"concurrency\":1}";

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""), ByteString.encodeUtf8(json)));

    assertThat(response.status().reasonPhrase(),
        response, hasStatus(belongsToFamily(StatusType.Family.CLIENT_ERROR)));
  }

  @Test
  public void shouldFailOnMisalignedStart() throws Exception {
    sinceVersion(Api.Version.V3);

    final String json = "{\"start\":\"2017-01-01T00:00:01Z\"," +
                        "\"end\":\"2017-02-01T00:00:00Z\"," +
                        "\"component\":\"component\"," +
                        "\"workflow\":\"workflow2\","+
                        "\"concurrency\":1}";

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""), ByteString.encodeUtf8(json)));

    assertThat(response.status().reasonPhrase(),
               response, hasStatus(belongsToFamily(StatusType.Family.CLIENT_ERROR)));
  }

  @Test
  public void shouldFailOnMisalignedEnd() throws Exception {
    sinceVersion(Api.Version.V3);

    final String json = "{\"start\":\"2017-01-01T00:00:00Z\"," +
                        "\"end\":\"2017-02-01T00:00:01Z\"," +
                        "\"component\":\"component\"," +
                        "\"workflow\":\"workflow2\","+
                        "\"concurrency\":1}";

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""), ByteString.encodeUtf8(json)));

    assertThat(response.status().reasonPhrase(),
        response, hasStatus(belongsToFamily(StatusType.Family.CLIENT_ERROR)));
  }

  @Test
  public void shouldFailOnAlreadyActiveWithinRange() throws Exception {
    sinceVersion(Api.Version.V3);

    final BackfillInput backfillInput = BackfillInput.newBuilder()
        .start(BACKFILL_1.start())
        .end(BACKFILL_1.end())
        .component(BACKFILL_1.workflowId().componentId())
        .workflow(BACKFILL_1.workflowId().id())
        .concurrency(BACKFILL_1.concurrency())
        .description(BACKFILL_1.description())
        .build();

    WorkflowInstance wfi = WorkflowInstance.create(BACKFILL_1.workflowId(),"2017-01-01T01");
    storage.writeActiveState(wfi, RunState.create(wfi, State.RUNNING,
        StateData.zero(), Instant.now(), 0L));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""), Json.serialize(backfillInput)));

    assertThat(response.status().reasonPhrase(),
               response, hasStatus(belongsToFamily(StatusType.Family.CLIENT_ERROR)));
  }

  @Test
  public void shouldUpdateBackfill() throws Exception {
    sinceVersion(Api.Version.V3);

    assertThat(storage.backfill(BACKFILL_1.id()).orElseThrow().concurrency(), equalTo(1));

    final EditableBackfillInput backfillInput = EditableBackfillInput.newBuilder()
        .id(BACKFILL_1.id())
        .concurrency(4)
        .description("foobar")
        .build();
    final String json = Json.OBJECT_MAPPER.writeValueAsString(backfillInput);

    final Response<ByteString> response =
        awaitResponse(serviceHelper.request("PUT", path("/" + BACKFILL_1.id()),
            ByteString.encodeUtf8(json)));

    assertThat(response.status().reasonPhrase(),
               response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "id", equalTo(BACKFILL_1.id()));
    assertJson(response, "concurrency", equalTo(4));
    assertJson(response, "description", is("foobar"));

    assertThat(storage.backfill(BACKFILL_1.id()).orElseThrow(), is(BACKFILL_1.builder()
        .concurrency(4)
        .description("foobar")
        .build()));
  }

  @Test
  public void shouldFailToUpdateNonexistentBackfill() throws Exception {
    sinceVersion(Api.Version.V3);

    assertThat(storage.backfill("foo"), is(Optional.empty()));

    final EditableBackfillInput backfillInput = EditableBackfillInput.newBuilder()
        .id("foo")
        .concurrency(4)
        .build();
    final String json = Json.OBJECT_MAPPER.writeValueAsString(backfillInput);

    final Response<ByteString> response =
        awaitResponse(serviceHelper.request("PUT", path("/foo"),
            ByteString.encodeUtf8(json)));

    assertThat(response.status(), is(Status.NOT_FOUND.withReasonPhrase("Backfill foo not found.")));

    assertThat(storage.backfill("foo"), is(Optional.empty()));
  }

  @Test
  public void shouldFailToUpdateBackfillDueToMismatchId() throws Exception {
    sinceVersion(Api.Version.V3);

    final EditableBackfillInput backfillInput = EditableBackfillInput.newBuilder()
        .id("bar")
        .concurrency(4)
        .build();
    final String json = Json.OBJECT_MAPPER.writeValueAsString(backfillInput);

    final Response<ByteString> response =
        awaitResponse(serviceHelper.request("PUT", path("/foo"),
            ByteString.encodeUtf8(json)));

    assertThat(response.status(), is(Status.BAD_REQUEST
        .withReasonPhrase("ID of payload does not match ID in uri.")));
  }

  @Test
  public void shouldHaltBackfillReversed() throws Exception {
    sinceVersion(Api.Version.V3);

    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.OK))
        .to(SCHEDULER_BASE + "/api/v0/halt");

    WorkflowInstance wfi = WorkflowInstance.create(BACKFILL_5.workflowId(), "2017-01-01T05");
    WorkflowInstance wfi1 = WorkflowInstance.create(BACKFILL_5.workflowId(), "2017-01-01T04");
    WorkflowInstance wfi2 = WorkflowInstance.create(BACKFILL_5.workflowId(), "2017-01-01T03");
    storage.storeBackfill(BACKFILL_5.builder().nextTrigger(Instant.parse("2017-01-01T02:00:00Z")).build());
    storeRunningWorkflowInstance(wfi, BACKFILL_5.id());
    storeRunningWorkflowInstance(wfi1, BACKFILL_5.id());
    storeRunningWorkflowInstance(wfi2, BACKFILL_5.id());

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("DELETE", path("/" + BACKFILL_5.id())));
    assertThat(response.status().reasonPhrase(),
        response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));

    assertThat(storage.backfill(BACKFILL_5.id()).orElseThrow().halted(), equalTo(true));
    verify(serviceHelper.stubClient(), times(3)).send(any());
  }

  @Test
  public void shouldHaltBackfillAndUpdateLastModified() throws Exception {
    sinceVersion(Api.Version.V3);


    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.OK))
        .to(SCHEDULER_BASE + "/api/v0/halt");

    WorkflowInstance wfi = WorkflowInstance.create(BACKFILL_1.workflowId(), "2017-01-01T01");
    storage.storeBackfill(BACKFILL_1.builder().nextTrigger(Instant.parse("2017-01-01T02:00:00Z")).build());
    storeRunningWorkflowInstance(wfi, BACKFILL_1.id());

    var previousTime = this.currentTime;
    var updateTime = Instant.parse("2019-10-16T00:00:00Z");
    this.currentTime = updateTime;

    final Response<ByteString> response;
    try {
      response = awaitResponse(serviceHelper.request("DELETE", path("/" + BACKFILL_1.id())));
    } finally {
      this.currentTime = previousTime;
    }

    assertThat(response.status().reasonPhrase(),
        response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));

    assertThat(storage.backfill(BACKFILL_1.id()).orElseThrow().halted(), equalTo(true));
    assertThat(storage.backfill(BACKFILL_1.id()).orElseThrow().lastModified().orElseThrow(), equalTo(updateTime));
    verify(serviceHelper.stubClient(), times(1)).send(any());
  }

  private void storeRunningWorkflowInstance(WorkflowInstance wfi, String backfillId) throws IOException {
    storage.writeEvent(SequenceEvent.create(
        Event.triggerExecution(wfi, Trigger.backfill(backfillId), TRIGGER_PARAMETERS),          1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi, RESOURCE_IDS),                   2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi, EXECUTION_DESCRIPTION, "exec-1"), 3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi, "exec-1", "test"),             4L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi),                                 5L, 5L));
    storage.writeActiveState(wfi, RunState.create(wfi, State.RUNNING, StateData.zero(), Instant.now(), 5L));
  }

  @Test
  public void shouldNotHaltWaitingInstanceInBackfill() throws Exception {
    sinceVersion(Api.Version.V3);

    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.OK))
        .to(SCHEDULER_BASE + "/api/v0/halt");

    WorkflowInstance wfi1 = WorkflowInstance.create(BACKFILL_1.workflowId(), "2017-01-01T01");
    WorkflowInstance wfi2 = WorkflowInstance.create(BACKFILL_1.workflowId(), "2017-01-01T02");
    storage.storeBackfill(BACKFILL_1.builder().nextTrigger(Instant.parse("2017-01-01T03:00:00Z")).build());
    storage.writeEvent(SequenceEvent.create(
        Event.triggerExecution(wfi1, Trigger.backfill("backfill-1"), TRIGGER_PARAMETERS),        1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi1, RESOURCE_IDS),                   2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi1, EXECUTION_DESCRIPTION, "exec-1"), 3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi1, "exec-1", "test"),             4L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi1),                                 5L, 5L));
    storage.writeActiveState(wfi1, RunState.create(wfi1, State.RUNNING,
        StateData.zero(), Instant.now(), 5L));
    storage.writeActiveState(wfi2, RunState.create(wfi2, State.RUNNING,
        StateData.zero(), Instant.now(), 5L));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("DELETE", path("/" + BACKFILL_1.id())));
    assertThat(response.status().reasonPhrase(),
               response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));

    assertThat(storage.backfill(BACKFILL_1.id()).orElseThrow().halted(), equalTo(true));
    verify(serviceHelper.stubClient(), times(1)).send(Request.forUri(SCHEDULER_BASE + "/api/v0/halt", "POST")
        .withPayload(Json.serialize(wfi1))
        .withService("backfill-test"));
  }

  @Test
  public void shouldNotHaltInstanceInBackfillIfGraceful() throws Exception {
    sinceVersion(Api.Version.V3);

    WorkflowInstance wfi1 = WorkflowInstance.create(BACKFILL_1.workflowId(), "2017-01-01T01");
    storage.storeBackfill(BACKFILL_1.builder().nextTrigger(Instant.parse("2017-01-01T03:00:00Z")).build());
    storage.writeEvent(SequenceEvent.create(
        Event.triggerExecution(wfi1, Trigger.backfill("backfill-1"), TRIGGER_PARAMETERS),        1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi1, RESOURCE_IDS),                   2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi1, EXECUTION_DESCRIPTION, "exec-1"), 3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi1, "exec-1", "test"),             4L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi1),                                 5L, 5L));
    storage.writeActiveState(wfi1, RunState.create(wfi1, State.RUNNING,
        StateData.zero(), Instant.now(), 5L));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("DELETE", path("/" + BACKFILL_1.id() + "?graceful=true")));
    assertThat(response.status().reasonPhrase(),
        response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));

    assertThat(storage.backfill(BACKFILL_1.id()).orElseThrow().halted(), equalTo(true));
    verify(serviceHelper.stubClient(), never()).send(any());
  }

  @Test
  public void shouldReturnServerErrorIfFailedToSend() throws Exception {
    sinceVersion(Api.Version.V3);

    serviceHelper.stubClient().respond(Responses.sequence(List.of(
        ResponseWithDelay.forResponse(Response.forStatus(Status.INTERNAL_SERVER_ERROR)),
        ResponseWithDelay.forResponse(Response.forStatus(Status.ACCEPTED))))
    ).to(SCHEDULER_BASE + "/api/v0/events");

    WorkflowInstance wfi1 = WorkflowInstance.create(BACKFILL_1.workflowId(), "2017-01-01T01");
    WorkflowInstance wfi2 = WorkflowInstance.create(BACKFILL_1.workflowId(), "2017-01-01T02");
    storage.storeBackfill(BACKFILL_1.builder().nextTrigger(Instant.parse("2017-01-01T03:00:00Z")).build());

    storage.writeEvent(SequenceEvent.create(
        Event.triggerExecution(wfi1, Trigger.backfill("backfill-1"), TRIGGER_PARAMETERS),        1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi1, RESOURCE_IDS),                   2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi1, EXECUTION_DESCRIPTION, "exec-1"), 3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi1, "exec-1", "test"),             4L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi1),                                 5L, 5L));

    storage.writeEvent(SequenceEvent.create(
        Event.triggerExecution(wfi2, Trigger.backfill("backfill-1"), TRIGGER_PARAMETERS),        1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi2, RESOURCE_IDS),                   2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi2, EXECUTION_DESCRIPTION, "exec-2"), 3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi2, "exec-2", "test"),             4L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi2),                                 5L, 5L));

    storage.writeActiveState(wfi1, RunState.create(wfi1, State.RUNNING,
        StateData.zero(), Instant.now(), 5L));
    storage.writeActiveState(wfi2, RunState.create(wfi2, State.RUNNING,
        StateData.zero(), Instant.now(), 5L));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("DELETE", path("/" + BACKFILL_1.id())));
    assertThat(response.status().reasonPhrase(),
               response, hasStatus(belongsToFamily(StatusType.Family.SERVER_ERROR)));

    assertThat(storage.backfill(BACKFILL_1.id()).orElseThrow().halted(), equalTo(true));
    verify(serviceHelper.stubClient(), times(2)).send(any());
  }

  @Test
  public void shouldOnlyUpdateBackfillIfSameId() throws Exception {
    sinceVersion(Api.Version.V3);

    final Backfill updatedBackfill = BACKFILL_1.builder().concurrency(4).build();
    final String json = Json.OBJECT_MAPPER.writeValueAsString(updatedBackfill);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PUT", path("/wrong-id"),
                                            ByteString.encodeUtf8(json)));

    assertThat(response.status().reasonPhrase(),
               response, hasStatus(belongsToFamily(StatusType.Family.CLIENT_ERROR)));
  }

  @Test
  public void shouldReturnBadRequestForPostBackfillIfInvalidWorkflowConfiguration() throws Exception {
    sinceVersion(Api.Version.V3);

    final String json = "{\"start\":\"2017-01-01T00:00:00Z\"," +
        "\"end\":\"2017-02-01T00:00:00Z\"," +
        "\"component\":\"component\"," +
        "\"workflow\":\"workflow2\","+
        "\"concurrency\":1}";

    when(workflowValidator.validateWorkflow(any())).thenReturn(List.of("bad", "f00d"));

    final Response<ByteString> response = awaitResponse(
        serviceHelper.request("POST", path(""), ByteString.encodeUtf8(json)));

    assertThat(response.status(),
        is(Status.BAD_REQUEST.withReasonPhrase("Invalid workflow configuration: bad, f00d")));
  }

  @Test
  public void shouldReturnBadRequestForPostBackfillIfMissingImage() throws Exception {
    sinceVersion(Api.Version.V3);

    final String json = "{\"start\":\"2017-01-01T00:00:00Z\"," +
        "\"end\":\"2017-02-01T00:00:00Z\"," +
        "\"component\":\"other_component\"," +
        "\"workflow\":\"other_workflow\","+
        "\"concurrency\":1}";

    final Response<ByteString> response = awaitResponse(
        serviceHelper.request("POST", path(""), ByteString.encodeUtf8(json)));

    assertThat(response.status(),
        is(Status.BAD_REQUEST
            .withReasonPhrase("Workflow is missing docker image and flyte execution config")));
  }

  @Test
  public void shouldReturnBadRequestForPostBackfillIfStartInFuture() throws Exception {
    sinceVersion(Api.Version.V3);

    final String json = "{\"start\":\"2018-10-18T00:00:00Z\"," +
                        "\"end\":\"2018-10-19T00:00:00Z\"," +
                        "\"component\":\"component\"," +
                        "\"workflow\":\"workflow2\","+
                        "\"concurrency\":1}";

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""), ByteString.encodeUtf8(json)));

    assertThat(response.status(),
        is(Status.BAD_REQUEST.withReasonPhrase("Cannot backfill future partitions")));
  }

  @Test
  public void shouldReturnBadRequestForPostBackfillIfEndInFuture() throws Exception {
    sinceVersion(Api.Version.V3);

    final String json = "{\"start\":\"2018-10-16T00:00:00Z\"," +
                        "\"end\":\"2018-10-17T02:00:00Z\"," +
                        "\"component\":\"component\"," +
                        "\"workflow\":\"workflow2\","+
                        "\"concurrency\":1}";

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""), ByteString.encodeUtf8(json)));

    assertThat(response.status(),
        is(Status.BAD_REQUEST.withReasonPhrase("Cannot backfill future partitions")));
  }

  @Test
  public void shouldAllowPostBackfillIfStartInFuture() throws Exception {
    sinceVersion(Api.Version.V3);

    final String json = "{\"start\":\"2018-10-18T00:00:00Z\"," +
                        "\"end\":\"2018-10-19T00:00:00Z\"," +
                        "\"component\":\"component\"," +
                        "\"workflow\":\"workflow2\","+
                        "\"concurrency\":1}";

    Response<ByteString> response = awaitResponse(
        serviceHelper.request("POST", path("") + "?allowFuture=true", ByteString.encodeUtf8(json)));

    assertThat(response.status().reasonPhrase(),
        response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    Backfill postedBackfill = Json.OBJECT_MAPPER.readValue(
        response.payload().orElseThrow().toByteArray(), Backfill.class);
    assertThat(postedBackfill.id().matches("backfill-[\\d-]+"), is(true));
    assertThat(postedBackfill.start(), equalTo(Instant.parse("2018-10-18T00:00:00Z")));
    assertThat(postedBackfill.end(), equalTo(Instant.parse("2018-10-19T00:00:00Z")));
    assertThat(postedBackfill.workflowId(), equalTo(WorkflowId.create("component", "workflow2")));
    assertThat(postedBackfill.concurrency(), equalTo(1));
    assertThat(postedBackfill.description(), equalTo(Optional.empty()));
    assertThat(postedBackfill.nextTrigger(), equalTo(Instant.parse("2018-10-18T00:00:00Z")));
    assertThat(postedBackfill.schedule(), equalTo(Schedule.HOURS));
    assertThat(postedBackfill.allTriggered(), equalTo(false));
    assertThat(postedBackfill.halted(), equalTo(false));
    assertThat(postedBackfill.reverse(), equalTo(false));
  }

  @Test
  public void shouldAllowPostBackfillIfEndInFuture() throws Exception {
    sinceVersion(Api.Version.V3);

    final String json = "{\"start\":\"2018-10-16T00:00:00Z\"," +
                        "\"end\":\"2018-10-17T02:00:00Z\"," +
                        "\"component\":\"component\"," +
                        "\"workflow\":\"workflow2\","+
                        "\"concurrency\":1}";

    Response<ByteString> response = awaitResponse(
        serviceHelper.request("POST", path("") + "?allowFuture=true", ByteString.encodeUtf8(json)));

    assertThat(response.status().reasonPhrase(),
        response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    Backfill postedBackfill = Json.OBJECT_MAPPER.readValue(
        response.payload().orElseThrow().toByteArray(), Backfill.class);
    assertThat(postedBackfill.id().matches("backfill-[\\d-]+"), is(true));
    assertThat(postedBackfill.start(), equalTo(Instant.parse("2018-10-16T00:00:00Z")));
    assertThat(postedBackfill.end(), equalTo(Instant.parse("2018-10-17T02:00:00Z")));
    assertThat(postedBackfill.workflowId(), equalTo(WorkflowId.create("component", "workflow2")));
    assertThat(postedBackfill.concurrency(), equalTo(1));
    assertThat(postedBackfill.description(), equalTo(Optional.empty()));
    assertThat(postedBackfill.nextTrigger(), equalTo(Instant.parse("2018-10-16T00:00:00Z")));
    assertThat(postedBackfill.schedule(), equalTo(Schedule.HOURS));
    assertThat(postedBackfill.allTriggered(), equalTo(false));
    assertThat(postedBackfill.halted(), equalTo(false));
    assertThat(postedBackfill.reverse(), equalTo(false));
  }

  @Test
  public void shouldPostBackfillGenerateCreatedTimestamp() throws Exception {
    sinceVersion(Api.Version.V3);

    final String json = "{\"start\":\"2018-10-16T00:00:00Z\"," +
                        "\"end\":\"2018-10-17T02:00:00Z\"," +
                        "\"component\":\"component\"," +
                        "\"workflow\":\"workflow2\","+
                        "\"concurrency\":1}";

    Response<ByteString> response = awaitResponse(
        serviceHelper.request("POST", path("") + "?allowFuture=true", ByteString.encodeUtf8(json)));

    assertThat(response.status().reasonPhrase(),
        response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    Backfill postedBackfill = Json.OBJECT_MAPPER.readValue(
        response.payload().orElseThrow().toByteArray(), Backfill.class);
    assertThat(postedBackfill.id().matches("backfill-[\\d-]+"), is(true));
    assertThat(postedBackfill.start(), equalTo(Instant.parse("2018-10-16T00:00:00Z")));
    assertThat(postedBackfill.end(), equalTo(Instant.parse("2018-10-17T02:00:00Z")));
    assertThat(postedBackfill.workflowId(), equalTo(WorkflowId.create("component", "workflow2")));
    assertThat(postedBackfill.concurrency(), equalTo(1));
    assertThat(postedBackfill.description(), equalTo(Optional.empty()));
    assertThat(postedBackfill.nextTrigger(), equalTo(Instant.parse("2018-10-16T00:00:00Z")));
    assertThat(postedBackfill.schedule(), equalTo(Schedule.HOURS));
    assertThat(postedBackfill.allTriggered(), equalTo(false));
    assertThat(postedBackfill.halted(), equalTo(false));
    assertThat(postedBackfill.reverse(), equalTo(false));
    assertThat(postedBackfill.created().orElseThrow(), equalTo(currentTime));
    assertThat(postedBackfill.lastModified().orElseThrow(), equalTo(currentTime));
  }

  @Test
  public void shouldReturnDifferentTimestampWhenUpdateBackfill() throws Exception {
    sinceVersion(Api.Version.V3);

    final EditableBackfillInput backfillInput = EditableBackfillInput.newBuilder()
        .id(BACKFILL_1.id())
        .description("updated")
        .build();

    var previousTime = this.currentTime;
    var updateTime = Instant.parse("2019-10-16T00:00:00Z");
    this.currentTime = Instant.parse("2019-10-16T00:00:00Z");

    final Response<ByteString> response;
    try {
      response = awaitResponse(serviceHelper.request("PUT", path("/" + BACKFILL_1.id()),
          Json.serialize(backfillInput)));
    } finally {
      this.currentTime = previousTime;
    }

    assertThat(response.status().reasonPhrase(),
        response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    Backfill postedBackfill = Json.OBJECT_MAPPER.readValue(
        response.payload().orElseThrow().toByteArray(), Backfill.class);
    assertThat(postedBackfill.lastModified().orElseThrow(), equalTo(updateTime));
  }

  @Test
  public void shouldRetrieveOneBackfillUsingStartParameter()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_4.builder().allTriggered(true).build());
    storage.storeBackfill(BACKFILL_6.builder().allTriggered(true).build());

    final String uri = path(String.format("?showAll=true&component=%s&workflow=%s&start=%s",
        BACKFILL_4.workflowId().componentId(),
        BACKFILL_4.workflowId().id(),
        "2019-01-01"));
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", uri));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(1));
  }

  @Test
  public void shouldFilterOutAllBackfillsUsingStartParameter()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_4.builder().allTriggered(true).build());
    storage.storeBackfill(BACKFILL_6.builder().allTriggered(true).build());

    final String uri = path(String.format("?showAll=true&component=%s&workflow=%s&start=%s",
        BACKFILL_4.workflowId().componentId(),
        BACKFILL_4.workflowId().id(),
        "2022-01-01"));
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", uri));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(0));
  }

  @Test
  public void shouldUseDefaultValueForStartParameter()
      throws IOException, ExecutionException, InterruptedException, TimeoutException {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_4.builder().allTriggered(true).build());
    storage.storeBackfill(BACKFILL_6.builder().allTriggered(true).build());

    final String uri = path(String.format("?showAll=true&component=%s&workflow=%s",
        BACKFILL_4.workflowId().componentId(),
        BACKFILL_4.workflowId().id()));
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", uri));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(2));
  }

  private Connection setupBigTableMockTable() {
    Connection bigtable = mock(Connection.class);
    try {
      new BigtableMocker(bigtable)
          .setNumFailures(0)
          .setupTable(BigtableStorage.EVENTS_TABLE_NAME)
          .finalizeMocking();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return bigtable;
  }
}
