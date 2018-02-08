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

import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.belongsToFamily;
import static com.spotify.styx.api.JsonMatchers.assertJson;
import static com.spotify.styx.api.JsonMatchers.assertNoJson;
import static com.spotify.styx.testdata.TestData.EXECUTION_DESCRIPTION;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.StatusType;
import com.spotify.apollo.test.StubClient;
import com.spotify.apollo.test.response.ResponseWithDelay;
import com.spotify.apollo.test.response.Responses;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.SequenceEvent;
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
import com.spotify.styx.util.WorkflowValidator;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import okio.ByteString;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BackfillResourceTest extends VersionedApiTest {

  private static final String SCHEDULER_BASE = "http://localhost:12345";

  private static LocalDatastoreHelper localDatastore;
  private Connection bigtable = setupBigTableMockTable();

  private AggregateStorage storage = new AggregateStorage(
      bigtable,
      localDatastore.getOptions().getService(),
      Duration.ZERO);

  private static final Backfill BACKFILL_1 = Backfill.newBuilder()
      .id("backfill-1")
      .start(Instant.parse("2017-01-01T00:00:00Z"))
      .end(Instant.parse("2017-01-02T00:00:00Z"))
      .workflowId(WorkflowId.create("component", "workflow1"))
      .concurrency(1)
      .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
      .schedule(Schedule.HOURS)
      .build();

  private static final Backfill BACKFILL_2 = Backfill.newBuilder()
      .id("backfill-2")
      .start(Instant.parse("2017-01-01T00:00:00Z"))
      .end(Instant.parse("2017-01-02T00:00:00Z"))
      .workflowId(WorkflowId.create("component", "workflow2"))
      .concurrency(2)
      .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
      .schedule(Schedule.DAYS)
      .build();

  private static final Backfill BACKFILL_3 = Backfill.newBuilder()
      .id("backfill-3")
      .start(Instant.parse("2017-01-01T00:00:00Z"))
      .end(Instant.parse("2017-01-02T00:00:00Z"))
      .workflowId(WorkflowId.create("other_component", "other_workflow"))
      .concurrency(2)
      .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
      .schedule(Schedule.DAYS)
      .build();

  private final WorkflowValidator workflowValidator = mock(WorkflowValidator.class);

  public BackfillResourceTest(Api.Version version) {
    super(BackfillResource.BASE, version, "backfill-test", spy(StubClient.class));
  }

  @Override
  protected void init(Environment environment) {
    environment.routingEngine()
        .registerRoutes(Api.withCommonMiddleware(
            new BackfillResource(SCHEDULER_BASE, storage, workflowValidator).routes()));
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    localDatastore = LocalDatastoreHelper.create(1.0); // 100% global consistency
    localDatastore.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (localDatastore != null) {
      try {
        localDatastore.stop(org.threeten.bp.Duration.ofSeconds(30));
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    when(workflowValidator.validateWorkflow(any())).thenReturn(Collections.emptyList());
    when(workflowValidator.validateWorkflowConfiguration(any())).thenReturn(Collections.emptyList());
    storage.storeWorkflow(Workflow.create(
        BACKFILL_1.workflowId().componentId(),
        WorkflowConfiguration.builder()
            .id(BACKFILL_1.workflowId().id())
            .schedule(Schedule.HOURS)
            .dockerImage("foobar")
            .build()));
    storage.storeWorkflow(Workflow.create(
        BACKFILL_2.workflowId().componentId(),
        WorkflowConfiguration.builder()
            .id(BACKFILL_2.workflowId().id())
            .schedule(Schedule.HOURS)
            .dockerImage("foobar")
            .build()));
    storage.storeWorkflow(Workflow.create(
        BACKFILL_3.workflowId().componentId(),
        WorkflowConfiguration.builder()
            .id(BACKFILL_3.workflowId().id())
            .schedule(Schedule.HOURS)
            .build()));
    storage.storeBackfill(BACKFILL_1);
  }

  @After
  public void tearDown() throws Exception {
    // clear datastore after each test
    Datastore datastore = localDatastore.getOptions().getService();
    KeyQuery query = Query.newKeyQueryBuilder().build();
    final QueryResults<Key> keys = datastore.run(query);
    while (keys.hasNext()) {
      datastore.delete(keys.next());
    }
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

    storage.storeBackfill(BACKFILL_3.builder().allTriggered(true).build());

    final String uri = path(String.format("?showAll=true&component=%s",
                                          BACKFILL_3.workflowId().componentId()));
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", uri));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(1));
  }

  @Test
  public void shouldFilterBackfillsOnComponent() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_3);

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

    storage.storeBackfill(BACKFILL_2);
    storage.storeBackfill(BACKFILL_3);

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

    storage.storeBackfill(BACKFILL_2);
    storage.storeBackfill(BACKFILL_3);

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

    storage.storeBackfill(BACKFILL_2.builder().allTriggered(true).build());
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(1));
  }

  @Test
  public void shouldListAllBackfillsWithFlag() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_2.builder().halted(true).build());
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("?showAll=true")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(2));
  }
   @Test
  public void shouldListMultipleBackfills() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_2);
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
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(wfi, Trigger.backfill("backfill-1")), 1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi),                                          2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi, EXECUTION_DESCRIPTION, "exec-1"),          3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi, "exec-1"),                              4L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi),                                          5L, 5L));
    storage.writeActiveState(wfi, RunState.create(wfi, State.RUNNING,
        StateData.zero(), Instant.now(), 5L));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/" + BACKFILL_1.id())));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfill.id", equalTo(BACKFILL_1.id()));
    assertJson(response, "statuses.active_states[0].state", equalTo("UNKNOWN"));
    assertJson(response, "statuses.active_states[1].state", equalTo("RUNNING"));
    assertJson(response, "statuses.active_states[2].state", equalTo("WAITING"));
    assertJson(response, "statuses.active_states[23].state", equalTo("WAITING"));
    assertJson(response, "statuses.active_states", hasSize(24));
  }

  @Test
  public void shouldGetBackfillWithoutStatus() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeBackfill(BACKFILL_1.builder().nextTrigger(Instant.parse("2017-01-01T02:00:00Z")).build());

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/" + BACKFILL_1.id() + "?status=false")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfill.id", equalTo(BACKFILL_1.id()));
    assertNoJson(response, "statuses");
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
        response.payload().get().toByteArray(), Backfill.class);
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
        response.payload().get().toByteArray(), Backfill.class);
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
  public void shouldFailOnMisalignedRange() throws Exception {
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
  public void shouldFailOnAlreadyActiveWithinRange() throws Exception {
    sinceVersion(Api.Version.V3);

    final BackfillInput backfillInput = BackfillInput.create(
        BACKFILL_1.start(),
        BACKFILL_1.end(),
        BACKFILL_1.workflowId().componentId(),
        BACKFILL_1.workflowId().id(),
        BACKFILL_1.concurrency(),
        BACKFILL_1.description());

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

    assertThat(storage.backfill(BACKFILL_1.id()).get().concurrency(), equalTo(1));

    final Backfill updatedBackfill = BACKFILL_1.builder().concurrency(4).build();
    final String json = Json.OBJECT_MAPPER.writeValueAsString(updatedBackfill);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PUT", path("/" + BACKFILL_1.id()),
                                            ByteString.encodeUtf8(json)));

    assertThat(response.status().reasonPhrase(),
               response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "id", equalTo(BACKFILL_1.id()));
    assertJson(response, "concurrency", equalTo(4));

    assertThat(storage.backfill(BACKFILL_1.id()).get().concurrency(), equalTo(4));
  }

  @Test
  public void shouldHaltBackfill() throws Exception {
    sinceVersion(Api.Version.V3);

    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.ACCEPTED))
        .to(SCHEDULER_BASE + "/api/v0/events");

    WorkflowInstance wfi = WorkflowInstance.create(BACKFILL_1.workflowId(), "2017-01-01T01");
    storage.storeBackfill(BACKFILL_1.builder().nextTrigger(Instant.parse("2017-01-01T02:00:00Z")).build());
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(wfi, Trigger.backfill("backfill-1")), 1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi),                                          2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi, EXECUTION_DESCRIPTION, "exec-1"),          3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi, "exec-1"),                              4L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi),                                          5L, 5L));
    storage.writeActiveState(wfi, RunState.create(wfi, State.RUNNING,
        StateData.zero(), Instant.now(), 5L));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("DELETE", path("/" + BACKFILL_1.id())));
    assertThat(response.status().reasonPhrase(),
        response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));

    assertThat(storage.backfill(BACKFILL_1.id()).get().halted(), equalTo(true));
    verify(serviceHelper.stubClient(), times(1)).send(any());
  }

  @Test
  public void shouldNotHaltWaitingInstanceInBackfill() throws Exception {
    sinceVersion(Api.Version.V3);

    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.ACCEPTED))
        .to(SCHEDULER_BASE + "/api/v0/events");

    WorkflowInstance wfi1 = WorkflowInstance.create(BACKFILL_1.workflowId(), "2017-01-01T01");
    WorkflowInstance wfi2 = WorkflowInstance.create(BACKFILL_1.workflowId(), "2017-01-01T02");
    storage.storeBackfill(BACKFILL_1.builder().nextTrigger(Instant.parse("2017-01-01T03:00:00Z")).build());
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(wfi1, Trigger.backfill("backfill-1")), 1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi1),                                          2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi1, EXECUTION_DESCRIPTION, "exec-1"),          3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi1, "exec-1"),                              4L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi1),                                          5L, 5L));
    storage.writeActiveState(wfi1, RunState.create(wfi1, State.RUNNING,
        StateData.zero(), Instant.now(), 5L));
    storage.writeActiveState(wfi2, RunState.create(wfi2, State.RUNNING,
        StateData.zero(), Instant.now(), 5L));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("DELETE", path("/" + BACKFILL_1.id())));
    assertThat(response.status().reasonPhrase(),
               response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));

    assertThat(storage.backfill(BACKFILL_1.id()).get().halted(), equalTo(true));
    verify(serviceHelper.stubClient(), times(1)).send(any());
  }

  @Test
  public void shouldReturnServerErrorIfFailedToSend() throws Exception {
    sinceVersion(Api.Version.V3);

    serviceHelper.stubClient().respond(Responses.sequence(ImmutableList.of(ResponseWithDelay
                                                                               .forResponse(Response
                                                                                                .forStatus(
                                                                                                    Status.INTERNAL_SERVER_ERROR)),
                                                                           ResponseWithDelay
                                                                               .forResponse(Response
                                                                                                .forStatus(
                                                                                                    Status.ACCEPTED)))))
        .to(SCHEDULER_BASE + "/api/v0/events");

    WorkflowInstance wfi1 = WorkflowInstance.create(BACKFILL_1.workflowId(), "2017-01-01T01");
    WorkflowInstance wfi2 = WorkflowInstance.create(BACKFILL_1.workflowId(), "2017-01-01T02");
    storage.storeBackfill(BACKFILL_1.builder().nextTrigger(Instant.parse("2017-01-01T03:00:00Z")).build());

    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(wfi1, Trigger.backfill("backfill-1")), 1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi1),                                          2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi1, EXECUTION_DESCRIPTION, "exec-1"),          3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi1, "exec-1"),                              4L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi1),                                          5L, 5L));

    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(wfi2, Trigger.backfill("backfill-1")), 1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi2),                                          2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi2, EXECUTION_DESCRIPTION, "exec-2"),          3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi2, "exec-2"),                              4L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi2),                                          5L, 5L));

    storage.writeActiveState(wfi1, RunState.create(wfi1, State.RUNNING,
        StateData.zero(), Instant.now(), 5L));
    storage.writeActiveState(wfi2, RunState.create(wfi2, State.RUNNING,
        StateData.zero(), Instant.now(), 5L));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("DELETE", path("/" + BACKFILL_1.id())));
    assertThat(response.status().reasonPhrase(),
               response, hasStatus(belongsToFamily(StatusType.Family.SERVER_ERROR)));

    assertThat(storage.backfill(BACKFILL_1.id()).get().halted(), equalTo(true));
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

    when(workflowValidator.validateWorkflow(any())).thenReturn(ImmutableList.of("bad", "f00d"));

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
        is(Status.BAD_REQUEST.withReasonPhrase("Workflow is missing docker image")));
  }

  private Connection setupBigTableMockTable() {
    Connection bigtable = mock(Connection.class);
    try {
      new BigtableMocker(bigtable)
          .setNumFailures(0)
          .setupTable(BigtableStorage.EVENTS_TABLE_NAME)
          .finalizeMocking();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return bigtable;
  }
}
