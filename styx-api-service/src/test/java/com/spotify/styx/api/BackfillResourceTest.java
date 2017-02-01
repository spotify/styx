/*-
 * -\-\-
 * Spotify Styx API Service
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

import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.belongsToFamily;
import static com.spotify.styx.api.JsonMatchers.assertJson;
import static com.spotify.styx.api.JsonMatchers.assertNoJson;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.google.common.base.Throwables;
import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.StatusType;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.DataEndpoint;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.BigtableMocker;
import com.spotify.styx.storage.BigtableStorage;
import com.spotify.styx.testdata.TestData;
import java.io.IOException;
import java.net.URI;
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

  private static LocalDatastoreHelper localDatastore;
  private Connection bigtable = setupBigTableMockTable();

  private AggregateStorage storage = new AggregateStorage(
      bigtable,
      localDatastore.options().service(),
      Duration.ZERO);

  private static final Backfill BACKFILL_1 = Backfill.newBuilder()
      .id("backfill-1")
      .start(Instant.parse("2017-01-01T00:00:00Z"))
      .end(Instant.parse("2017-01-02T00:00:00Z"))
      .workflowId(WorkflowId.create("component", "workflow1"))
      .concurrency(1)
      .resource("backfill-1")
      .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
      .partitioning(Partitioning.HOURS)
      .build();

  private static final Backfill BACKFILL_2 = Backfill.newBuilder()
      .id("backfill-2")
      .start(Instant.parse("2017-01-01T00:00:00Z"))
      .end(Instant.parse("2017-01-02T00:00:00Z"))
      .workflowId(WorkflowId.create("component", "workflow2"))
      .concurrency(2)
      .resource("backfill-2")
      .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
      .partitioning(Partitioning.DAYS)
      .build();

  public BackfillResourceTest(Api.Version version) {
    super(BackfillResource.BASE, version, "backfill-test");
  }

  @Override
  void init(Environment environment) {
    environment.routingEngine().registerRoutes(new BackfillResource(storage).routes());
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    localDatastore = LocalDatastoreHelper.create(1.0); // 100% global consistency
    localDatastore.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (localDatastore != null) {
      localDatastore.stop();
    }
  }

  @Before
  public void setUp() throws Exception {
    storage.storeWorkflow(Workflow.create(BACKFILL_1.workflowId().componentId(), URI.create("http://example.com"), DataEndpoint
        .create(BACKFILL_1.workflowId().endpointId(), Partitioning.HOURS, Optional.empty(), Optional.empty(), Optional.empty(), Collections
            .emptyList())));
    storage.storeWorkflow(Workflow.create(BACKFILL_2.workflowId().componentId(), URI.create("http://example.com"), DataEndpoint
        .create(BACKFILL_2.workflowId().endpointId(), Partitioning.HOURS, Optional.empty(), Optional.empty(), Optional.empty(), Collections
            .emptyList())));
    storage.storeBackfill(BACKFILL_1);
  }

  @After
  public void tearDown() throws Exception {
    // clear datastore after each test
    Datastore datastore = localDatastore.options().service();
    KeyQuery query = Query.keyQueryBuilder().build();
    final QueryResults<Key> keys = datastore.run(query);
    while (keys.hasNext()) {
      datastore.delete(keys.next());
    }
  }

  @Test
  public void shouldListBackfillsNoStatus() throws Exception {
    sinceVersion(Api.Version.V1);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills[0].backfill.id", equalTo(BACKFILL_1.id()));
    assertNoJson(response, "backfills[0].statuses.active_states");
  }

  @Test
  public void shouldListBackfillsWithStatus() throws Exception {
    sinceVersion(Api.Version.V1);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("?status=true")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills[0].backfill.id", equalTo(BACKFILL_1.id()));
    assertJson(response, "backfills[0].statuses.active_states", hasSize(24));
  }

  @Test
  public void shouldListMultipleBackfills() throws Exception {
    sinceVersion(Api.Version.V1);

    storage.storeBackfill(BACKFILL_2);
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "backfills", hasSize(2));
  }

  @Test
  public void shouldGetBackfillStatus() throws Exception {
    sinceVersion(Api.Version.V1);

    WorkflowInstance wfi = WorkflowInstance.create(BACKFILL_1.workflowId(), "2017-01-01T01");
    storage.storeBackfill(BACKFILL_1.builder().nextTrigger(Instant.parse("2017-01-01T02:00:00Z")).build());
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(wfi, Trigger.backfill("backfill-1")), 1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.dequeue(wfi),                                          2L, 2L));
    storage.writeEvent(SequenceEvent.create(Event.submit(wfi, TestData.EXECUTION_DESCRIPTION),           3L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.submitted(wfi, "exec-1"),                              4L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(wfi),                                          5L, 5L));
    storage.writeActiveState(wfi, 5L);

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
  public void shouldPostBackfill() throws Exception {
    sinceVersion(Api.Version.V1);

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
    assertThat(postedBackfill.resource(), equalTo(postedBackfill.id()));
    assertThat(postedBackfill.nextTrigger(), equalTo(Instant.parse("2017-01-01T00:00:00Z")));
    assertThat(postedBackfill.partitioning(), equalTo(Partitioning.HOURS));
    assertThat(postedBackfill.completed(), equalTo(false));
    assertThat(postedBackfill.halted(), equalTo(false));
  }

  @Test
  public void shouldFailOnMisalignedRange() throws Exception {
    sinceVersion(Api.Version.V1);

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
    sinceVersion(Api.Version.V1);

    final BackfillInput backfillInput = BackfillInput.create(
        BACKFILL_1.start(),
        BACKFILL_1.end(),
        BACKFILL_1.workflowId().componentId(),
        BACKFILL_1.workflowId().endpointId(),
        BACKFILL_1.concurrency());

    storage.writeActiveState(WorkflowInstance.create(BACKFILL_1.workflowId(), "2017-01-01T01"), 0L);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""), Json.serialize(backfillInput)));

    assertThat(response.status().reasonPhrase(),
               response, hasStatus(belongsToFamily(StatusType.Family.CLIENT_ERROR)));
  }

  @Test
  public void shouldUpdateBackfill() throws Exception {
    sinceVersion(Api.Version.V1);

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
  public void shouldDeleteBackfill() throws Exception {
    sinceVersion(Api.Version.V1);

    assertThat(storage.backfill(BACKFILL_1.id()).get().concurrency(), equalTo(1));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("DELETE", path("/" + BACKFILL_1.id())));

    assertThat(response.status().reasonPhrase(),
               response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));

    assertThat(storage.backfill(BACKFILL_1.id()).get().halted(), equalTo(true));
  }

  @Test
  public void shouldOnlyUpdateBackfillIfSameId() throws Exception {
    sinceVersion(Api.Version.V1);

    final Backfill updatedBackfill = BACKFILL_1.builder().concurrency(4).build();
    final String json = Json.OBJECT_MAPPER.writeValueAsString(updatedBackfill);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PUT", path("/wrong-id"),
                                            ByteString.encodeUtf8(json)));

    assertThat(response.status().reasonPhrase(),
               response, hasStatus(belongsToFamily(StatusType.Family.CLIENT_ERROR)));
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
