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

import static com.spotify.apollo.test.unit.ResponseMatchers.hasPayload;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withCode;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withReasonPhrase;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.styx.api.RunStateDataPayload.RunStateData;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.TriggerParameters;
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
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.logging.Level;
import okio.ByteString;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class StatusResourceTest extends VersionedApiTest {

  private static final String COMPONENT_ID = "styx";
  private static final String ID = "test";
  private static final String PARAMETER = "1234";
  private static final Trigger TRIGGER = Trigger.unknown("foobar");
  private static final String OTHER_COMPONENT_ID = "styx-other";
  private static final WorkflowInstance WFI =
      WorkflowInstance.create(WorkflowId.create(COMPONENT_ID, ID), PARAMETER);
  private static final WorkflowInstance OTHER_WFI =
      WorkflowInstance.create(WorkflowId.create(OTHER_COMPONENT_ID, ID), PARAMETER);

  private static final RunState PERSISTENT_STATE = RunState.create(WFI, State.RUNNING,
      StateData.zero(), Instant.now(), 42L);

  private static final RunState OTHER_PERSISTENT_STATE = RunState.create(OTHER_WFI, State.RUNNING,
      StateData.zero(), Instant.now(), 84L);

  private static LocalDatastoreHelper localDatastore;
  private Connection bigtable = setupBigTableMockTable();

  private Storage storage;
  private ServiceAccountUsageAuthorizer accountUsageAuthorizer;

  public StatusResourceTest(Api.Version version) {
    super(StatusResource.BASE, version);
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    final java.util.logging.Logger datastoreEmulatorLogger =
        java.util.logging.Logger.getLogger(LocalDatastoreHelper.class.getName());
    datastoreEmulatorLogger.setLevel(Level.OFF);

    localDatastore = LocalDatastoreHelper.create(1.0); // 100% global consistency
    localDatastore.start();
  }

  @AfterClass
  public static void tearDownClass() {
    if (localDatastore != null) {
      try {
        localDatastore.stop(org.threeten.bp.Duration.ofSeconds(30));
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  @After
  public void tearDown() throws Exception {
    localDatastore.reset();
  }

  @Override
  protected void init(Environment environment) {
    accountUsageAuthorizer = mock(ServiceAccountUsageAuthorizer.class);
    storage = spy(new AggregateStorage(bigtable, localDatastore.getOptions().getService(),
        Duration.ZERO));
    final StatusResource statusResource = new StatusResource(storage, accountUsageAuthorizer);

    environment.routingEngine()
        .registerRoutes(statusResource.routes());
  }

  @Test
  public void testEventsRoundtrip() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI, TRIGGER, TriggerParameters.zero()), 0L, 0L));
    storage.writeEvent(SequenceEvent.create(Event.created(WFI, "exec0", "img0"), 1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.started(WFI), 2L, 2L));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/events/styx/test/1234")));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertThat(response, hasPayload(any(ByteString.class)));

    String json = response.payload().get().utf8();
    EventsPayload parsed = Json.OBJECT_MAPPER.readValue(json, EventsPayload.class);

    assertThat(parsed.events(), hasSize(3));
  }

  @Test
  public void testGetAllActiveStates() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.writeActiveState(WFI, PERSISTENT_STATE);
    storage.writeActiveState(OTHER_WFI, OTHER_PERSISTENT_STATE);
    assertThat(storage.readActiveStates().entrySet(), hasSize(2));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/activeStates")));

    assertThat(response, hasStatus(withCode(Status.OK)));

    String json = response.payload().get().utf8();
    RunStateDataPayload
        parsed = Json.OBJECT_MAPPER.readValue(json, RunStateDataPayload.class);

    assertThat(parsed.activeStates(), containsInAnyOrder(
        RunStateData.newBuilder()
            .workflowInstance(PERSISTENT_STATE.workflowInstance())
            .state(PERSISTENT_STATE.state().name())
            .stateData(PERSISTENT_STATE.data())
            .latestTimestamp(PERSISTENT_STATE.timestamp())
            .build(),
        RunStateData.newBuilder()
            .workflowInstance(OTHER_PERSISTENT_STATE.workflowInstance())
            .state(OTHER_PERSISTENT_STATE.state().name())
            .stateData(OTHER_PERSISTENT_STATE.data())
            .latestTimestamp(OTHER_PERSISTENT_STATE.timestamp())
            .build()
    ));
  }

  @Test
  public void testFilterActiveStatesOnComponent() throws Exception {
    sinceVersion(Api.Version.V3);

    WorkflowInstance OTHER_WFI =
        WorkflowInstance.create(WorkflowId.create(COMPONENT_ID + "-other", ID), PARAMETER);

    storage.writeActiveState(WFI, PERSISTENT_STATE);
    storage.writeActiveState(OTHER_WFI, OTHER_PERSISTENT_STATE);
    assertThat(storage.readActiveStates().entrySet(), hasSize(2));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/activeStates?component=" + COMPONENT_ID)));

    assertThat(response, hasStatus(withCode(Status.OK)));

    String json = response.payload().get().utf8();
    RunStateDataPayload
        parsed = Json.OBJECT_MAPPER.readValue(json, RunStateDataPayload.class);

    assertThat(parsed.activeStates(), hasSize(1));
    assertThat(parsed.activeStates().get(0).workflowInstance().workflowId().componentId(), is(COMPONENT_ID));
  }

  @Test
  public void testGetActiveStatesFailedDueToStorageIOException() throws Exception {
    sinceVersion(Api.Version.V3);

    IOException ioException = new IOException("forced failure");
    when(storage.readActiveStates()).thenThrow(ioException);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/activeStates")));

    assertThat(response, hasStatus(withCode(Status.INTERNAL_SERVER_ERROR)));
    assertThat(response, hasStatus(withReasonPhrase(is(": \"" + ioException.toString() + "\""))));
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
