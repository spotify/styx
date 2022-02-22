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
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.StatusType;
import com.spotify.styx.api.RunStateDataPayload.RunStateData;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer.ServiceAccountUsageAuthorizationResult;
import com.spotify.styx.api.util.ApiTestUtil;
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
import com.spotify.styx.storage.DatastoreEmulator;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.time.Instant;
import okio.ByteString;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

public class StatusResourceTest extends VersionedApiTest {

  private static final String C_ID_1 = "styx";
  private static final String WF_ID_1 = "test";
  private static final String WF_ID_2 = "foo";
  private static final String PARAMETER = "1234";
  private static final Trigger TRIGGER = Trigger.unknown("foobar");
  private static final String C_ID_2 = "styx-second";
  private static final String C_ID_3 = "styx-third";

  private static final WorkflowInstance WFI_1 =
      WorkflowInstance.create(WorkflowId.create(C_ID_1, WF_ID_1), PARAMETER);
  private static final WorkflowInstance WFI_2 =
      WorkflowInstance.create(WorkflowId.create(C_ID_2, WF_ID_1), PARAMETER);
  private static final WorkflowInstance WFI_3 =
      WorkflowInstance.create(WorkflowId.create(C_ID_1, WF_ID_2), PARAMETER);
  private static final WorkflowInstance WFI_4 =
      WorkflowInstance.create(WorkflowId.create(C_ID_3, WF_ID_1), PARAMETER);

  private static final RunState PERSISTENT_STATE_1 = RunState.create(WFI_1, State.RUNNING,
      StateData.zero(), Instant.now(), 42L);

  private static final RunState PERSISTENT_STATE_2 = RunState.create(WFI_2, State.RUNNING,
      StateData.zero(), Instant.now(), 84L);

  private static final RunState PERSISTENT_STATE_3 = RunState.create(WFI_3, State.RUNNING,
      StateData.zero(), Instant.now(), 84L);

  private static final RunState PERSISTENT_STATE_4 = RunState.create(WFI_4, State.RUNNING,
      StateData.zero(), Instant.now(), 84L);

  @ClassRule public static final DatastoreEmulator datastoreEmulator = new DatastoreEmulator();

  private Connection bigtable = setupBigTableMockTable();

  private Storage storage;

  private static final String AUTH_SERVICE_ACCOUNT = "foo@bar.iam.gserviceaccounts.com";
  private static final String AUTH_PRINCIPAL = "bar@example.com";
  private static final ByteString AUTH_PAYLOAD = ByteString.encodeUtf8(
      String.format("{\"service_account\":\"%s\",\"principal\":\"%s\"}", AUTH_SERVICE_ACCOUNT, AUTH_PRINCIPAL));
  private ServiceAccountUsageAuthorizer accountUsageAuthorizer;

  public StatusResourceTest(Api.Version version) {
    super(StatusResource.BASE, version);
  }

  @After
  public void tearDown() {
    datastoreEmulator.reset();
  }

  @Override
  protected void init(Environment environment) {
    accountUsageAuthorizer = mock(ServiceAccountUsageAuthorizer.class);
    storage = spy(new AggregateStorage(bigtable, datastoreEmulator.client()));
    final StatusResource statusResource = new StatusResource(storage, accountUsageAuthorizer);

    environment.routingEngine()
        .registerRoutes(statusResource.routes().map(r ->
            r.withMiddleware(Middlewares.exceptionAndRequestIdHandler())));
  }

  @Test
  public void testEventsRoundtrip() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI_1, TRIGGER, TriggerParameters.zero()), 0L, 0L));
    storage.writeEvent(SequenceEvent.create(Event.created(WFI_1, "exec0", "img0"), 1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.started(WFI_1), 2L, 2L));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/events/styx/test/1234")));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertThat(response, hasPayload(any(ByteString.class)));

    String json = response.payload().orElseThrow().utf8();
    EventsPayload parsed = Json.OBJECT_MAPPER.readValue(json, EventsPayload.class);

    assertThat(parsed.events(), hasSize(3));
  }

  @Test
  public void testGetAllActiveStates() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.writeActiveState(WFI_1, PERSISTENT_STATE_1);
    storage.writeActiveState(WFI_2, PERSISTENT_STATE_2);
    assertThat(storage.readActiveStates().entrySet(), hasSize(2));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/activeStates")));

    assertThat(response, hasStatus(withCode(Status.OK)));

    String json = response.payload().orElseThrow().utf8();
    RunStateDataPayload
        parsed = Json.OBJECT_MAPPER.readValue(json, RunStateDataPayload.class);

    assertThat(parsed.activeStates(), containsInAnyOrder(
        RunStateData.newBuilder()
            .workflowInstance(PERSISTENT_STATE_1.workflowInstance())
            .state(PERSISTENT_STATE_1.state().name())
            .stateData(PERSISTENT_STATE_1.data())
            .latestTimestamp(PERSISTENT_STATE_1.timestamp())
            .build(),
        RunStateData.newBuilder()
            .workflowInstance(PERSISTENT_STATE_2.workflowInstance())
            .state(PERSISTENT_STATE_2.state().name())
            .stateData(PERSISTENT_STATE_2.data())
            .latestTimestamp(PERSISTENT_STATE_2.timestamp())
            .build()
    ));
  }

  @Test
  public void testFilterActiveStatesOnComponent() throws Exception {
    sinceVersion(Api.Version.V3);

    WorkflowInstance OTHER_WFI =
        WorkflowInstance.create(WorkflowId.create(C_ID_1 + "-other", WF_ID_1), PARAMETER);

    storage.writeActiveState(WFI_1, PERSISTENT_STATE_1);
    storage.writeActiveState(OTHER_WFI, PERSISTENT_STATE_2);
    storage.writeActiveState(WFI_3, PERSISTENT_STATE_3);
    assertThat(storage.readActiveStates().entrySet(), hasSize(3));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/activeStates?component=" + C_ID_1)));

    assertThat(response, hasStatus(withCode(Status.OK)));

    String json = response.payload().orElseThrow().utf8();
    RunStateDataPayload
        parsed = Json.OBJECT_MAPPER.readValue(json, RunStateDataPayload.class);

    assertThat(parsed.activeStates(), hasSize(2));
    assertThat(parsed.activeStates().get(0).workflowInstance().workflowId().componentId(), is(C_ID_1));
    assertThat(parsed.activeStates().get(0).workflowInstance().workflowId().id(), is(WF_ID_2));
    assertThat(parsed.activeStates().get(1).workflowInstance().workflowId().componentId(), is(C_ID_1));
    assertThat(parsed.activeStates().get(1).workflowInstance().workflowId().id(), is(WF_ID_1));
  }

  @Test
  public void testGetActiveStatesForMultiComponent() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.writeActiveState(WFI_1, PERSISTENT_STATE_1);
    storage.writeActiveState(WFI_2, PERSISTENT_STATE_2);
    storage.writeActiveState(WFI_3, PERSISTENT_STATE_3);
    storage.writeActiveState(WFI_4, PERSISTENT_STATE_4);
    assertThat(storage.readActiveStates().entrySet(), hasSize(4));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET",
            path("/activeStates?components=" + C_ID_1 + "," + C_ID_3 + "," + C_ID_1)));

    assertThat(response, hasStatus(withCode(Status.OK)));

    String json = response.payload().orElseThrow().utf8();
    RunStateDataPayload
        parsed = Json.OBJECT_MAPPER.readValue(json, RunStateDataPayload.class);

    assertThat(parsed.activeStates(), hasSize(3));

    var activeStates = ApiTestUtil.groupStatesByWorkflow(parsed.activeStates());

    assertThat(activeStates.get(WorkflowId.create(C_ID_1, WF_ID_1)).size(), is(1));
    assertThat(activeStates.get(WorkflowId.create(C_ID_1, WF_ID_2)).size(), is(1));
    assertThat(activeStates.get(WorkflowId.create(C_ID_3, WF_ID_1)).size(), is(1));
  }

  @Test
  public void testFilterActiveStatesOnWorkflow() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.writeActiveState(WFI_1, PERSISTENT_STATE_1);
    storage.writeActiveState(WFI_2, PERSISTENT_STATE_2);
    storage.writeActiveState(WFI_3, PERSISTENT_STATE_3);
    assertThat(storage.readActiveStates().entrySet(), hasSize(3));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET",
            path("/activeStates?component=" + C_ID_1 + "&workflow=" + WF_ID_1)));

    assertThat(response, hasStatus(withCode(Status.OK)));

    String json = response.payload().orElseThrow().utf8();
    RunStateDataPayload
        parsed = Json.OBJECT_MAPPER.readValue(json, RunStateDataPayload.class);

    assertThat(parsed.activeStates(), hasSize(1));
    assertThat(parsed.activeStates().get(0).workflowInstance().workflowId().componentId(), is(C_ID_1));
    assertThat(parsed.activeStates().get(0).workflowInstance().workflowId().id(), is(WF_ID_1));
  }

  @Test
  public void testGetMultiComponentsActiveStatesFailedDueToStorageIOException() throws Exception {
    sinceVersion(Api.Version.V3);

    IOException ioException = new IOException("forced failure");
    when(storage.readActiveStates()).thenThrow(ioException);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/activeStates")));

    assertThat(response, hasStatus(withCode(Status.INTERNAL_SERVER_ERROR)));
    assertThat(response.status().reasonPhrase(), containsString(": " + ioException.toString()));
  }

  @Test
  public void testGetActiveStatesFailedDueToStorageIOException() throws Exception {
    sinceVersion(Api.Version.V3);

    IOException ioException = new IOException("forced failure");
    when(storage.readActiveStates(anyString())).thenThrow(ioException);

    Response<ByteString> response =
        awaitResponse(
            serviceHelper.request(
                "GET", path("/activeStates?components=" + C_ID_1 + "," + C_ID_3)));

    assertThat(response, hasStatus(withCode(Status.INTERNAL_SERVER_ERROR)));
    assertThat(response.status().reasonPhrase(), containsString(": " + ioException.toString()));
  }

  @Test
  public void testGetActiveStatesFailedDueToMissingComponentId() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/activeStates?workflow=" + WF_ID_1)));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
    assertThat(response.status().reasonPhrase(), containsString("No component id specified!"));
  }

  @Test
  public void testAuthEndpointShouldFordwardAuthorizerResponse() throws Exception {

    final ServiceAccountUsageAuthorizationResult result = ServiceAccountUsageAuthorizationResult.builder()
        .authorized(true)
        .blacklisted(true)
        .message("Some access message")
        .serviceAccountProjectId("project")
        .build();

    sinceVersion(Api.Version.V3);

    when(accountUsageAuthorizer.checkServiceAccountUsageAuthorization(AUTH_SERVICE_ACCOUNT, AUTH_PRINCIPAL,
        isFlyteWorkflow))
        .thenReturn(result);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request(
            "POST",
            path("/testServiceAccountUsageAuthorization"),
            AUTH_PAYLOAD));

    assertThat(response, hasStatus(withCode(Status.OK)));

    String json = response.payload().orElseThrow().utf8();
    TestServiceAccountUsageAuthorizationResponse
        parsed = Json.OBJECT_MAPPER.readValue(json, TestServiceAccountUsageAuthorizationResponse.class);

    assertThat(parsed.authorized(), is(result.authorized()));
    assertThat(parsed.blacklisted(), is(result.blacklisted()));
    assertThat(parsed.serviceAccount(), is(AUTH_SERVICE_ACCOUNT));
    assertThat(parsed.principal(), is(AUTH_PRINCIPAL));
    assertThat(parsed.message(), is(result.message()));
  }

  @Test
  public void testAuthEndpointShouldReturnBadRequestIfProjectDoesNotExist() throws Exception {
    sinceVersion(Api.Version.V3);

    StatusType statusCode = Status.BAD_REQUEST.withReasonPhrase("Project does not exist: baz");
    when(accountUsageAuthorizer.checkServiceAccountUsageAuthorization(anyString(), anyString(), isFlyteWorkflow))
        .thenReturn(ServiceAccountUsageAuthorizationResult.ofErrorResponse(Response.forStatus(statusCode)));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request(
            "POST",
            path("/testServiceAccountUsageAuthorization"),
            AUTH_PAYLOAD));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
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
