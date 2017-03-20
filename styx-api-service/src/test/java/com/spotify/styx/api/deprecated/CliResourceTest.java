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

package com.spotify.styx.api.deprecated;

import static com.spotify.apollo.test.unit.ResponseMatchers.hasPayload;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withCode;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.styx.api.Api;
import com.spotify.styx.api.EventsPayload;
import com.spotify.styx.api.StatusResource;
import com.spotify.styx.api.VersionedApiTest;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.InMemStorage;
import com.spotify.styx.storage.Storage;
import okio.ByteString;
import org.junit.Test;

@Deprecated
public class CliResourceTest extends VersionedApiTest {

  private static final String SCHEDULER_BASE = "http://localhost:8080";
  private static final String COMPONENT_ID = "styx";
  private static final String ENDPOINT_ID = "test";
  private static final String PARAMETER = "1234";
  private static final Trigger TRIGGER = Trigger.unknown("foobar");
  private static final String OTHER_COMPONENT_ID = "styx-other";
  private static final WorkflowInstance WFI =
      WorkflowInstance.create(WorkflowId.create(COMPONENT_ID, ENDPOINT_ID), PARAMETER);
  private static final WorkflowInstance OTHER_WFI =
      WorkflowInstance.create(WorkflowId.create(OTHER_COMPONENT_ID, ENDPOINT_ID), PARAMETER);

  private Storage storage = new InMemStorage();

  public CliResourceTest(Api.Version version) {
    super(CliResource.BASE, version);
  }

  @Override
  protected void init(Environment environment) {
    final CliResource
        cliResource =
        new CliResource(new StatusResource(storage), SCHEDULER_BASE);

    environment.routingEngine().registerRoutes(cliResource.routes());
  }

  @Test
  public void testEventInjectionProxy() throws Exception {
    tillVersion(Api.Version.V1);

    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.ACCEPTED))
        .to(SCHEDULER_BASE + "/api/v0/events");

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path("/events")));

    assertThat(response, hasStatus(withCode(Status.ACCEPTED)));
  }

  @Test
  public void testTriggerWorkflowInstanceProxy() throws Exception {
    tillVersion(Api.Version.V1);

    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.ACCEPTED).withPayload(Json.serialize(WFI)))
        .to(SCHEDULER_BASE + "/api/v0/trigger");

    com.spotify.styx.model.deprecated.WorkflowInstance workflowInstance =
        com.spotify.styx.model.deprecated.WorkflowInstance.create(WFI);
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path("/trigger"),
                                            Json.serialize(workflowInstance)));

    assertThat(response, hasStatus(withCode(Status.ACCEPTED)));
    assertThat(Json.deserialize(response.payload().get(),
                                com.spotify.styx.model.deprecated.WorkflowInstance.class),
               is(workflowInstance));
  }

  @Test
  public void testEventsRoundtrip() throws Exception {
    tillVersion(Api.Version.V1);

    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI, TRIGGER), 0L, 0L));
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
    tillVersion(Api.Version.V1);

    storage.writeActiveState(WFI, 42L);
    storage.writeActiveState(OTHER_WFI, 84L);
    assertThat(storage.readActiveWorkflowInstances().entrySet(), hasSize(2));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/activeStates")));

    assertThat(response, hasStatus(withCode(Status.OK)));

    String json = response.payload().get().utf8();
    RunStateDataPayload parsed = Json.OBJECT_MAPPER.readValue(json, RunStateDataPayload.class);

    assertThat(parsed.activeStates(), hasSize(2));
  }

  @Test
  public void testFilterActiveStatesOnComponent() throws Exception {
    tillVersion(Api.Version.V1);

    WorkflowInstance OTHER_WFI =
        WorkflowInstance.create(WorkflowId.create(COMPONENT_ID + "-other", ENDPOINT_ID), PARAMETER);

    storage.writeActiveState(WFI, 42L);
    storage.writeActiveState(OTHER_WFI, 84L);
    assertThat(storage.readActiveWorkflowInstances().entrySet(), hasSize(2));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/activeStates?component=" + COMPONENT_ID)));

    assertThat(response, hasStatus(withCode(Status.OK)));

    String json = response.payload().get().utf8();
    RunStateDataPayload parsed = Json.OBJECT_MAPPER.readValue(json, RunStateDataPayload.class);

    assertThat(parsed.activeStates(), hasSize(1));
    assertThat(parsed.activeStates().get(0).workflowInstance().workflowId().componentId(), is(COMPONENT_ID));
  }
}
