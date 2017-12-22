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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.InMemStorage;
import com.spotify.styx.storage.Storage;
import java.time.Instant;
import okio.ByteString;
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
  private static final RunState RUN_STATE = RunState.create(WFI, RunState.State.RUNNING, ()->Instant.now(),
                                                            OutputHandler.NOOP);
  private static final RunState OTHER_RUN_STATE = RunState.create(OTHER_WFI, RunState.State.RUNNING, ()->Instant.now(),
                                                                  OutputHandler.NOOP);

  private Storage storage = new InMemStorage();

  public StatusResourceTest(Api.Version version) {
    super(StatusResource.BASE, version);
  }

  @Override
  protected void init(Environment environment) {
    final StatusResource statusResource = new StatusResource(storage);

    environment.routingEngine()
        .registerRoutes(Api.withCommonMiddleware(
            statusResource.routes()));
  }

  @Test
  public void testEventsRoundtrip() throws Exception {
    sinceVersion(Api.Version.V3);

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
    sinceVersion(Api.Version.V3);

    storage.writeActiveState(WFI, RUN_STATE, 42L);
    storage.writeActiveState(OTHER_WFI, OTHER_RUN_STATE, 84L);
    assertThat(storage.readActiveWorkflowInstances().entrySet(), hasSize(2));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/activeStates")));

    assertThat(response, hasStatus(withCode(Status.OK)));

    String json = response.payload().get().utf8();
    RunStateDataPayload
        parsed = Json.OBJECT_MAPPER.readValue(json, RunStateDataPayload.class);

    assertThat(parsed.activeStates(), hasSize(2));
  }

  @Test
  public void testFilterActiveStatesOnComponent() throws Exception {
    sinceVersion(Api.Version.V3);

    WorkflowInstance OTHER_WFI =
        WorkflowInstance.create(WorkflowId.create(COMPONENT_ID + "-other", ID), PARAMETER);

    storage.writeActiveState(WFI, RUN_STATE, 42L);
    storage.writeActiveState(OTHER_WFI, OTHER_RUN_STATE, 84L);
    assertThat(storage.readActiveWorkflowInstances().entrySet(), hasSize(2));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/activeStates?component=" + COMPONENT_ID)));

    assertThat(response, hasStatus(withCode(Status.OK)));

    String json = response.payload().get().utf8();
    RunStateDataPayload
        parsed = Json.OBJECT_MAPPER.readValue(json, RunStateDataPayload.class);

    assertThat(parsed.activeStates(), hasSize(1));
    assertThat(parsed.activeStates().get(0).workflowInstance().workflowId().componentId(), is(COMPONENT_ID));
  }
}
