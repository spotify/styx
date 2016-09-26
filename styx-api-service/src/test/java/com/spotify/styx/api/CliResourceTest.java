/*
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

import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.test.ServiceHelper;
import com.spotify.styx.api.cli.EventsPayload;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.storage.EventStorage;
import com.spotify.styx.storage.InMemStorage;
import com.spotify.styx.util.Json;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import okio.ByteString;

import static com.spotify.apollo.test.unit.ResponseMatchers.hasPayload;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withCode;
import static com.spotify.styx.api.ApiVersionTestUtils.ALL_VERSIONS;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class CliResourceTest {

  private static final String COMPONENT_ID = "styx";
  private static final String ENDPOINT_ID = "test";
  private static final String PARAMETER = "1234";
  private static final String TRIGGER = "foobar";
  private static final WorkflowInstance WFI =
      WorkflowInstance.create(WorkflowId.create(COMPONENT_ID, ENDPOINT_ID), PARAMETER);

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> versions() {
    return Stream.of(ALL_VERSIONS)
        .map(v -> new Object[]{v})
        .collect(Collectors.toList());
  }

  @Rule
  public ServiceHelper serviceHelper = ServiceHelper.create(this::init, "styx");

  private final Api.Version version;

  private EventStorage eventStorage = new InMemStorage();

  public CliResourceTest(Api.Version version) {
    this.version = version;
  }

  private void init(Environment environment) {
    final CliResource cliResource = new CliResource(eventStorage);

    environment.routingEngine()
        .registerRoutes(cliResource.routes());
  }

  @Test
  public void testEventInjectionProxy() throws Exception {
    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.ACCEPTED))
        .to("hm://styx-scheduler/api/v0/events");

    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", path("/events"));

    Response<ByteString> response = post.toCompletableFuture().get();
    assertThat(response, hasStatus(withCode(Status.ACCEPTED)));
  }

  @Test
  public void testTriggerWorkflowInstanceProxy() throws Exception {
    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.ACCEPTED))
        .to("hm://styx-scheduler/api/v0/trigger");

    CompletionStage<Response<ByteString>> post =
        serviceHelper.request("POST", path("/trigger"));

    Response<ByteString> response = post.toCompletableFuture().get();
    assertThat(response, hasStatus(withCode(Status.ACCEPTED)));
  }

  @Test
  public void testEventsRoundtrip() throws Exception {
    eventStorage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI, TRIGGER), 0L, 0L));
    eventStorage.writeEvent(SequenceEvent.create(Event.created(WFI, "exec0", "img0"), 1L, 1L));
    eventStorage.writeEvent(SequenceEvent.create(Event.started(WFI), 2L, 2L));

    CompletionStage<Response<ByteString>> get =
        serviceHelper.request("GET", path("/events/styx/test/1234"));

    Response<ByteString> response = get.toCompletableFuture().get();
    assertThat(response, hasStatus(withCode(Status.OK)));
    assertThat(response, hasPayload(any(ByteString.class)));

    final String json = response.payload().get().utf8();
    EventsPayload parsed = Json.OBJECT_MAPPER.readValue(json, EventsPayload.class);

    assertThat(parsed.events(), hasSize(3));
  }

  private String path(String path) {
    return version.prefix() + CliResource.BASE + path;
  }
}
