/*
 * -\-\-
 * Spotify Styx Common
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
package com.spotify.styx.model;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;

import okio.ByteString;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class EventSerializerTest {

  private static final WorkflowId WORKFLOW1 = WorkflowId.create("component", "endpoint1");
  private static final String PARAMETER1 = "2016-01-01";
  private static final String TRIGGER1 = "trig1";
  private static final WorkflowInstance INSTANCE1 = WorkflowInstance.create(WORKFLOW1, PARAMETER1);
  private static final String POD_NAME = "test-event";
  private static final String DOCKER_IMAGE = "busybox:1.1";
  private static final String COMMIT_SHA = "00000ef508c1cb905e360590ce3e7e9193f6b370";
  private static final ExecutionDescription EXECUTION_DESCRIPTION = ExecutionDescription.create(
      DOCKER_IMAGE,
      Arrays.asList("foo", "bar"),
      Optional.of(DataEndpoint.Secret.create("secret", "/dev/null")),
      Optional.of(COMMIT_SHA));

  EventSerializer eventSerializer = new EventSerializer();

  @Test
  public void testRoundtripAllEvents() {
    assertRoundtrip(Event.timeTrigger(INSTANCE1));
    assertRoundtrip(Event.triggerExecution(INSTANCE1, TRIGGER1));
    assertRoundtrip(Event.created(INSTANCE1, POD_NAME, DOCKER_IMAGE));
    assertRoundtrip(Event.started(INSTANCE1));
    assertRoundtrip(Event.terminate(INSTANCE1, 20));
    assertRoundtrip(Event.runError(INSTANCE1, "ErrorMessage"));
    assertRoundtrip(Event.success(INSTANCE1));
    assertRoundtrip(Event.retryAfter(INSTANCE1, 12345));
    assertRoundtrip(Event.retry(INSTANCE1));
    assertRoundtrip(Event.stop(INSTANCE1));
    assertRoundtrip(Event.timeout(INSTANCE1));
    assertRoundtrip(Event.halt(INSTANCE1));
    assertRoundtrip(Event.submit(INSTANCE1, EXECUTION_DESCRIPTION));
    assertRoundtrip(Event.submitted(INSTANCE1, POD_NAME));
  }

  @Test
  public void testDeserializeFromJson() throws Exception {
    assertThat(eventSerializer.convert(json("timeTrigger")), is(Event.timeTrigger(INSTANCE1)));
    assertThat(eventSerializer.convert(json("started")), is(Event.started(INSTANCE1)));
    assertThat(eventSerializer.convert(json("success")), is(Event.success(INSTANCE1)));
    assertThat(eventSerializer.convert(json("retry")), is(Event.retry(INSTANCE1)));
    assertThat(eventSerializer.convert(json("stop")), is(Event.stop(INSTANCE1)));
    assertThat(eventSerializer.convert(json("timeout")), is(Event.timeout(INSTANCE1)));
    assertThat(eventSerializer.convert(json("halt")), is(Event.halt(INSTANCE1)));
    assertThat(
        eventSerializer.convert(json("submit", "\"execution_description\": { "
                                               + "\"docker_image\":\"" + DOCKER_IMAGE + "\","
                                               + "\"docker_args\":[\"foo\",\"bar\"],"
                                               + "\"secret\":{\"name\":\"secret\",\"mount_path\":\"/dev/null\"},"
                                               + "\"commit_sha\":\"" + COMMIT_SHA
                                               + "\"}")),
        is(Event.submit(INSTANCE1, EXECUTION_DESCRIPTION)));
    assertThat(
        eventSerializer.convert(json("submitted", "\"execution_id\":\"" + POD_NAME + "\"")),
        is(Event.submitted(INSTANCE1, POD_NAME)));
    assertThat(
        eventSerializer.convert(json("created", "\"execution_id\":\"" + POD_NAME + "\",\"docker_image\":\"" + DOCKER_IMAGE + "\"")),
        is(Event.created(INSTANCE1, POD_NAME, DOCKER_IMAGE)));
    assertThat(
        eventSerializer.convert(json("runError", "\"message\":\"ErrorMessage\"")),
        is(Event.runError(INSTANCE1, "ErrorMessage")));
    assertThat(
        eventSerializer.convert(json("retryAfter", "\"delay_millis\":12345")),
        is(Event.retryAfter(INSTANCE1, 12345)));
    assertThat(
        eventSerializer.convert(json("triggerExecution", "\"trigger_id\":\"trig\"")),
        is(Event.triggerExecution(INSTANCE1, "trig")));
    assertThat(
        eventSerializer.convert(json("terminate", "\"exit_code\":20")),
        is(Event.terminate(INSTANCE1, 20)));
  }

  @Test
  public void testDeserializeFromJsonWhenTransformationRequired() throws Exception {
    assertThat(
        eventSerializer.convert(json("started", "\"pod_name\":\"" + POD_NAME + "\"")),
        is(Event.started(INSTANCE1))); // for backwards compatibility
    assertThat(
        eventSerializer.convert(json("created", "\"execution_id\":\"" + POD_NAME + "\"")),
        is(Event.created(INSTANCE1, POD_NAME, "UNKNOWN")));
    assertThat(
        eventSerializer.convert(json("triggerExecution")),
        is(Event.triggerExecution(INSTANCE1, "UNKNOWN")));
  }

  private void assertRoundtrip(Event event) {
    ByteString byteString = eventSerializer.convert(event);
    Event deserializedEvent = eventSerializer.convert(byteString);
    Assert.assertThat(deserializedEvent, is(event));
  }

  private ByteString json(String eventType) {
    return ByteString.encodeUtf8(String.format(
        "{\"@type\":\"%s\",\"workflow_instance\":\"%s\"}",
        eventType, INSTANCE1.toKey()));
  }

  private ByteString json(String eventType, String more) {
    return ByteString.encodeUtf8(String.format(
        "{\"@type\":\"%s\",\"workflow_instance\":\"%s\"%s}",
        eventType, INSTANCE1.toKey(), more.isEmpty() ? "" : ("," + more)));
  }
}
