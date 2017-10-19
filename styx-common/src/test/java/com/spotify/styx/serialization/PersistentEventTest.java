/*-
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

package com.spotify.styx.serialization;

import static com.spotify.styx.serialization.Json.deserializeEvent;
import static com.spotify.styx.serialization.Json.serialize;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.Trigger;
import java.util.Optional;
import okio.ByteString;
import org.junit.Assert;
import org.junit.Test;

public class PersistentEventTest {

  private static final WorkflowId WORKFLOW1 = WorkflowId.create("component", "endpoint1");
  private static final String PARAMETER1 = "2016-01-01";
  private static final Trigger UNKNOWN_TRIGGER = Trigger.unknown("trig");
  private static final Trigger NATURAL_TRIGGER1 = Trigger.natural();
  private static final Trigger ADHOC_TRIGGER2 = Trigger.adhoc("trig2");
  private static final Trigger BACKFILL_TRIGGER3 = Trigger.backfill("trig3");
  private static final Trigger TRIGGER_UNKNOWN = Trigger.unknown("UNKNOWN");
  private static final WorkflowInstance INSTANCE1 = WorkflowInstance.create(WORKFLOW1, PARAMETER1);
  private static final String POD_NAME = "test-event";
  private static final String DOCKER_IMAGE = "busybox:1.1";
  private static final String COMMIT_SHA = "00000ef508c1cb905e360590ce3e7e9193f6b370";
  private static final ExecutionDescription EXECUTION_DESCRIPTION = ExecutionDescription.builder()
      .dockerImage(DOCKER_IMAGE)
      .dockerArgs("foo", "bar")
      .secret(WorkflowConfiguration.Secret.create("secret", "/dev/null"))
      .commitSha(COMMIT_SHA)
      .build();

  @Test
  public void testRoundtripAllEvents() throws Exception {
    assertRoundtrip(Event.timeTrigger(INSTANCE1));
    assertRoundtrip(Event.triggerExecution(INSTANCE1, UNKNOWN_TRIGGER));
    assertRoundtrip(Event.info(INSTANCE1, Message.info("InfoMessage")));
    assertRoundtrip(Event.created(INSTANCE1, POD_NAME, DOCKER_IMAGE));
    assertRoundtrip(Event.dequeue(INSTANCE1));
    assertRoundtrip(Event.started(INSTANCE1));
    assertRoundtrip(Event.terminate(INSTANCE1, Optional.of(20)));
    assertRoundtrip(Event.runError(INSTANCE1, "ErrorMessage"));
    assertRoundtrip(Event.success(INSTANCE1));
    assertRoundtrip(Event.retryAfter(INSTANCE1, 12345));
    assertRoundtrip(Event.retry(INSTANCE1));
    assertRoundtrip(Event.stop(INSTANCE1));
    assertRoundtrip(Event.timeout(INSTANCE1));
    assertRoundtrip(Event.halt(INSTANCE1));
    assertRoundtrip(Event.submit(INSTANCE1, EXECUTION_DESCRIPTION, POD_NAME));
    assertRoundtrip(Event.submitted(INSTANCE1, POD_NAME));
  }

  @Test
  public void testDeserializeFromJson() throws Exception {
    assertThat(deserializeEvent(json("timeTrigger")), is(Event.timeTrigger(INSTANCE1)));
    assertThat(deserializeEvent(json("dequeue")), is(Event.dequeue(INSTANCE1)));
    assertThat(deserializeEvent(json("started")), is(Event.started(INSTANCE1)));
    assertThat(deserializeEvent(json("success")), is(Event.success(INSTANCE1)));
    assertThat(deserializeEvent(json("retry")), is(Event.retry(INSTANCE1)));
    assertThat(deserializeEvent(json("stop")), is(Event.stop(INSTANCE1)));
    assertThat(deserializeEvent(json("timeout")), is(Event.timeout(INSTANCE1)));
    assertThat(deserializeEvent(json("halt")), is(Event.halt(INSTANCE1)));
    assertThat(deserializeEvent(json("submit", "\"execution_description\": { "
                                               + "\"docker_image\":\"" + DOCKER_IMAGE + "\","
                                               + "\"docker_args\":[\"foo\",\"bar\"],"
                                               + "\"secret\":{\"name\":\"secret\",\"mount_path\":\"/dev/null\"},"
                                               + "\"commit_sha\":\"" + COMMIT_SHA
                                               + "\"}")),
        is(Event.submit(INSTANCE1, EXECUTION_DESCRIPTION, null)));
    assertThat(deserializeEvent(json("submit", "\"execution_description\": { "
                                               + "\"docker_image\":\"" + DOCKER_IMAGE + "\","
                                               + "\"docker_args\":[\"foo\",\"bar\"],"
                                               + "\"secret\":{\"name\":\"secret\",\"mount_path\":\"/dev/null\"},"
                                               + "\"commit_sha\":\"" + COMMIT_SHA
                                               + "\"}, "
                                               + "\"execution_id\": \"" + POD_NAME + "\"")),
        is(Event.submit(INSTANCE1, EXECUTION_DESCRIPTION, POD_NAME)));
    assertThat(
        deserializeEvent(json("info", "\"message\":{\"line\":\"InfoMessage\",\"level\":\"INFO\"}")),
        is(Event.info(INSTANCE1, Message.info("InfoMessage"))));
    assertThat(
        deserializeEvent(json("submitted")),
        is(Event.submitted(INSTANCE1, null)));
    assertThat(
        deserializeEvent(json("submitted", "\"execution_id\":\"" + POD_NAME + "\"")),
        is(Event.submitted(INSTANCE1, POD_NAME)));
    assertThat(
        deserializeEvent(json("created", "\"execution_id\":\"" + POD_NAME + "\",\"docker_image\":\"" + DOCKER_IMAGE + "\"")),
        is(Event.created(INSTANCE1, POD_NAME, DOCKER_IMAGE)));
    assertThat(
        deserializeEvent(json("runError", "\"message\":\"ErrorMessage\"")),
        is(Event.runError(INSTANCE1, "ErrorMessage")));
    assertThat(
        deserializeEvent(json("retryAfter", "\"delay_millis\":12345")),
        is(Event.retryAfter(INSTANCE1, 12345)));
    assertThat(
        deserializeEvent(json("triggerExecution", "\"trigger\":{\"@type\":\"natural\"}")),
        is(Event.triggerExecution(INSTANCE1, NATURAL_TRIGGER1)));
    assertThat(
        deserializeEvent(json("triggerExecution", "\"trigger\":{\"@type\":\"adhoc\",\"trigger_id\":\"trig2\"}")),
        is(Event.triggerExecution(INSTANCE1, ADHOC_TRIGGER2)));
    assertThat(
        deserializeEvent(json("triggerExecution", "\"trigger\":{\"@type\":\"backfill\",\"trigger_id\":\"trig3\"}")),
        is(Event.triggerExecution(INSTANCE1, BACKFILL_TRIGGER3)));
    assertThat(
        deserializeEvent(json("terminate", "\"exit_code\":20")),
        is(Event.terminate(INSTANCE1, Optional.of(20))));
  }

  @Test
  public void testDeserializeFromJsonWhenTransformationRequired() throws Exception {
    assertThat(
        deserializeEvent(json("triggerExecution", "\"trigger_id\":\"trig\"")),
        is(Event.triggerExecution(INSTANCE1, UNKNOWN_TRIGGER)));
    assertThat(
        deserializeEvent(json("started", "\"pod_name\":\"" + POD_NAME + "\"")),
        is(Event.started(INSTANCE1))); // for backwards compatibility
    assertThat(
        deserializeEvent(json("created", "\"execution_id\":\"" + POD_NAME + "\"")),
        is(Event.created(INSTANCE1, POD_NAME, "UNKNOWN")));
    assertThat(
        deserializeEvent(json("triggerExecution")),
        is(Event.triggerExecution(INSTANCE1, TRIGGER_UNKNOWN)));
  }

  private void assertRoundtrip(Event event) throws Exception {
    ByteString byteString = serialize(event);
    Event deserializedEvent = deserializeEvent(byteString);
    Assert.assertThat(
        "serialized event did not match actual event after deserialization: " + byteString.utf8(),
        deserializedEvent, is(event));
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
