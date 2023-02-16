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

package com.spotify.styx.state;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.isJson;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.withoutJsonPath;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.util.TriggerUtil;
import java.util.List;
import org.junit.Test;

public class StateDataSerializationTest {

  private static final String PAYLOAD_NATURAL_TRIGGER =
      "{"
      + "\"execution_description\": {"
      + "  \"commit_sha\": \"474339ec18d3d04d5d513856bc8ca1d4f1aed03f\","
      + "  \"docker_args\": ["
      + "    \"echo\","
      + "    \"hello\","
      + "    \"world\""
      + "  ],"
      + "  \"docker_image\": \"pipeline-core:474339e\""
      + "},"
      + "\"execution_id\": \"styx-run-12172683-c62f-4f32-899a-63a9741b73f9\","
      + "\"last_exit\": 20,"
      + "\"retry_cost\": 0.4,"
      + "\"retry_delay_millis\": 600000,"
      + "\"tries\": 4,"
      + "\"trigger\":{\"@type\":\"natural\"},"
      + "\"trigger_id\": \"natural-trigger\","
      + "\"messages\": ["
      + "  {\"level\":\"INFO\",\"line\":\"Message 1\"},"
      + "  {\"level\":\"WARNING\",\"line\":\"Message 2\"}"
      + "],"
      + "\"unknown_field\": \"foo\""
      + "}";

  private static final String PAYLOAD_BACKFILL_TRIGGER =
      "{"
      + "\"execution_description\": {"
      + "  \"commit_sha\": \"474339ec18d3d04d5d513856bc8ca1d4f1aed03f\","
      + "  \"docker_args\": ["
      + "    \"echo\","
      + "    \"hello\","
      + "    \"world\""
      + "  ],"
      + "  \"docker_image\": \"pipeline-core:474339e\""
      + "},"
      + "\"execution_id\": \"styx-run-12172683-c62f-4f32-899a-63a9741b73f9\","
      + "\"last_exit\": 20,"
      + "\"retry_cost\": 0.4,"
      + "\"retry_delay_millis\": 600000,"
      + "\"tries\": 4,"
      + "\"trigger\":{\"@type\":\"backfill\",\"trigger_id\":\"backfill-1\"},"
      + "\"trigger_id\": \"backfill-1\","
      + "\"messages\": ["
      + "  {\"level\":\"INFO\",\"line\":\"Message 1\"},"
      + "  {\"level\":\"WARNING\",\"line\":\"Message 2\"}"
      + "],"
      + "\"unknown_field\": \"foo\""
      + "}";

  private static final StateData EXPECTED_NO_TRIGGER = StateData.newBuilder()
      .tries(4)
      .retryDelayMillis(600000L)
      .retryCost(0.4)
      .lastExit(20)
      .executionId("styx-run-12172683-c62f-4f32-899a-63a9741b73f9")
      .executionDescription(ExecutionDescription.builder()
          .dockerImage("pipeline-core:474339e")
          .dockerArgs(List.of("echo", "hello", "world"))
          .commitSha("474339ec18d3d04d5d513856bc8ca1d4f1aed03f")
          .build())
      .addMessage(Message.info("Message 1"))
      .addMessage(Message.warning("Message 2"))
      .build();

  private static final StateData EXPECTED_NATURAL_TRIGGER =
      EXPECTED_NO_TRIGGER.builder()
          .triggerId(TriggerUtil.NATURAL_TRIGGER_ID)
          .trigger(Trigger.natural())
          .build();

  private static final StateData EXPECTED_BACKFILL_TRIGGER =
      EXPECTED_NO_TRIGGER.builder()
          .triggerId("backfill-1")
          .trigger(Trigger.backfill("backfill-1"))
          .build();

  @Test
  public void serializes() throws Exception {
    String jsonNaturalTrigger = Json.OBJECT_MAPPER.writeValueAsString(EXPECTED_NATURAL_TRIGGER);
    String jsonBackfillTrigger = Json.OBJECT_MAPPER.writeValueAsString(EXPECTED_BACKFILL_TRIGGER);

    assertStateDataJsonNoTrigger(jsonNaturalTrigger);
    assertThat(jsonNaturalTrigger, hasJsonPath("trigger_id", equalTo(TriggerUtil.NATURAL_TRIGGER_ID)));
    assertThat(jsonNaturalTrigger, hasJsonPath("trigger.@type", equalTo("natural")));
    assertThat(jsonNaturalTrigger, isJson(withoutJsonPath("trigger.trigger_id")));

    assertStateDataJsonNoTrigger(jsonBackfillTrigger);
    assertThat(jsonBackfillTrigger, hasJsonPath("trigger_id", equalTo("backfill-1")));
    assertThat(jsonBackfillTrigger, hasJsonPath("trigger.@type", equalTo("backfill")));
    assertThat(jsonBackfillTrigger, hasJsonPath("trigger.trigger_id", equalTo("backfill-1")));
  }

  private void assertStateDataJsonNoTrigger(String json) {
    assertThat(json, hasJsonPath("tries", equalTo(4)));
    assertThat(json, hasJsonPath("retry_delay_millis", equalTo(600000)));
    assertThat(json, hasJsonPath("retry_cost", equalTo(0.4)));
    assertThat(json, hasJsonPath("last_exit", equalTo(20)));
    assertThat(json, hasJsonPath("execution_id", equalTo("styx-run-12172683-c62f-4f32-899a-63a9741b73f9")));
    assertThat(json, hasJsonPath("execution_description"));
    assertThat(json, hasJsonPath("messages.[0].level", equalTo("INFO")));
    assertThat(json, hasJsonPath("messages.[0].line", equalTo("Message 1")));
    assertThat(json, hasJsonPath("messages.[1].level", equalTo("WARNING")));
    assertThat(json, hasJsonPath("messages.[1].line", equalTo("Message 2")));
  }

  @Test
  public void deserializes() throws Exception {
    StateData stateData = Json.OBJECT_MAPPER.readValue(PAYLOAD_NATURAL_TRIGGER, StateData.class);
    assertThat(stateData, equalTo(EXPECTED_NATURAL_TRIGGER));

    StateData stateData2 = Json.OBJECT_MAPPER.readValue(PAYLOAD_BACKFILL_TRIGGER, StateData.class);
    assertThat(stateData2, equalTo(EXPECTED_BACKFILL_TRIGGER));
  }

  @Test
  public void deserializesEmptyObject() throws Exception {
    StateData stateData = Json.OBJECT_MAPPER.readValue("{}", StateData.class);

    assertThat(stateData.triggerId(), isEmpty());
    assertThat(stateData.trigger(), isEmpty());
    assertThat(stateData.tries(), equalTo(0));
    assertThat(stateData.lastExit(), isEmpty());
    assertThat(stateData.retryCost(), equalTo(0.0));
    assertThat(stateData.retryDelayMillis(), isEmpty());
    assertThat(stateData.executionId(), isEmpty());
    assertThat(stateData.executionDescription(), isEmpty());
    assertThat(stateData.messages(), is(empty()));
  }

  @Test
  public void deserializesUnknownMessageLevels() throws Exception {
    final Message.MessageLevel level =
        Json.OBJECT_MAPPER.readValue("\"FOOBAR\"", Message.MessageLevel.class);

    assertThat(level, equalTo(Message.MessageLevel.UNKNOWN));
  }
}
