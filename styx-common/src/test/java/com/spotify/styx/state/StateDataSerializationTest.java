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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.styx.model.DataEndpoint;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.util.Json;
import java.util.Arrays;
import java.util.Optional;
import org.junit.Test;

public class StateDataSerializationTest {

  private static final String PAYLOAD =
      "{"
      + "\"execution_description\": {"
      + "  \"commit_sha\": \"474339ec18d3d04d5d513856bc8ca1d4f1aed03f\","
      + "  \"docker_args\": ["
      + "    \"echo\","
      + "    \"hello\","
      + "    \"world\""
      + "  ],"
      + "  \"docker_image\": \"pipeline-core:474339e\","
      + "  \"secret\": {"
      + "    \"mount_path\": \"/etc/keys\","
      + "    \"name\": \"pipeline-core-secret\""
      + "  }"
      + "},"
      + "\"execution_id\": \"styx-run-12172683-c62f-4f32-899a-63a9741b73f9\","
      + "\"last_exit\": 20,"
      + "\"retry_cost\": 0.4,"
      + "\"retry_delay_millis\": 600000,"
      + "\"tries\": 4,"
      + "\"trigger_id\": \"natural-trigger\","
      + "\"messages\": ["
      + "  {\"level\":\"INFO\",\"line\":\"Message 1\"},"
      + "  {\"level\":\"WARNING\",\"line\":\"Message 2\"}"
      + "],"
      + "\"unknown_field\": \"foo\""
      + "}";

  private static final StateData EXPECTED = StateData.newBuilder()
      .triggerId("natural-trigger")
      .tries(4)
      .retryDelayMillis(600000L)
      .retryCost(0.4)
      .lastExit(20)
      .executionId("styx-run-12172683-c62f-4f32-899a-63a9741b73f9")
      .executionDescription(
          ExecutionDescription.create(
              "pipeline-core:474339e",
              Arrays.asList("echo", "hello", "world"),
              Optional.of(DataEndpoint.Secret.create("pipeline-core-secret", "/etc/keys")),
              Optional.of("474339ec18d3d04d5d513856bc8ca1d4f1aed03f")
          )
      )
      .addMessage(Message.info("Message 1"))
      .addMessage(Message.warning("Message 2"))
      .build();

  @Test
  public void serializes() throws Exception {
    String json = Json.OBJECT_MAPPER.writeValueAsString(EXPECTED);

    assertThat(json, hasJsonPath("trigger_id", equalTo("natural-trigger")));
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
    StateData stateData = Json.OBJECT_MAPPER.readValue(PAYLOAD, StateData.class);
    assertThat(stateData, equalTo(EXPECTED));
  }

  @Test
  public void deserializesEmptyObject() throws Exception {
    StateData stateData = Json.OBJECT_MAPPER.readValue("{}", StateData.class);

    assertThat(stateData.tries(), equalTo(0));
    assertThat(stateData.lastExit(), isEmpty());
    assertThat(stateData.retryCost(), equalTo(0.0));
    assertThat(stateData.retryDelayMillis(), isEmpty());
    assertThat(stateData.trigger(), isEmpty());
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
