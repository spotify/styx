/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2020 Spotify AB
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

package com.spotify.styx.model.data;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.model.FlyteIdentifier;
import com.spotify.styx.serialization.Json;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import okio.ByteString;
import org.junit.Test;

public class ExecutionTest {

  private static final Instant INSTANT = LocalDate.of(2020, 8, 20)
      .atStartOfDay(ZoneOffset.UTC)
      .toInstant();
  private static final List<ExecStatus> STATUS_LIST = List.of(ExecStatus.create(INSTANT, "0", Optional.of("ok")));
  private static final Execution DOCKER_EXECUTION = Execution.create(
      Optional.of("123"),
      Optional.of("busybox"),
      Optional.of("abc"),
      Optional.empty(),
      Optional.of("1"),
      STATUS_LIST,
      Optional.empty()
  );
  private static final FlyteExecConf FLYTE_EXEC_CONF = FlyteExecConf.builder()
      .referenceId(FlyteIdentifier.builder()
          .resourceType("lp")
          .project("flytetest")
          .domain("production")
          .name("GoldenPathWF")
          .version("1")
          .build())
      .build();
  private static final Execution FLYTE_EXECUTION = Execution.create(
      Optional.of("123"),
      Optional.empty(),
      Optional.empty(),
      Optional.of(FLYTE_EXEC_CONF),
      Optional.of("1"),
      STATUS_LIST,
      Optional.of("123")
  );

  @Test
  public void shouldParseDockerExecutions() throws IOException {
    String json = "{"
                  + "\"execution_id\":\"123\","
                  + "\"docker_image\":\"busybox\","
                  + "\"commit_sha\":\"abc\","
                  + "\"runner_id\":\"1\","
                  + "\"statuses\":[{\"timestamp\":\"2020-08-20T00:00:00.0Z\",\"status\":\"0\",\"message\":\"ok\"}]"
                  + "}";

    var actualExecution = Json.deserialize(ByteString.encodeUtf8(json), Execution.class);

    assertEquals(DOCKER_EXECUTION, actualExecution);
  }

  @Test
  public void shouldParseFlyteExecutions() throws IOException {
    String json = "{"
                  + "\"execution_id\":\"123\","
                  + "\"flyte_exec_conf\":{"
                  + "\"reference_id\":{"
                  + "\"resource_type\":\"lp\","
                  + "\"project\":\"flytetest\","
                  + "\"domain\":\"production\","
                  + "\"name\":\"GoldenPathWF\","
                  + "\"version\":\"1\""
                  + "},"
                  + "\"input_fields\":{}"
                  + "},"
                  + "\"runner_id\":\"1\","
                  + "\"flyte_execution_id\":\"123\","
                  + "\"statuses\":[{\"timestamp\":\"2020-08-20T00:00:00.0Z\",\"status\":\"0\",\"message\":\"ok\"}]}";
    var expectedExecution = Execution.create(
        Optional.of("123"),
        Optional.empty(),
        Optional.empty(),
        Optional.of(FLYTE_EXEC_CONF),
        Optional.of("1"),
        STATUS_LIST,
        Optional.of("123")
    );

    var actualExecution = Json.deserialize(ByteString.encodeUtf8(json), Execution.class);

    assertEquals(expectedExecution, actualExecution);
  }

  @Test
  public void shouldNotBePossibleToCreateExecutionForDockerAndFlyte() {
    var exception = assertThrows(IllegalArgumentException.class, () ->
        Execution.create(
            Optional.of("123"),
            Optional.of("busybox"),
            Optional.of("1"),
            Optional.of(FLYTE_EXEC_CONF),
            Optional.of("1"),
            STATUS_LIST,
            Optional.of("123")
        )
    );

    assertEquals(
        "Conflicting configuration: Both docker image and flyte conf specified for exec id: 123",
        exception.getMessage()
    );
  }
}
