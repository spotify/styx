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

package com.spotify.styx.model.data;

import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.WorkflowInstance;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class WorkflowInstanceExecutionDataTest {
  
  private static final String TEST_RUNNER_ID = "test";

  @Test
  public void shouldDeserializeExecStatus() throws Exception {
    String json = statusJson("09:56", "STARTED", Optional.empty());
    ExecStatus executionStatus = OBJECT_MAPPER.readValue(json, ExecStatus.class);

    assertThat(executionStatus.timestamp(), is(Instant.parse("2016-08-03T09:56:03.607Z")));
    assertThat(executionStatus.status(), is("STARTED"));
  }

  @Test
  public void shouldDeserializeExecution() throws Exception {
    String json =
        executionJson("exec-id", "busybox:1.0", "commit-sha", TEST_RUNNER_ID, "09", "SUCCESS", Optional.empty());

    Execution execution = OBJECT_MAPPER.readValue(json, Execution.class);
    Execution expected = Execution.create(
        Optional.of("exec-id"),
        Optional.of("busybox:1.0"),
        Optional.of("commit-sha"),
        Optional.of(TEST_RUNNER_ID),
        Arrays.asList(
            ExecStatus.create(Instant.parse("2016-08-03T09:56:03.607Z"), "STARTED", Optional.empty()),
            ExecStatus.create(Instant.parse("2016-08-03T09:57:03.607Z"), "RUNNING", Optional.empty()),
            ExecStatus.create(Instant.parse("2016-08-03T09:58:03.607Z"), "SUCCESS", Optional.empty())
        )
    );
    assertThat(execution, is(expected));
  }

  @Test
  public void shouldDeserializeExecutionNoOptionalFields() throws Exception {
    String json = "{"
                  + "\"statuses\":["
                  + statusJson("16:56", "HALTED", Optional.empty())
                  + "]}";

    Execution execution = OBJECT_MAPPER.readValue(json, Execution.class);
    Execution expected = Execution.create(
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        List.of(
            ExecStatus.create(Instant.parse("2016-08-03T16:56:03.607Z"), "HALTED", Optional.empty())
        )
    );
    assertThat(execution, is(expected));
  }

  @Test
  public void shouldDeserializeTrigger() throws Exception {
    String jsonExec0 = executionJson("exec-id-0", "busybox:1.0", "commit-sha0", TEST_RUNNER_ID, "09", "FAILED",
        Optional.of("Exit code 1"));
    String jsonExec1 = executionJson("exec-id-1", "busybox:1.1","commit-sha1", TEST_RUNNER_ID, "10", "SUCCESS",
        Optional.empty());

    String json =
        "{"
        + "\"trigger_id\":\"trig-0\","
        + "\"timestamp\":\"" + time("07:55") + "\","
        + "\"complete\":true,"
        + "\"executions\":["
        + jsonExec0 + "," + jsonExec1
        + "]}";

    Trigger trigger = OBJECT_MAPPER.readValue(json, Trigger.class);
    Trigger expected = Trigger.create(
        "trig-0",
        time("07:55"),
        TriggerParameters.zero(),
        true,
        Arrays.asList(
            Execution.create(
                Optional.of("exec-id-0"),
                Optional.of("busybox:1.0"),
                Optional.of("commit-sha0"),
                Optional.of("test"),
                Arrays.asList(
                    ExecStatus.create(Instant.parse("2016-08-03T09:56:03.607Z"), "STARTED", Optional.empty()),
                    ExecStatus.create(Instant.parse("2016-08-03T09:57:03.607Z"), "RUNNING", Optional.empty()),
                    ExecStatus.create(Instant.parse("2016-08-03T09:58:03.607Z"), "FAILED", Optional.of("Exit code 1"))
                )
            ),
            Execution.create(
                Optional.of("exec-id-1"),
                Optional.of("busybox:1.1"),
                Optional.of("commit-sha1"),
                Optional.of("test"),
                Arrays.asList(
                    ExecStatus.create(Instant.parse("2016-08-03T10:56:03.607Z"), "STARTED", Optional.empty()),
                    ExecStatus.create(Instant.parse("2016-08-03T10:57:03.607Z"), "RUNNING", Optional.empty()),
                    ExecStatus.create(Instant.parse("2016-08-03T10:58:03.607Z"), "SUCCESS", Optional.empty())
                )
            )
        )
    );
    assertThat(trigger, is(expected));
  }

  @Test
  public void shouldDeserializeTriggerWithParameters() throws Exception {
    String jsonExec0 = executionJson("exec-id-0", "busybox:1.0","commit-sha0", TEST_RUNNER_ID, "09", "FAILED",
        Optional.of("Exit code 1"));
    String jsonExec1 = executionJson("exec-id-1", "busybox:1.1","commit-sha1", TEST_RUNNER_ID, "10", "SUCCESS",
        Optional.empty());

    String json =
        "{"
        + "\"trigger_id\":\"trig-0\","
        + "\"timestamp\":\"" + time("07:55") + "\","
        + "\"parameters\":{\"env\":{\"FOO\":\"foo\",\"BAR\":\"bar\"}},"
        + "\"complete\":true,"
        + "\"executions\":["
        + jsonExec0 + "," + jsonExec1
        + "]}";

    Trigger trigger = OBJECT_MAPPER.readValue(json, Trigger.class);
    Trigger expected = Trigger.create(
        "trig-0",
        time("07:55"),
        TriggerParameters.builder()
            .env("FOO", "foo",
                "BAR", "bar")
            .build(),
        true,
        Arrays.asList(
            Execution.create(
                Optional.of("exec-id-0"),
                Optional.of("busybox:1.0"),
                Optional.of("commit-sha0"),
                Optional.of("test"),
                Arrays.asList(
                    ExecStatus.create(Instant.parse("2016-08-03T09:56:03.607Z"), "STARTED", Optional.empty()),
                    ExecStatus.create(Instant.parse("2016-08-03T09:57:03.607Z"), "RUNNING", Optional.empty()),
                    ExecStatus.create(Instant.parse("2016-08-03T09:58:03.607Z"), "FAILED", Optional.of("Exit code 1"))
                )
            ),
            Execution.create(
                Optional.of("exec-id-1"),
                Optional.of("busybox:1.1"),
                Optional.of("commit-sha1"),
                Optional.of("test"),
                Arrays.asList(
                    ExecStatus.create(Instant.parse("2016-08-03T10:56:03.607Z"), "STARTED", Optional.empty()),
                    ExecStatus.create(Instant.parse("2016-08-03T10:57:03.607Z"), "RUNNING", Optional.empty()),
                    ExecStatus.create(Instant.parse("2016-08-03T10:58:03.607Z"), "SUCCESS", Optional.empty())
                )
            )
        )
    );
    assertThat(trigger, is(expected));
  }

  @Test
  public void shouldDeserializeExecutionData() throws Exception {
    String jsonExec00 = executionJson("exec-id-00", "busybox:1.0","commit-sha0", TEST_RUNNER_ID, "07", "FAILED",
        Optional.of("Exit code 1"));
    String jsonExec01 = executionJson("exec-id-01", "busybox:1.1","commit-sha1", TEST_RUNNER_ID, "08", "SUCCESS",
        Optional.empty());

    String jsonTrigger0 =
        "{"
        + "\"trigger_id\":\"trig-0\","
        + "\"timestamp\":\"" + time("07:55") + "\","
        + "\"complete\":true,"
        + "\"executions\":["
        + jsonExec00 + "," + jsonExec01
        + "]}";

    String jsonExec10 = executionJson("exec-id-10", "busybox:1.2","commit-sha2", TEST_RUNNER_ID, "09", "FAILED",
        Optional.of("Exit code 1"));
    String jsonExec11 = executionJson("exec-id-11", "busybox:1.3","commit-sha3", TEST_RUNNER_ID, "10", "SUCCESS",
        Optional.empty());

    String jsonTrigger1 =
        "{"
        + "\"trigger_id\":\"trig-1\","
        + "\"timestamp\":\"" + time("09:55") + "\","
        + "\"complete\":false,"
        + "\"executions\":["
        + jsonExec10 + "," + jsonExec11
        + "]}";

    String json =
        "{"
        + "\"workflow_instance\":{"
          + "\"workflow_id\":{"
            + "\"component_id\":\"component1\","
            + "\"id\":\"endpoint1\""
          + "},"
          + "\"parameter\":\"2016-08-03T06\""
        + "},"
        + "\"triggers\":["
        + jsonTrigger0 + "," + jsonTrigger1
        + "]}";

    WorkflowInstanceExecutionData
        executionData = OBJECT_MAPPER.readValue(json, WorkflowInstanceExecutionData.class);
    WorkflowInstanceExecutionData expected = WorkflowInstanceExecutionData.create(
        WorkflowInstance.parseKey("component1#endpoint1#2016-08-03T06"),
        Arrays.asList(
            Trigger.create(
                "trig-0",
                time("07:55"),
                TriggerParameters.zero(),
                true,
                Arrays.asList(
                    Execution.create(
                        Optional.of("exec-id-00"),
                        Optional.of("busybox:1.0"),
                        Optional.of("commit-sha0"),
                        Optional.of("test"),
                        Arrays.asList(
                            ExecStatus.create(Instant.parse("2016-08-03T07:56:03.607Z"), "STARTED", Optional.empty()),
                            ExecStatus.create(Instant.parse("2016-08-03T07:57:03.607Z"), "RUNNING", Optional.empty()),
                            ExecStatus.create(Instant.parse("2016-08-03T07:58:03.607Z"), "FAILED", Optional.of("Exit code 1"))
                        )
                    ),
                    Execution.create(
                        Optional.of("exec-id-01"),
                        Optional.of("busybox:1.1"),
                        Optional.of("commit-sha1"),
                        Optional.of("test"),
                        Arrays.asList(
                            ExecStatus.create(Instant.parse("2016-08-03T08:56:03.607Z"), "STARTED", Optional.empty()),
                            ExecStatus.create(Instant.parse("2016-08-03T08:57:03.607Z"), "RUNNING", Optional.empty()),
                            ExecStatus.create(Instant.parse("2016-08-03T08:58:03.607Z"), "SUCCESS", Optional.empty())
                        )
                    )
                )
            ),
            Trigger.create(
                "trig-1",
                time("09:55"),
                TriggerParameters.zero(),
                false,
                Arrays.asList(
                    Execution.create(
                        Optional.of("exec-id-10"),
                        Optional.of("busybox:1.2"),
                        Optional.of("commit-sha2"),
                        Optional.of("test"),
                        Arrays.asList(
                            ExecStatus.create(Instant.parse("2016-08-03T09:56:03.607Z"), "STARTED", Optional.empty()),
                            ExecStatus.create(Instant.parse("2016-08-03T09:57:03.607Z"), "RUNNING", Optional.empty()),
                            ExecStatus.create(Instant.parse("2016-08-03T09:58:03.607Z"), "FAILED", Optional.of("Exit code 1"))
                        )
                    ),
                    Execution.create(
                        Optional.of("exec-id-11"),
                        Optional.of("busybox:1.3"),
                        Optional.of("commit-sha3"),
                        Optional.of("test"),
                        Arrays.asList(
                            ExecStatus.create(Instant.parse("2016-08-03T10:56:03.607Z"), "STARTED", Optional.empty()),
                            ExecStatus.create(Instant.parse("2016-08-03T10:57:03.607Z"), "RUNNING", Optional.empty()),
                            ExecStatus.create(Instant.parse("2016-08-03T10:58:03.607Z"), "SUCCESS", Optional.empty())
                        )
                    )
                )
            )
        )
    );

    assertThat(executionData, is(expected));
  }

  private String statusJson(String time, String status, Optional<String> message) {
    final String baseJson = "{\"timestamp\":\"2016-08-03T" + time + ":03.607Z\", \"status\":\"" + status + "\"";
    return message.map(s -> baseJson + ", \"message\":\"" + s + "\"}").orElse(baseJson + "}");
  }

  private String executionJson(String id, String image, String sha, String runner_id, String hour, String endStatus,
                               Optional<String> endMessage) {
    String jsonStatus1 = statusJson(hour + ":56", "STARTED", Optional.empty());
    String jsonStatus2 = statusJson(hour + ":57", "RUNNING", Optional.empty());
    String jsonStatus3 = statusJson(hour + ":58", endStatus, endMessage);

    return
        "{"
        + "\"execution_id\":\"" + id + "\","
        + "\"docker_image\":\"" + image + "\","
        + "\"commit_sha\":\"" + sha + "\","
        + "\"runner_id\":\"" + runner_id + "\","
        + "\"statuses\":["
        + jsonStatus1 + "," + jsonStatus2 + "," + jsonStatus3
        + "]}";
  }

  private static Instant time(String time) {
    return Instant.parse("2016-08-03T" + time + ":03.607Z");
  }
}
