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

package com.spotify.styx.model.data.deprecated;

import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.data.ExecStatus;
import com.spotify.styx.model.data.Execution;
import com.spotify.styx.model.data.Trigger;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import org.junit.Test;

@Deprecated
public class WorkflowInstanceExecutionDataTest {

  @Test
  public void shouldDeserializeExecStatus() throws Exception {
    String json = statusJson("09:56", "STARTED");
    ExecStatus executionStatus = OBJECT_MAPPER.readValue(json, ExecStatus.class);

    assertThat(executionStatus.timestamp(), is(Instant.parse("2016-08-03T09:56:03.607Z")));
    assertThat(executionStatus.status(), is("STARTED"));
  }

  @Test
  public void shouldDeserializeExecution() throws Exception {
    String json = executionJson("exec-id", "busybox:1.0", "09", "SUCCESS");

    Execution execution = OBJECT_MAPPER.readValue(json, Execution.class);
    Execution expected = Execution.create(
        Optional.of("exec-id"),
        Optional.of("busybox:1.0"),
        Arrays.asList(
            ExecStatus.create(Instant.parse("2016-08-03T09:56:03.607Z"), "STARTED"),
            ExecStatus.create(Instant.parse("2016-08-03T09:57:03.607Z"), "RUNNING"),
            ExecStatus.create(Instant.parse("2016-08-03T09:58:03.607Z"), "SUCCESS")
        )
    );
    assertThat(execution, is(expected));
  }

  @Test
  public void shouldDeserializeTrigger() throws Exception {
    String jsonExec0 = executionJson("exec-id-0", "busybox:1.0", "09", "FAILED");
    String jsonExec1 = executionJson("exec-id-1", "busybox:1.1", "10", "SUCCESS");

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
        true,
        Arrays.asList(
            Execution.create(
                Optional.of("exec-id-0"),
                Optional.of("busybox:1.0"),
                Arrays.asList(
                    ExecStatus.create(Instant.parse("2016-08-03T09:56:03.607Z"), "STARTED"),
                    ExecStatus.create(Instant.parse("2016-08-03T09:57:03.607Z"), "RUNNING"),
                    ExecStatus.create(Instant.parse("2016-08-03T09:58:03.607Z"), "FAILED")
                )
            ),
            Execution.create(
                Optional.of("exec-id-1"),
                Optional.of("busybox:1.1"),
                Arrays.asList(
                    ExecStatus.create(Instant.parse("2016-08-03T10:56:03.607Z"), "STARTED"),
                    ExecStatus.create(Instant.parse("2016-08-03T10:57:03.607Z"), "RUNNING"),
                    ExecStatus.create(Instant.parse("2016-08-03T10:58:03.607Z"), "SUCCESS")
                )
            )
        )
    );
    assertThat(trigger, is(expected));
  }

  @Test
  public void shouldDeserializeExecutionData() throws Exception {
    String jsonExec00 = executionJson("exec-id-00", "busybox:1.0", "07", "FAILED");
    String jsonExec01 = executionJson("exec-id-01", "busybox:1.1", "08", "SUCCESS");

    String jsonTrigger0 =
        "{"
        + "\"trigger_id\":\"trig-0\","
        + "\"timestamp\":\"" + time("07:55") + "\","
        + "\"complete\":true,"
        + "\"executions\":["
        + jsonExec00 + "," + jsonExec01
        + "]}";

    String jsonExec10 = executionJson("exec-id-10", "busybox:1.2", "09", "FAILED");
    String jsonExec11 = executionJson("exec-id-11", "busybox:1.3", "10", "SUCCESS");

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

    com.spotify.styx.model.data.WorkflowInstanceExecutionData
        executionData = OBJECT_MAPPER.readValue(json, WorkflowInstanceExecutionData.class);
    com.spotify.styx.model.data.WorkflowInstanceExecutionData
        expected = WorkflowInstanceExecutionData.create(
        WorkflowInstance.parseKey("component1#endpoint1#2016-08-03T06"),
        Arrays.asList(
            Trigger.create(
                "trig-0",
                time("07:55"),
                true,
                Arrays.asList(
                    Execution.create(
                        Optional.of("exec-id-00"),
                        Optional.of("busybox:1.0"),
                        Arrays.asList(
                            ExecStatus.create(Instant.parse("2016-08-03T07:56:03.607Z"), "STARTED"),
                            ExecStatus.create(Instant.parse("2016-08-03T07:57:03.607Z"), "RUNNING"),
                            ExecStatus.create(Instant.parse("2016-08-03T07:58:03.607Z"), "FAILED")
                        )
                    ),
                    Execution.create(
                        Optional.of("exec-id-01"),
                        Optional.of("busybox:1.1"),
                        Arrays.asList(
                            ExecStatus.create(Instant.parse("2016-08-03T08:56:03.607Z"), "STARTED"),
                            ExecStatus.create(Instant.parse("2016-08-03T08:57:03.607Z"), "RUNNING"),
                            ExecStatus.create(Instant.parse("2016-08-03T08:58:03.607Z"), "SUCCESS")
                        )
                    )
                )
            ),
            Trigger.create(
                "trig-1",
                time("09:55"),
                false,
                Arrays.asList(
                    Execution.create(
                        Optional.of("exec-id-10"),
                        Optional.of("busybox:1.2"),
                        Arrays.asList(
                            ExecStatus.create(Instant.parse("2016-08-03T09:56:03.607Z"), "STARTED"),
                            ExecStatus.create(Instant.parse("2016-08-03T09:57:03.607Z"), "RUNNING"),
                            ExecStatus.create(Instant.parse("2016-08-03T09:58:03.607Z"), "FAILED")
                        )
                    ),
                    Execution.create(
                        Optional.of("exec-id-11"),
                        Optional.of("busybox:1.3"),
                        Arrays.asList(
                            ExecStatus.create(Instant.parse("2016-08-03T10:56:03.607Z"), "STARTED"),
                            ExecStatus.create(Instant.parse("2016-08-03T10:57:03.607Z"), "RUNNING"),
                            ExecStatus.create(Instant.parse("2016-08-03T10:58:03.607Z"), "SUCCESS")
                        )
                    )
                )
            )
        )
    );

    assertThat(executionData, is(expected));
  }

  private String statusJson(String time, String status) {
    return "{\"timestamp\":\"2016-08-03T" + time + ":03.607Z\", \"status\":\"" + status + "\"}";
  }

  private String executionJson(String id, String image, String hour, String endStatus) {
    String jsonStatus1 = statusJson(hour + ":56", "STARTED");
    String jsonStatus2 = statusJson(hour + ":57", "RUNNING");
    String jsonStatus3 = statusJson(hour + ":58", endStatus);

    return
        "{"
        + "\"execution_id\":\"" + id + "\","
        + "\"docker_image\":\"" + image + "\","
        + "\"statuses\":["
        + jsonStatus1 + "," + jsonStatus2 + "," + jsonStatus3
        + "]}";
  }

  private static Instant time(String time) {
    return Instant.parse("2016-08-03T" + time + ":03.607Z");
  }
}
