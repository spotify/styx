/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
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

package com.spotify.styx.cli;

import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.api.RunStateDataPayload.RunStateData;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.StateData;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class JsonCliOutputTest {
  
  private static final Backfill BACKFILL = Backfill.newBuilder()
      .id("backfill-2")
      .start(Instant.parse("2017-01-01T00:00:00Z"))
      .end(Instant.parse("2017-01-02T00:00:00Z"))
      .workflowId(WorkflowId.create("component", "workflow2"))
      .concurrency(2)
      .description("Description")
      .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
      .schedule(Schedule.DAYS)
      .build();
  
  private static final String EXPECTED_OUTPUT = "{\"id\":\"backfill-2\"," 
                                               + "\"start\":\"2017-01-01T00:00:00Z\"," 
                                               + "\"end\":\"2017-01-02T00:00:00Z\"," 
                                               + "\"workflow_id\":" 
                                               + "{\"component_id\":\"component\"," 
                                               + "\"id\":\"workflow2\"}," 
                                               + "\"concurrency\":2," 
                                               + "\"description\":\"Description\"," 
                                               + "\"next_trigger\":\"2017-01-01T00:00:00Z\"," 
                                               + "\"schedule\":\"days\"," 
                                               + "\"all_triggered\":false," 
                                               + "\"halted\":false}";

  private static final String EXPECTED_OUTPUT_WITH_STATUS = "{\"backfill\":"
                                                            + EXPECTED_OUTPUT
                                                            + ",\"statuses\":null}";

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

  private PrintStream old;

  private CliOutput cliOutput;

  @Before
  public void setUp() {
    old = System.out;
    System.setOut(new PrintStream(outContent));
    cliOutput = new JsonCliOutput();
  }

  @After
  public void tearDown() {
    System.setOut(old);
  }

  @Test
  public void shouldPrintBackfill() {
    cliOutput.printBackfill(BACKFILL, true);
    assertEquals(EXPECTED_OUTPUT + "\n", outContent.toString());
  }

  @Test
  public void shouldPrintBackfills() {
    cliOutput.printBackfills(
        ImmutableList.of(BackfillPayload.create(BACKFILL, Optional.empty())),
        true);
    assertEquals("[" + EXPECTED_OUTPUT_WITH_STATUS + "]\n", outContent.toString());
  }

  @Test
  public void shouldPrintBackfillPayload() {
    cliOutput.printBackfillPayload(BackfillPayload.create(BACKFILL, Optional.empty()), true);
    assertEquals(EXPECTED_OUTPUT_WITH_STATUS + "\n", outContent.toString());
  }

  @Test
  public void shouldPrintStatesKeyedOnWorkflowIdKey() throws IOException {
    final WorkflowId fooWorkflowId = WorkflowId.create("foo-component", "foo-workflow");
    final WorkflowId barWorkflowId = WorkflowId.create("bar-component", "bar-workflow");
    final WorkflowInstance fooWorkflowInstance = WorkflowInstance.create(fooWorkflowId, "foo-param");
    final WorkflowInstance barWorkflowInstance = WorkflowInstance.create(barWorkflowId, "bar-param");
    final RunStateData fooRunStateData = RunStateData.create(fooWorkflowInstance, "PREPARE",
        StateData.newBuilder().executionId("foo-e").build());
    final RunStateData barRunStateData = RunStateData.create(barWorkflowInstance, "RUNNING",
        StateData.newBuilder().executionId("bar-e").build());
    cliOutput.printStates(RunStateDataPayload.create(ImmutableList.of(fooRunStateData, barRunStateData)));
    final Map<String, List<RunStateData>> expectedOutput = ImmutableMap.of(
        fooWorkflowId.toKey(), ImmutableList.of(fooRunStateData),
        barWorkflowId.toKey(), ImmutableList.of(barRunStateData));
    final Map<String, List<RunStateData>> output = OBJECT_MAPPER.readValue(outContent.toString(),
        new TypeReference<Map<String, List<RunStateData>>>() { });
    assertThat(output, is(expectedOutput));
  }
}
