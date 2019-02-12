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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfiguration.Secret;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowState;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PlainCliOutputTest {

  private static final Backfill BACKFILL = Backfill.newBuilder()
      .id("backfill-2")
      .start(Instant.parse("2017-01-01T00:00:00Z"))
      .end(Instant.parse("2017-01-02T00:00:00Z"))
      .workflowId(WorkflowId.create("component", "workflow2"))
      .concurrency(2)
      .description("Description")
      .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
      .schedule(Schedule.DAYS)
      .reverse(false)
      .triggerParameters(TriggerParameters.builder().env("FOO", "bar").build())
      .build();
  private static final String EXPECTED_OUTPUT =
      "backfill-2 component workflow2 false false 2 2017-01-01T00:00:00Z"
        + " 2017-01-02T00:00:00Z false 2017-01-01T00:00:00Z Description FOO=bar\n";

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

  private PrintStream old;

  private CliOutput cliOutput;

  @Before
  public void setUp() {
    old = System.out;
    System.setOut(new PrintStream(outContent));
    cliOutput = new PlainCliOutput();
  }

  @After
  public void tearDown() {
    System.setOut(old);
  }

  @Test
  public void shouldPrintBackfill() {
    cliOutput.printBackfill(BACKFILL, true);
    assertEquals(EXPECTED_OUTPUT,
        outContent.toString());
  }

  @Test
  public void shouldPrintBackfills() {
    cliOutput.printBackfills(List.of(BackfillPayload.create(BACKFILL, Optional.empty())), true);
    assertEquals(EXPECTED_OUTPUT,
        outContent.toString());
  }

  @Test
  public void shouldPrintBackfillPayload() {
    cliOutput.printBackfillPayload(BackfillPayload.create(BACKFILL, Optional.empty()), true);
    assertEquals(EXPECTED_OUTPUT,
        outContent.toString());
  }

  @Test
  public void shouldPrintWorkflows() {
    final Workflow foo1 = Workflow.create("foo1", WorkflowConfiguration.builder()
        .id("bar1")
        .schedule(Schedule.DAYS)
        .build());
    final Workflow foo2 = Workflow.create("foo2", WorkflowConfiguration.builder()
        .id("bar2")
        .schedule(Schedule.DAYS)
        .build());
    cliOutput.printWorkflows(List.of(foo1, foo2));
    assertThat(outContent.toString(), is(String.format("foo1 bar1%nfoo2 bar2%n")));
  }

  @Test
  public void shouldPrintWorkflow() {
    final Workflow workflow = Workflow.create("foo1", WorkflowConfiguration.builder()
        .id("bar1")
        .schedule(Schedule.DAYS)
        .offset("6h")
        .dockerImage("foo/bar:baz")
        .dockerArgs(List.of("foo", "the", "bar"))
        .dockerTerminationLogging(true)
        .secret(Secret.create("secret-foo", "/foo-secret"))
        .serviceAccount("foo@bar.baz")
        .resources("r1", "r2")
        .env("FOO", "foo", "BAR", "bar")
        .runningTimeout(Duration.parse("PT20H"))
        .commitSha("deadbeef")
        .build());
    final WorkflowState state = WorkflowState.builder()
        .enabled(true)
        .nextNaturalTrigger(OffsetDateTime.of(2018, 1, 2, 3, 4, 5, 6, ZoneOffset.UTC).toInstant())
        .nextNaturalOffsetTrigger(OffsetDateTime.of(2018, 1, 2, 9, 4, 5, 6, ZoneOffset.UTC).toInstant())
        .build();
    cliOutput.printWorkflow(workflow, state);
    assertThat(outContent.toString(), is(
        "foo1 bar1 DAYS 6h foo/bar:baz [foo, the, bar] true secret-foo:/foo-secret foo@bar.baz [r1, r2] {BAR=bar, "
        + "FOO=foo} PT20H deadbeef true 2018-01-02T03:04:05.000000006Z 2018-01-02T09:04:05.000000006Z\n"));

  }
}
