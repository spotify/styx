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

import static com.spotify.styx.cli.CliUtil.colored;
import static com.spotify.styx.cli.CliUtil.coloredBright;
import static org.fusesource.jansi.Ansi.Color.BLACK;
import static org.fusesource.jansi.Ansi.Color.BLUE;
import static org.fusesource.jansi.Ansi.Color.CYAN;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.YELLOW;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.api.RunStateDataPayload.RunStateData;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.StateData;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PrettyCliOutputTest {

  private static final String SHORT_DESCRIPTION = "Description";
  private static final String LONG_DESCRIPTION = "Description which is long enough to truncate";

  private static final String EXPECTED_HEADER =
      "                 BACKFILL ID  HALTED  ALL TRIGGERED  CONCURRENCY  "
      + "START (INCL)          END (EXCL)            REVERSE  NEXT TRIGGER          COMPONENT"
      + "  WORKFLOW  DESCRIPTION TRIGGER ENV\n";

  private static final String EXPECTED_HEADER_WITH_FULL_DESCRIPTION =
      "                 BACKFILL ID  HALTED  ALL TRIGGERED  CONCURRENCY  "
      + "START (INCL)          END (EXCL)            REVERSE  NEXT TRIGGER          COMPONENT"
      + "  WORKFLOW  DESCRIPTION                                  TRIGGER ENV\n";

  private static final String EXPECTED_HEADER_WITH_TRUNCATED_DESCRIPTION =
      "                 BACKFILL ID  HALTED  ALL TRIGGERED  CONCURRENCY  "
      + "START (INCL)          END (EXCL)            REVERSE  NEXT TRIGGER          COMPONENT"
      + "  WORKFLOW  DESCRIPTION             TRIGGER ENV\n";

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

  private PrintStream old;

  private CliOutput cliOutput;

  private static Backfill backfill(String description) {
    return Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-02T00:00:00Z"))
        .workflowId(WorkflowId.create("component", "workflow2"))
        .concurrency(2)
        .reverse(false)
        .description(description)
        .triggerParameters(TriggerParameters.builder().env("FOO", "bar").build())
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .build();
  }

  @Before
  public void setUp() {
    old = System.out;
    System.setOut(new PrintStream(outContent));
    cliOutput = new PrettyCliOutput();
  }

  @After
  public void tearDown() {
    System.setOut(old);
  }

  @Test
  public void shouldPrintBackfill() {
    cliOutput.printBackfill(backfill(LONG_DESCRIPTION)
        .builder()
        .triggerParameters(TriggerParameters
            .builder()
            .env("FOO", "bar", "BAR", "foo", "FOOBAR", "foobar")
            .build())
        .build(), false);
    assertEquals("                  backfill-2   false          false            2  "
                 + "2017-01-01            2017-01-02            false    2017-01-01            component  "
                 + "workflow2 Description which is... BAR=foo FOO=bar FOOB...\n",
        outContent.toString());
  }

  @Test
  public void shouldPrintBackfillWithShortDescription() {
    cliOutput.printBackfill(backfill(SHORT_DESCRIPTION), false);
    assertEquals("                  backfill-2   false          false            2  "
                 + "2017-01-01            2017-01-02            false    2017-01-01            component  "
                 + "workflow2 Description FOO=bar\n",
        outContent.toString());
  }

  @Test
  public void shouldPrintBackfillWithoutTruncating() {
    cliOutput.printBackfill(backfill(LONG_DESCRIPTION)
        .builder()
        .triggerParameters(TriggerParameters
            .builder()
            .env("FOO", "bar", "BAR", "foo", "FOOBAR", "foobar")
            .build())
        .build(), true);
    assertEquals("                  backfill-2   false          false            2  "
                 + "2017-01-01            2017-01-02            false    2017-01-01            component  "
                 + "workflow2 Description which is long enough to truncate BAR=foo FOO=bar FOOBAR=foobar\n",
        outContent.toString());
  }

  @Test
  public void shouldPrintBackfillPayload() {
    cliOutput.printBackfillPayload(BackfillPayload.create(backfill(LONG_DESCRIPTION),
        Optional.empty()), false);
    assertEquals(EXPECTED_HEADER_WITH_TRUNCATED_DESCRIPTION +
                 "                  backfill-2   false          false          " 
                 + "  2  "
                 + "2017-01-01            2017-01-02            false    2017-01-01            component  "
                 + "workflow2 Description which is... FOO=bar\n",
        outContent.toString());
  }

  @Test
  public void shouldPrintBackfillPayloadWithShortDescription() {
    cliOutput.printBackfillPayload(BackfillPayload.create(backfill(SHORT_DESCRIPTION),
        Optional.empty()), false);
    assertEquals(EXPECTED_HEADER +
                 "                  backfill-2   false          false          " 
                 + "  2  "
                 + "2017-01-01            2017-01-02            false    2017-01-01            component  "
                 + "workflow2 Description FOO=bar\n",
        outContent.toString());
  }

  @Test
  public void shouldPrintBackfillPayloadWithoutTruncating() {
    cliOutput.printBackfillPayload(BackfillPayload.create(backfill(LONG_DESCRIPTION),
        Optional.empty()), true);
    assertEquals(EXPECTED_HEADER_WITH_FULL_DESCRIPTION +
                 "                  backfill-2   false          false            2  "
                 + "2017-01-01            2017-01-02            false    2017-01-01            component  "
                 + "workflow2 Description which is long enough to truncate FOO=bar\n",
        outContent.toString());
  }

  @Test
  public void shouldPrintBackfills() {
    cliOutput.printBackfills(
        ImmutableList.of(BackfillPayload.create(backfill(LONG_DESCRIPTION), Optional.empty())),
        false);
    assertEquals(EXPECTED_HEADER_WITH_TRUNCATED_DESCRIPTION
                 + "                  backfill-2   false          false            2  "
                 + "2017-01-01            2017-01-02            false    2017-01-01            component  "
                 + "workflow2 Description which is... FOO=bar\n",
        outContent.toString());
  }

  @Test
  public void shouldPrintBackfillsWithoutTruncating() {
    cliOutput.printBackfills(
        ImmutableList.of(BackfillPayload.create(backfill(LONG_DESCRIPTION), Optional.empty())),
        true);
    assertEquals(EXPECTED_HEADER_WITH_FULL_DESCRIPTION
                 + "                  backfill-2   false          false            2  "
                 + "2017-01-01            2017-01-02            false    2017-01-01            component  "
                 + "workflow2 Description which is long enough to truncate FOO=bar\n",
        outContent.toString());
  }

  @Test
  public void shouldPrintNAWhenBackfillLacksDescription() {
    cliOutput.printBackfills(
        ImmutableList.of(BackfillPayload.create(backfill(null), Optional.empty()))
        , false);
    assertEquals(EXPECTED_HEADER
                 + "                  backfill-2   false          false            2  "
                 + "2017-01-01            2017-01-02            false    2017-01-01            "
                 + "component  workflow2 N/A         FOO=bar\n",
        outContent.toString());
  }

  @Test
  public void shouldPrintStates() throws Exception {
    final RunStateDataPayload states = RunStateDataPayload.create(ImmutableList.of(
        RunStateData.create(
            WorkflowInstance.create(WorkflowId.create("c0", "w0"), "2016-09-01"),
            "QUEUED",
            StateData.newBuilder()
                .messages(Message.info("foo"))
                .executionId("e0")
                .tries(17)
                .build()),
        RunStateData.create(
            WorkflowInstance.create(WorkflowId.create("c1", "w1"), "2016-09-02"),
            "SUBMITTED",
            StateData.newBuilder()
                .messages(Message.warning("baz"))
                .executionId("e1")
                .tries(3)
                .build())));
    cliOutput.printStates(states);
    assertThat(outContent.toString(),
        is("  WORKFLOW INSTANCE    STATE        EXECUTION ID                                    TRIES   PREVIOUS EXECUTION MESSAGE\n"
            + "\n"
            + colored(CYAN, "c0") + " " + colored(BLUE, "w0") + "\n"
            + "  2016-09-01           " + coloredBright(BLACK, "QUEUED") + "       e0                                  "
            + "            17      " + colored(GREEN, "foo") + "\n"
            + "\n"
            + colored(CYAN, "c1") + " " + colored(BLUE, "w1") + "\n"
            + "  2016-09-02           " + colored(CYAN,        "SUBMITTED") + "    e1                                  "
            + "            3       " + colored(YELLOW, "baz") + "\n"));
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
    cliOutput.printWorkflows(ImmutableList.of(foo1, foo2));
    assertThat(outContent.toString(), is(String.format(
        "COMPONENT  WORKFLOW%n"
           + "foo1  bar1%n"
           + "foo2  bar2%n")));

  }
}
