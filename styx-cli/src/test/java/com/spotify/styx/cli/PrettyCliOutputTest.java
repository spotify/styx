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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.WorkflowId;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Instant;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PrettyCliOutputTest {

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

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

  private PrintStream old;

  private CliOutput cliOutput;

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
    cliOutput.printBackfill(BACKFILL);
    assertEquals("                  backfill-2   false          false            2  "
                 + "2017-01-01     2017-01-02     2017-01-01     component  workflow2 Description\n",
        outContent.toString());
  }

  @Test
  public void shouldPrintBackfills() {

    cliOutput.printBackfills(ImmutableList.of(BackfillPayload.create(BACKFILL, Optional.empty())));
    assertEquals("                 BACKFILL ID  HALTED  ALL TRIGGERED  CONCURRENCY  "
                 + "START (INCL)   END (EXCL)     NEXT TRIGGER   COMPONENT  WORKFLOW  DESCRIPTION\n"
                 + "                  backfill-2   false          false            2  "
                 + "2017-01-01     2017-01-02     2017-01-01     component  workflow2 Description\n",
        outContent.toString());
  }

  @Test
  public void shouldPrintNAWhenBackfillLacksDescription() {

    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-02T00:00:00Z"))
        .workflowId(WorkflowId.create("component", "workflow2"))
        .concurrency(2)
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .build();

    cliOutput.printBackfills(ImmutableList.of(BackfillPayload.create(backfill, Optional.empty())));
    assertEquals("                 BACKFILL ID  HALTED  ALL TRIGGERED  CONCURRENCY  "
                 + "START (INCL)   END (EXCL)     NEXT TRIGGER   COMPONENT  WORKFLOW  DESCRIPTION\n"
                 + "                  backfill-2   false          false            2  "
                 + "2017-01-01     2017-01-02     2017-01-01     component  workflow2 N/A\n",
        outContent.toString());
  }
}
