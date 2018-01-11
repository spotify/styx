package com.spotify.styx.cli;

import static org.junit.Assert.*;

import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.WorkflowId;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.time.Instant;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PlainCliOutputTest {

  private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

  private CliOutput cliOutput;

  @Before
  public void setUp() {
    System.setOut(new PrintStream(outContent));
    cliOutput = new PlainCliOutput();
  }

  @After
  public void tearDown() {
    System.setOut(null);
    System.setErr(null);
  }

  @Test
  public void shouldPrintBackfill() {
    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-02T00:00:00Z"))
        .workflowId(WorkflowId.create("component", "workflow2"))
        .concurrency(2)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .build();

    cliOutput.printBackfill(backfill);
    assertEquals("backfill-2 component workflow2 false false 2 2017-01-01T00:00:00Z"
                 + " 2017-01-02T00:00:00Z 2017-01-01T00:00:00Z Description\n",
        outContent.toString());
  }
}