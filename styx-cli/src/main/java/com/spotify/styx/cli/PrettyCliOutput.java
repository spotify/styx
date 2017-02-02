/*-
 * -\-\-
 * Spotify Styx CLI
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

package com.spotify.styx.cli;

import static com.spotify.styx.cli.CliUtil.colored;
import static com.spotify.styx.cli.CliUtil.coloredBright;
import static com.spotify.styx.cli.CliUtil.formatTimestamp;
import static com.spotify.styx.util.ParameterUtil.toParameter;
import static org.fusesource.jansi.Ansi.Color.BLACK;
import static org.fusesource.jansi.Ansi.Color.BLUE;
import static org.fusesource.jansi.Ansi.Color.CYAN;
import static org.fusesource.jansi.Ansi.Color.DEFAULT;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.RED;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

import com.google.common.collect.Iterables;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.cli.RunStateDataPayload;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.StateData;
import java.util.List;
import org.fusesource.jansi.Ansi;

class PrettyCliOutput implements CliOutput {

  private void println() {
    System.out.println();
  }

  private static void println(Object output) {
    System.out.println(output.toString());
  }

  private static void format(String format, Object... args) {
    System.out.println(String.format(format, args));
  }

  @Override
  public void printStates(RunStateDataPayload runStateDataPayload) {
    format(
        "  %-20s %-12s %-47s %-7s %s",
        "WORKFLOW INSTANCE",
        "STATE",
        "LAST EXECUTION ID",
        "TRIES",
        "PREVIOUS EXECUTION MESSAGE");

    CliUtil.groupStates(runStateDataPayload.activeStates()).entrySet().forEach(entry -> {
      println();
      format("%s %s",
             colored(CYAN, entry.getKey().componentId()),
             colored(BLUE, entry.getKey().endpointId()));
      entry.getValue().forEach(runStateData -> {
        final StateData stateData = runStateData.stateData();
        final Ansi ansiState = getAnsiForState(runStateData);

        final Message lastMessage = Iterables.getLast(
            stateData.messages(), Message.create(Message.MessageLevel.UNKNOWN, "No info"));
        final Ansi ansiMessage = colored(messageColor(lastMessage.level()),
                                         lastMessage.line());

        format("  %-20s %-20s %-47s %-7d %s",
               runStateData.workflowInstance().parameter(),
               ansiState,
               stateData.executionId().orElse("<no-execution-id>"),
               stateData.tries(),
               ansiMessage
        );
      });
    });
  }

  @Override
  public void printEvents(List<EventInfo> eventInfos) {
    final String formatString = "%-25s %-25s %s";
    format(formatString,
           "TIME",
           "EVENT",
           "DATA");
    eventInfos.forEach(
        eventInfo ->
            format(formatString,
                   formatTimestamp(eventInfo.timestamp()),
                   eventInfo.name(),
                   eventInfo.info()));
  }

  @Override
  public void printBackfill(Backfill backfill) {
    final Partitioning partitioning = backfill.partitioning();
    final WorkflowId workflowId = backfill.workflowId();
    final String s = "%15s %s";

    format(s, "id:",           colored(CYAN, backfill.id()));
    format(s, "component:",    colored(CYAN, workflowId.componentId()));
    format(s, "workflow:",     colored(CYAN, workflowId.endpointId()));
    format(s, "halted:",       colored(CYAN, backfill.halted()));
    format(s, "completed:",    colored(CYAN, backfill.completed()));
    format(s, "concurrency:",  colored(CYAN, backfill.concurrency()));
    format(s, "start (incl):", colored(CYAN, toParameter(partitioning, backfill.start())));
    format(s, "end (excl):",   colored(CYAN, toParameter(partitioning, backfill.end())));
    format(s, "next trigger:", colored(CYAN, toParameter(partitioning, backfill.nextTrigger())));
  }

  @Override
  public void printBackfillPayload(BackfillPayload backfillPayload) {
    println(coloredBright(RED, "BACKFILL DATA"));
    printBackfill(backfillPayload.backfill());
    if (backfillPayload.statuses().isPresent()) {
      println();
      println(coloredBright(RED, "BACKFILL PROGRESS"));
      printStates(backfillPayload.statuses().get());
    }
    println();
    println();
  }

  private Ansi getAnsiForState(RunStateDataPayload.RunStateData RunStateData) {
    final String state = RunStateData.state();
    switch (state) {
      case "WAITING":    return coloredBright(BLACK, state);
      case "NEW":        return coloredBright(BLACK, state);
      case "QUEUED":     return coloredBright(BLACK, state);
      case "PREPARE":    return colored(CYAN, state);
      case "SUBMITTING": return colored(CYAN, state);
      case "SUBMITTED":  return colored(CYAN, state);
      case "RUNNING":    return coloredBright(BLUE, state);
      case "TERMINATED": return coloredBright(BLACK, state);
      case "FAILED":     return colored(RED, state);
      case "UNKNOWN":    return coloredBright(RED, state);
      case "ERROR":      return coloredBright(RED, state);
      case "DONE":       return coloredBright(GREEN, state);
      default:           return colored(DEFAULT, state);
    }
  }

  private Ansi.Color messageColor(Message.MessageLevel level) {
    switch (level) {
      case INFO:    return GREEN;
      case WARNING: return YELLOW;
      case ERROR:   return RED;
      default:      return DEFAULT;
    }
  }
}
