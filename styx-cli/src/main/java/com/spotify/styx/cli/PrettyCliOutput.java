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

  private static final String BACKFILL_FORMAT = "%28s  %6s  %13s %12s  %-13s  %-13s  %-13s  %s  %s";

  @Override
  public void printStates(RunStateDataPayload runStateDataPayload) {
    System.out.println(String.format("  %-20s %-12s %-47s %-7s %s",
                                     "WORKFLOW INSTANCE",
                                     "STATE",
                                     "LAST EXECUTION ID",
                                     "TRIES",
                                     "PREVIOUS EXECUTION MESSAGE"));

    CliUtil.groupStates(runStateDataPayload.activeStates()).entrySet().forEach(entry -> {
      System.out.println();
      System.out.println(String.format("%s %s",
                                       colored(CYAN, entry.getKey().componentId()),
                                       colored(BLUE, entry.getKey().endpointId())));
      entry.getValue().forEach(runStateData -> {
        final StateData stateData = runStateData.stateData();
        final Ansi ansiState = getAnsiForState(runStateData);

        final Message lastMessage = Iterables.getLast(
            stateData.messages(), Message.create(Message.MessageLevel.UNKNOWN, "No info"));
        final Ansi ansiMessage = colored(messageColor(lastMessage.level()),
                                         lastMessage.line());

        System.out.println(String.format("  %-20s %-20s %-47s %-7d %s",
                                         runStateData.workflowInstance().parameter(),
                                         ansiState,
                                         stateData.executionId().orElse("<no-execution-id>"),
                                         stateData.tries(),
                                         ansiMessage));
      });
    });
  }

  @Override
  public void printEvents(List<EventInfo> eventInfos) {
    final String formatString = "%-25s %-25s %s";
    System.out.println(String.format(formatString,
                                     "TIME",
                                     "EVENT",
                                     "DATA"));
    eventInfos.forEach(
        eventInfo -> System.out.println(String.format(formatString,
                                                      formatTimestamp(eventInfo.timestamp()),
                                                      eventInfo.name(),
                                                      eventInfo.info())));
  }

  @Override
  public void printBackfill(Backfill backfill) {
    final Partitioning partitioning = backfill.partitioning();
    final WorkflowId workflowId = backfill.workflowId();

    System.out.println(String.format(BACKFILL_FORMAT,
                                     backfill.id(),
                                     backfill.halted(),
                                     backfill.allTriggered(),
                                     backfill.concurrency(),
                                     toParameter(partitioning, backfill.start()),
                                     toParameter(partitioning, backfill.end()),
                                     toParameter(partitioning, backfill.nextTrigger()),
                                     workflowId.componentId(),
                                     workflowId.endpointId()));
  }

  private void printBackfillHeader() {
    System.out.println(String.format(BACKFILL_FORMAT,
                                     "BACKFILL ID",
                                     "HALTED",
                                     "ALL TRIGGERED",
                                     "CONCURRENCY",
                                     "START (INCL)",
                                     "END (EXCL)",
                                     "NEXT TRIGGER",
                                     "COMPONENT",
                                     "WORKFLOW"));
  }

  @Override
  public void printBackfillPayload(BackfillPayload backfillPayload) {
    printBackfillHeader();

    printBackfill(backfillPayload.backfill());
    if (backfillPayload.statuses().isPresent()) {
      System.out.println();
      System.out.println();
      printStates(backfillPayload.statuses().get());
    }
  }

  @Override
  public void printBackfills(List<BackfillPayload> backfills) {
    printBackfillHeader();

    for (BackfillPayload backfillPayload : backfills) {
      printBackfill(backfillPayload.backfill());
      if (backfillPayload.statuses().isPresent()) {
        System.out.println();
        System.out.println();
        printStates(backfillPayload.statuses().get());
      }
    }
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
