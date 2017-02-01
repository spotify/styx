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
import static org.fusesource.jansi.Ansi.Color.BLACK;
import static org.fusesource.jansi.Ansi.Color.BLUE;
import static org.fusesource.jansi.Ansi.Color.CYAN;
import static org.fusesource.jansi.Ansi.Color.DEFAULT;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.RED;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.cli.RunStateDataPayload;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.StateData;
import java.util.List;
import org.fusesource.jansi.Ansi;

class PrettyCliOutput implements CliOutput {

  @Override
  public void printStates(RunStateDataPayload runStateDataPayload) {
    System.out.println(String.format(
        "  %-20s %-12s %-47s %-7s %s",
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
      entry.getValue().forEach(RunStateData -> {
        final StateData stateData = RunStateData.stateData();
        final List<Message> messages = stateData.messages();

        final Ansi lastMessage;
        if (messages.isEmpty()) {
          lastMessage = colored(DEFAULT, "No info");
        } else {
          final Message message = messages.get(messages.size() - 1);
          final Ansi.Color messageColor = messageColor(message.level());
          lastMessage = colored(messageColor, message.line());
        }

        final Ansi ansiState = getAnsiForState(RunStateData);

        System.out.println(String.format(
            "  %-20s %-20s %-47s %-7d %s",
            RunStateData.workflowInstance().parameter(),
            ansiState,
            stateData.executionId().orElse("<no-execution-id>"),
            stateData.tries(),
            lastMessage
        ));
      });
    });
  }

  @Override
  public void printEvents(List<EventInfo> eventInfos) {
    final String format = "%-25s %-25s %s";
    System.out.println(String.format(format,
                                     "TIME",
                                     "EVENT",
                                     "DATA"));
    eventInfos.forEach(
        eventInfo ->
            System.out.println(String.format(format,
                                             formatTimestamp(eventInfo.timestamp()),
                                             eventInfo.name(),
                                             eventInfo.info())));
  }

  @Override
  public void printBackfill(Backfill backfill) {
    System.out.println(String.format("%32s %s",
                                     "Backfill id:",
                                     colored(CYAN, backfill.id())));
    System.out.println(String.format("%32s %s",
                                     "Component id:",
                                     colored(CYAN, backfill.workflowId().componentId())));
    System.out.println(String.format("%32s %s",
                                     "Workflow id:",
                                     colored(CYAN, backfill.workflowId().endpointId())));
    System.out.println(String.format("%32s %20s",
                                     "Start date/datehour (inclusive):",
                                     colored(CYAN, backfill.start())));
    System.out.println(String.format("%32s %20s",
                                     "End date/datehour (exclusive):",
                                     colored(CYAN, backfill.end())));
    if (!backfill.halted() && !backfill.completed()) {
      System.out.println(String.format("%32s %20s",
                                       "Next date/datehour:",
                                       colored(CYAN, backfill.nextTrigger())));
    }
    System.out.println(String.format("%32s %s",
                                     "Completed:",
                                     colored(CYAN, backfill.completed())));
    System.out.println(String.format("%32s %s",
                                     "Halted:",
                                     colored(CYAN, backfill.halted())));
  }

  @Override
  public void printBackfill(BackfillPayload backfillPayload) {
    System.out.println(coloredBright(RED, "BACKFILL DATA"));
    System.out.println();
    printBackfill(backfillPayload.backfill());
    System.out.println();
    System.out.println(coloredBright(RED, "BACKFILL PROGRESS"));
    System.out.println();
    if (backfillPayload.statuses().isPresent()) {
      printStates(backfillPayload.statuses().get());
    }
  }

  private Ansi getAnsiForState(RunStateDataPayload.RunStateData RunStateData) {
    Ansi ansiState;
    switch (RunStateData.state()) {
      case "WAITING":
        ansiState = coloredBright(BLACK, RunStateData.state());
        break;

      case "NEW":
        ansiState = coloredBright(BLACK, RunStateData.state());
        break;

      case "QUEUED":
        ansiState = coloredBright(BLACK, RunStateData.state());
        break;

      case "PREPARE":
        ansiState = colored(CYAN, RunStateData.state());
        break;

      case "SUBMITTING":
        ansiState = colored(CYAN, RunStateData.state());
        break;

      case "SUBMITTED":
        ansiState = colored(CYAN, RunStateData.state());
        break;

      case "RUNNING":
        ansiState = coloredBright(BLUE, RunStateData.state());
        break;

      case "TERMINATED":
        ansiState = coloredBright(BLACK, RunStateData.state());
        break;

      case "FAILED":
        ansiState = colored(RED, RunStateData.state());
        break;

      case "UNKNOWN":
        ansiState = coloredBright(RED, RunStateData.state());
        break;

      case "ERROR":
        ansiState = coloredBright(RED, RunStateData.state());
        break;

      case "DONE":
        ansiState = coloredBright(GREEN, RunStateData.state());
        break;

      default:
        ansiState = colored(DEFAULT, RunStateData.state());
    }
    return ansiState;
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
