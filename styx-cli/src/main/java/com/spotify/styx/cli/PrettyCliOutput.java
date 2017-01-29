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

        final Ansi ansi_state;
        switch (RunStateData.state()) {
          case "QUEUED":
            ansi_state = coloredBright(BLACK, RunStateData.state());
            break;

          case "RUNNING":
            ansi_state = coloredBright(GREEN, RunStateData.state());
            break;

          default:
            ansi_state = colored(DEFAULT, RunStateData.state());
        }

        System.out.println(String.format(
            "  %-20s %-20s %-47s %-7d %s",
            RunStateData.workflowInstance().parameter(),
            ansi_state,
            stateData.executionId().orElse("<no-execution-id>"),
            stateData.tries(),
            lastMessage
        ));
      });
    });
  }

  private Ansi.Color messageColor(Message.MessageLevel level) {
    switch (level) {
      case INFO:    return GREEN;
      case WARNING: return YELLOW;
      case ERROR:   return RED;
      default:      return DEFAULT;
    }
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
    System.out.println(backfill);
  }

  @Override
  public void printBackfill(BackfillPayload backfillPayload) {
    printBackfill(backfillPayload.backfill());
    if (backfillPayload.statuses().isPresent()) {
      printStates(backfillPayload.statuses().get());
    }
  }
}
