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

import com.spotify.styx.api.cli.ActiveStatesPayload;
import com.spotify.styx.state.StateData;
import java.util.List;
import org.fusesource.jansi.Ansi;

class PrettyCliOutput implements CliOutput {

  @Override
  public void printActiveStates(ActiveStatesPayload activeStatesPayload) {
    System.out.println(String.format(
        "  %-20s %-12s %-47s %-7s %s",
        "WORKFLOW INSTANCE",
        "STATE",
        "LAST EXECUTION ID",
        "TRIES",
        "PREVIOUS EXECUTION MESSAGE"));

    CliUtil.groupActiveStates(activeStatesPayload.activeStates()).entrySet().forEach(entry -> {
      System.out.println();
      System.out.println(String.format("%s %s",
                                       colored(CYAN, entry.getKey().componentId()),
                                       colored(BLUE, entry.getKey().endpointId())));
      entry.getValue().forEach(activeState -> {
        final StateData stateData = activeState.stateData();
        final List<StateData.Message> messages = stateData.messages();

        final Ansi lastMessage;
        if (messages.isEmpty()) {
          lastMessage = colored(DEFAULT, "No info");
        } else {
          final StateData.Message message = messages.get(messages.size() - 1);
          final Ansi.Color messageColor = messageColor(message.level());
          lastMessage = colored(messageColor, message.line());
        }

        final Ansi state;
        switch (activeState.state()) {
          case "QUEUED":
            state = coloredBright(BLACK, activeState.state());
            break;

          case "RUNNING":
            state = coloredBright(GREEN, activeState.state());
            break;

          default:
            state = colored(DEFAULT, activeState.state());
        }

        System.out.println(String.format(
            "  %-20s %-20s %-47s %-7d %s",
            activeState.workflowInstance().parameter(),
            state,
            stateData.executionId().orElse("<no-execution-id>"),
            stateData.tries(),
            lastMessage
        ));
      });
    });
  }

  private Ansi.Color messageColor(StateData.MessageLevel level) {
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
}
