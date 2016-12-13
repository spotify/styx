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
import static com.spotify.styx.cli.CliUtil.formatTimestamp;
import static org.fusesource.jansi.Ansi.Color.BLUE;
import static org.fusesource.jansi.Ansi.Color.CYAN;
import static org.fusesource.jansi.Ansi.Color.DEFAULT;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.MAGENTA;
import static org.fusesource.jansi.Ansi.Color.WHITE;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

import com.spotify.styx.api.cli.ActiveStatesPayload;
import com.spotify.styx.api.cli.EventsPayload;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventVisitor;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.util.EventUtil;
import org.fusesource.jansi.Ansi;

class PrettyCliOutput implements CliOutput {

  @Override
  public void printActiveStates(ActiveStatesPayload activeStatesPayload) {
    final String format = "  %-20s %-16s %-47s %s";
    System.out.println(String.format(format,
                                     "WORKFLOW INSTANCE",
                                     "STATE",
                                     "LAST EXECUTION ID",
                                     "PREVIOUS EXECUTION INFO"));
    CliUtil.groupActiveStates(activeStatesPayload.activeStates()).entrySet().forEach(entry -> {
      System.out.println();
      System.out.println(String.format("%s %s",
                                       colored(CYAN, entry.getKey().componentId()),
                                       colored(BLUE, entry.getKey().endpointId())));
      entry.getValue().forEach(activeState -> {
        final Ansi previousExecutionInfo;
        if (activeState.previousExecutionLastEvent().isPresent()) {
          final Event event = activeState.previousExecutionLastEvent().get().toEvent();
          final String message = CliUtil.lastExecutionMessage(event);
          final Ansi.Color messageColor = event.accept(LastExecutionColor.LAST_EXECUTION_COLOR);
          previousExecutionInfo = colored(messageColor, message);
        } else {
          previousExecutionInfo = colored(DEFAULT, "No data found");
        }
        System.out.println(String.format(format,
                                         activeState.workflowInstance().parameter(),
                                         activeState.state(),
                                         activeState.lastExecutionId(),
                                         previousExecutionInfo));
      });
    });
  }

  @Override
  public void printEvents(EventsPayload eventsPayload) {
    final String format = "%-25s %-25s %s";
    System.out.println(String.format(format,
                                     "TIME",
                                     "EVENT",
                                     "DATA"));
    eventsPayload.events().forEach(
        timestampedEvent ->
            System.out.println(String.format(format,
                                             formatTimestamp(timestampedEvent.timestamp()),
                                             EventUtil.name(timestampedEvent.event().toEvent()),
                                             CliUtil.data(timestampedEvent.event().toEvent()))));
  }

  private enum LastExecutionColor implements EventVisitor<Ansi.Color> {
    LAST_EXECUTION_COLOR;

    @Override
    public Ansi.Color terminate(WorkflowInstance workflowInstance, int exitCode) {
      switch (exitCode) {
        case CliUtil.SUCCESS_EXIT_CODE:
          return GREEN;
        case CliUtil.MISSING_DEPENDENCIES_EXIT_CODE:
          return YELLOW;
        default:
          return MAGENTA;
      }
    }

    @Override
    public Ansi.Color runError(WorkflowInstance workflowInstance, String message) {
      return MAGENTA;
    }

    @Override
    public Ansi.Color timeTrigger(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color triggerExecution(WorkflowInstance workflowInstance, String triggerId) {
      return WHITE;
    }

    @Override
    public Ansi.Color created(WorkflowInstance workflowInstance, String executionId,
                              String dockerImage) {
      return WHITE;
    }

    @Override
    public Ansi.Color dequeue(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color started(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color success(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color retryAfter(WorkflowInstance workflowInstance, long delayMillis) {
      return WHITE;
    }

    @Override
    public Ansi.Color retry(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color stop(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color timeout(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color halt(WorkflowInstance workflowInstance) {
      return WHITE;
    }

    @Override
    public Ansi.Color submit(WorkflowInstance workflowInstance,
                             ExecutionDescription executionDescription) {
      return WHITE;
    }

    @Override
    public Ansi.Color submitted(WorkflowInstance workflowInstance, String executionId) {
      return WHITE;
    }
  }
}
