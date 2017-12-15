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
import static com.spotify.styx.util.ParameterUtil.rangeOfInstants;
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
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.EventInfo;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.StateData;
import java.util.Collections;
import java.util.List;
import org.fusesource.jansi.Ansi;

class PrettyCliOutput implements CliOutput {

  @Override
  public void printStates(RunStateDataPayload runStateDataPayload) {
    System.out.println(String.format("  %-20s %-12s %-47s %-7s %s",
                                     "WORKFLOW INSTANCE",
                                     "STATE",
                                     "EXECUTION ID",
                                     "TRIES",
                                     "PREVIOUS EXECUTION MESSAGE"));

    CliUtil.groupStates(runStateDataPayload.activeStates()).entrySet().forEach(entry -> {
      System.out.println();
      System.out.println(String.format("%s %s",
                                       colored(CYAN, entry.getKey().componentId()),
                                       colored(BLUE, entry.getKey().id())));
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

  private static final String BACKFILL_FORMAT = "%28s  %7s  %8s  %10s  %9s  %11s  %-13s  %-13s  %-13s  %s  %s";

  @Override
  public void printBackfill(Backfill backfill) {
    final Schedule schedule = backfill.schedule();
    final WorkflowId workflowId = backfill.workflowId();
    final int remaining = rangeOfInstants(backfill.nextTrigger(), backfill.end(), schedule).size();
    final int total = rangeOfInstants(backfill.start(), backfill.end(), schedule).size();
    final String state;
    if (backfill.halted()) {
      state = "HALTED";
    } else if (backfill.allTriggered()) {
      state = "DONE";
    } else {
      state = "RUNNING";
    }

    System.out.println(String.format(BACKFILL_FORMAT,
                                     backfill.id(),
                                     state,
                                     String.format("%.0f%%", 100 - 100f * remaining / total),
                                     total,
                                     remaining,
                                     backfill.concurrency(),
                                     toParameter(schedule, backfill.start()),
                                     toParameter(schedule, backfill.end()),
                                     toParameter(schedule, backfill.nextTrigger()),
                                     workflowId.componentId(),
                                     workflowId.id()));
  }

  private void printBackfillHeader() {
    System.out.println(String.format(BACKFILL_FORMAT,
                                     "BACKFILL_ID",
                                     "STATE",
                                     "PROGRESS",
                                     "PARTITIONS",
                                     "REMAINING",
                                     "CONCURRENCY",
                                     "START(INCL)",
                                     "END(EXCL)",
                                     "NEXT_TRIGGER",
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

  @Override
  public void printResources(List<Resource> resources) {
    final String format = "%50s %12s";
    System.out.println(String.format(format, "RESOURCE", "CONCURRENCY"));
    resources.forEach(resource -> System.out.println(
        String.format(format, resource.id(), resource.concurrency())));
  }

  @Override
  public void printMessage(String message) {
    System.out.println(message);
  }

  @Override
  public void printWorkflow(Workflow wf, WorkflowState state) {
    System.out.println("Component: " + wf.componentId());
    System.out.println(" Workflow: " + wf.id().id());
    System.out.println(" Schedule: " + wf.configuration().schedule());
    System.out.println("   Offset: " + wf.configuration().offset().orElse(""));
    System.out.println("    Image: " + wf.configuration().dockerImage().orElse(""));
    System.out.println("     Args: " + wf.configuration().dockerArgs().orElse(Collections.emptyList()));
    System.out.println("  TermLog: " + wf.configuration().dockerTerminationLogging());
    System.out.println("   Secret: " + wf.configuration().secret().map(s -> s.name() + ':' + s.mountPath()).orElse(""));
    System.out.println(" Svc Acct: " + wf.configuration().serviceAccount().orElse(""));
    System.out.println("Resources: " + wf.configuration().resources());
    System.out.println("   Commit: " + wf.configuration().commitSha().orElse(""));
    System.out.println("  Enabled: " + state.enabled().map(Object::toString).orElse(""));
    System.out.println("     Trig: " + state.nextNaturalTrigger().map(Object::toString).orElse(""));
    System.out.println("Ofst Trig: " + state.nextNaturalOffsetTrigger().map(Object::toString).orElse(""));
  }

  @Override
  public void printError(String message) {
    System.err.println(message);
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
