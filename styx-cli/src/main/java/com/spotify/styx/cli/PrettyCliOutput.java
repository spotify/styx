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
import static com.spotify.styx.cli.CliUtil.formatMap;
import static com.spotify.styx.cli.CliUtil.formatTimestamp;
import static com.spotify.styx.util.ParameterUtil.toParameter;
import static org.fusesource.jansi.Ansi.Color.BLACK;
import static org.fusesource.jansi.Ansi.Color.BLUE;
import static org.fusesource.jansi.Ansi.Color.CYAN;
import static org.fusesource.jansi.Ansi.Color.DEFAULT;
import static org.fusesource.jansi.Ansi.Color.GREEN;
import static org.fusesource.jansi.Ansi.Color.RED;
import static org.fusesource.jansi.Ansi.Color.YELLOW;

import com.google.common.base.Joiner;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.api.RunStateDataPayload.RunStateData;
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
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import org.fusesource.jansi.Ansi;

class PrettyCliOutput implements CliOutput {

  private static final String BACKFILL_FORMAT =
      "%28s  %6s  %13s %12s  %-20s  %-20s  %-7s  %-20s  %-<cid-length>s  %-<wid-length>s %-<description-length>s %s";

  private static final String WORKFLOW_FORMAT =
      "%-<cid-length>s  %-<wid-length>s";

  private static final String NA_VALUE = "N/A";

  private static final int TRUNCATED_LENGTH = 20;

  private static final String ELLIPSIS = "...";

  private static final String COMPONENT_HEADER = "COMPONENT";

  private static final String WORKFLOW_HEADER = "WORKFLOW";

  private static final String DESCRIPTION_HEADER = "DESCRIPTION";

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

        final Message lastMessage =
            stateData.message().orElse(Message.create(Message.MessageLevel.UNKNOWN, "No info"));
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
  public void printBackfill(Backfill backfill, boolean noTruncate) {
    printBackfill(backfill,
        effectiveLongFieldLength(
            Optional.ofNullable(backfill.workflowId().componentId()), COMPONENT_HEADER, true),
        effectiveLongFieldLength(
            Optional.ofNullable(backfill.workflowId().id()), WORKFLOW_HEADER, true),
        effectiveLongFieldLength(
            backfill.description(), DESCRIPTION_HEADER, noTruncate),
        noTruncate);
  }

  private void printBackfill(Backfill backfill, int cidLength, int widLength,
                             int descriptionLength, boolean noTruncate) {
    final Schedule schedule = backfill.schedule();
    final WorkflowId workflowId = backfill.workflowId();

    final String format = BACKFILL_FORMAT
        .replaceAll("<cid-length>", String.valueOf(cidLength))
        .replaceAll("<wid-length>", String.valueOf(widLength))
        .replaceAll("<description-length>", String.valueOf(descriptionLength));

    System.out.println(String.format(format,
        backfill.id(),
        backfill.halted(),
        backfill.allTriggered(),
        backfill.concurrency(),
        toParameter(schedule, backfill.start()),
        toParameter(schedule, backfill.end()),
        backfill.reverse(),
        toParameter(schedule, backfill.nextTrigger()),
        workflowId.componentId(),
        workflowId.id(),
        formatLongField(backfill.description(), noTruncate),
        formatLongField(backfill.triggerParameters()
                .map(triggerParameters -> formatMap(triggerParameters.env())),
            noTruncate)));
  }

  private void printWorkflow(Workflow workflow, int cidLength, int widLength) {
    final String format = WORKFLOW_FORMAT
        .replaceAll("<cid-length>", String.valueOf(cidLength))
        .replaceAll("<wid-length>", String.valueOf(widLength));

    System.out.println(String.format(format,
                                     workflow.componentId(),
                                     workflow.workflowId()));
  }

  private void printBackfillHeader(int cidLength, int widLength, int descriptionLength) {
    final String format = BACKFILL_FORMAT
        .replaceAll("<cid-length>", String.valueOf(cidLength))
        .replaceAll("<wid-length>", String.valueOf(widLength))
        .replaceAll("<description-length>", String.valueOf(descriptionLength));

    System.out.println(String.format(format,
        "BACKFILL ID",
        "HALTED",
        "ALL TRIGGERED",
        "CONCURRENCY",
        "START (INCL)",
        "END (EXCL)",
        "REVERSE",
        "NEXT TRIGGER",
        COMPONENT_HEADER,
        WORKFLOW_HEADER,
        DESCRIPTION_HEADER,
        "TRIGGER ENV"));
  }

  private void printWorkflowHeader(int cidLength, int widLength) {
    final String format = WORKFLOW_FORMAT
        .replaceAll("<cid-length>", String.valueOf(cidLength))
        .replaceAll("<wid-length>", String.valueOf(widLength));

    System.out.println(String.format(format,
                                     "COMPONENT",
                                     "WORKFLOW"));
  }

  @Override
  public void printBackfillPayload(BackfillPayload backfillPayload, boolean noTruncate) {
    printBackfillHeader(
        effectiveLongFieldLength(
            Optional.ofNullable(backfillPayload.backfill().workflowId().componentId()),
            COMPONENT_HEADER, true),
        effectiveLongFieldLength(Optional.ofNullable(backfillPayload.backfill().workflowId().id()),
            WORKFLOW_HEADER, true),
        effectiveLongFieldLength(backfillPayload.backfill().description(), DESCRIPTION_HEADER,
            noTruncate));

    printBackfill(backfillPayload.backfill(), noTruncate);
    if (backfillPayload.statuses().isPresent()) {
      System.out.println();
      System.out.println();
      printStates(backfillPayload.statuses().get());
    }
  }

  @Override
  public void printBackfills(List<BackfillPayload> backfills, boolean noTruncate) {
    final int cidLength = backfills.stream()
        .map(x -> effectiveLongFieldLength(
            Optional.of(x.backfill().workflowId().componentId()), COMPONENT_HEADER, true))
        .max(Comparator.naturalOrder())
        .orElse(1);
    final int widLength = backfills.stream()
        .map(x -> effectiveLongFieldLength(
            Optional.of(x.backfill().workflowId().id()), WORKFLOW_HEADER, true))
        .max(Comparator.naturalOrder())
        .orElse(1);
    final int descriptionLength = backfills.stream()
        .map(x -> effectiveLongFieldLength(
            x.backfill().description(), DESCRIPTION_HEADER, noTruncate))
        .max(Comparator.naturalOrder())
        .orElse(1);

    printBackfillHeader(cidLength, widLength, descriptionLength);

    for (BackfillPayload backfillPayload : backfills) {
      printBackfill(backfillPayload.backfill(), cidLength, widLength, descriptionLength, noTruncate);
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
    System.out.println("      Env: " + Joiner.on(' ').withKeyValueSeparator('=').join(wf.configuration().env()));
    System.out.println("   Commit: " + wf.configuration().commitSha().orElse(""));
    System.out.println("  Enabled: " + state.enabled().map(Object::toString).orElse(""));
    System.out.println("     Trig: " + state.nextNaturalTrigger().map(Object::toString).orElse(""));
    System.out.println("Ofst Trig: " + state.nextNaturalOffsetTrigger().map(Object::toString).orElse(""));
  }

  @Override
  public void printWorkflows(List<Workflow> workflows) {
    final int cidLength = workflows.stream()
        .map(x -> x.componentId().length())
        .max(Comparator.naturalOrder())
        .orElse(1);
    final int widLength = workflows.stream()
        .map(x -> x.workflowId().length())
        .max(Comparator.naturalOrder())
        .orElse(1);

    printWorkflowHeader(cidLength, widLength);

    workflows.forEach(wf -> printWorkflow(wf, cidLength, widLength));
  }

  @Override
  public void printError(String message) {
    System.err.println(message);
  }

  private String formatLongField(Optional<String> value, boolean noTruncate) {
    return value.map(x -> {
      if (noTruncate || x.length() <= TRUNCATED_LENGTH) {
        return x;
      } else {
        return x.substring(0, TRUNCATED_LENGTH) + ELLIPSIS;
      }
    }).orElse(NA_VALUE);
  }

  private int effectiveLongFieldLength(Optional<String> value, String header, boolean noTruncate) {
    final int length = value.orElse(NA_VALUE).length();
    if (length <= header.length()) {
      return header.length();
    }

    if (!noTruncate && length > (TRUNCATED_LENGTH + ELLIPSIS.length())) {
      return TRUNCATED_LENGTH + ELLIPSIS.length();
    } else {
      return length;
    }
  }

  private Ansi getAnsiForState(RunStateData RunStateData) {
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
