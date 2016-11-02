/*
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

import static org.fusesource.jansi.Ansi.ansi;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.styx.api.cli.ActiveStatesPayload;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventVisitor;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import org.fusesource.jansi.Ansi;

class CliUtil {

  public static final int SUCCESS_EXIT_CODE = 0;
  public static final int MISSING_DEPENDENCIES_EXIT_CODE = 20;

  private CliUtil() {
    // no instantiation
  }

  static Ansi colored(Ansi.Color color, Object obj) {
    return ansi().fg(color).a(obj).reset();
  }

  static SortedMap<WorkflowId, SortedSet<ActiveStatesPayload.ActiveState>> groupActiveStates(
      List<ActiveStatesPayload.ActiveState> activeStates) {

    SortedMap<WorkflowId, SortedSet<ActiveStatesPayload.ActiveState>> groupedWorkflowInstances =
        newSortedWorkflowIdSet();

    for (ActiveStatesPayload.ActiveState activeState : activeStates) {
      groupedWorkflowInstances.compute(activeState.workflowInstance().workflowId(), (k, v) -> {
        if (v == null) {
          v = newSortedActiveStateSet();
        }
        v.add(activeState);
        return v;
      });
    }
    return groupedWorkflowInstances;
  }

  private static TreeSet<ActiveStatesPayload.ActiveState> newSortedActiveStateSet() {
    return Sets.newTreeSet(ActiveStatesPayload.ActiveState.PARAMETER_COMPARATOR);
  }

  private static TreeMap<WorkflowId, SortedSet<ActiveStatesPayload.ActiveState>> newSortedWorkflowIdSet() {
    return Maps.newTreeMap(WorkflowId.KEY_COMPARATOR);
  }

  static String formatTimestamp(long timestamp) {
    return Instant
        .ofEpochMilli(timestamp)
        .atZone(ZoneId.of("UTC"))
        .toLocalDateTime()
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
  }

  public static String lastExecutionMessage(Event event) {
    return event.accept(LastExecutionMessage.INSTANCE);
  }

  public static String data(Event event) {
    return event.accept(EventDataVisitor.INSTANCE);
  }

  private enum LastExecutionMessage implements EventVisitor<String> {
    INSTANCE;

    @Override
    public String terminate(WorkflowInstance workflowInstance, int exitCode) {
      switch (exitCode) {
        case SUCCESS_EXIT_CODE:
        case MISSING_DEPENDENCIES_EXIT_CODE:
          return "Exit code: " + exitCode;
        default:
          return "Exit code: " + exitCode;
      }
    }

    @Override
    public String runError(WorkflowInstance workflowInstance, String message) {
      return "Error message: " + message;
    }

    @Override
    public String timeTrigger(WorkflowInstance workflowInstance) {
      return "Unexpected data";
    }

    @Override
    public String triggerExecution(WorkflowInstance workflowInstance, String triggerId) {
      return "Unexpected data";
    }

    @Override
    public String created(WorkflowInstance workflowInstance, String executionId, String dockerImage) {
      return "Unexpected data";
    }

    @Override
    public String started(WorkflowInstance workflowInstance) {
      return "Unexpected data";
    }

    @Override
    public String success(WorkflowInstance workflowInstance) {
      return "Unexpected data";
    }

    @Override
    public String retryAfter(WorkflowInstance workflowInstance, long delayMillis) {
      return "Unexpected data";
    }

    @Override
    public String retry(WorkflowInstance workflowInstance) {
      return "Unexpected data";
    }

    @Override
    public String stop(WorkflowInstance workflowInstance) {
      return "Unexpected data";
    }

    @Override
    public String timeout(WorkflowInstance workflowInstance) {
      return "Unexpected data";
    }

    @Override
    public String halt(WorkflowInstance workflowInstance) {
      return "Unexpected data";
    }

    @Override
    public String submit(WorkflowInstance workflowInstance, ExecutionDescription executionDescription) {
      return "Unexpected data";
    }

    @Override
    public String submitted(WorkflowInstance workflowInstance, String executionId) {
      return "Unexpected data";
    }
  }

  private enum EventDataVisitor implements EventVisitor<String> {
    INSTANCE;

    @Override
    public String timeTrigger(WorkflowInstance workflowInstance) {
      return "";
    }

    @Override
    public String triggerExecution(WorkflowInstance workflowInstance, String triggerId) {
      return String.format("Trigger id: %s", triggerId);
    }

    @Override
    public String created(WorkflowInstance workflowInstance, String executionId, String dockerImage) {
      return String.format("Execution id: %s, Docker image: %s", executionId, dockerImage);
    }

    @Override
    public String started(WorkflowInstance workflowInstance) {
      return "";
    }

    @Override
    public String terminate(WorkflowInstance workflowInstance, int exitCode) {
      return String.format("Exit code: %d", exitCode);
    }

    @Override
    public String runError(WorkflowInstance workflowInstance, String message) {
      return "Error message: " + message;
    }

    @Override
    public String success(WorkflowInstance workflowInstance) {
      return "";
    }

    @Override
    public String retryAfter(WorkflowInstance workflowInstance, long delayMillis) {
      return String.format("Delay (seconds): %d", TimeUnit.MILLISECONDS.toSeconds(delayMillis));
    }

    @Override
    public String retry(WorkflowInstance workflowInstance) {
      return "";
    }

    @Override
    public String stop(WorkflowInstance workflowInstance) {
      return "";
    }

    @Override
    public String timeout(WorkflowInstance workflowInstance) {
      return "";
    }

    @Override
    public String halt(WorkflowInstance workflowInstance) {
      return "";
    }

    @Override
    public String submit(WorkflowInstance workflowInstance, ExecutionDescription executionDescription) {
      return String.format("Execution description: %s", executionDescription);
    }

    @Override
    public String submitted(WorkflowInstance workflowInstance, String executionId) {
      return String.format("Execution id: %s", executionId);
    }
  }
}
