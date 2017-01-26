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

import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;
import static org.fusesource.jansi.Ansi.ansi;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.styx.api.cli.ActiveStatesPayload.ActiveState;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventVisitor;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.util.TriggerUtil;
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

  private CliUtil() {
    // no instantiation
  }

  static Ansi colored(Ansi.Color color, Object obj) {
    return ansi().fg(color).a(obj).reset();
  }

  static Ansi coloredBright(Ansi.Color color, Object obj) {
    return ansi().fgBright(color).a(obj).reset();
  }

  static SortedMap<WorkflowId, SortedSet<ActiveState>> groupActiveStates(List<ActiveState> activeStates) {
    return activeStates.stream()
        .collect(groupingBy(
            activeState -> activeState.workflowInstance().workflowId(),
            CliUtil::newSortedWorkflowIdSet,
            toCollection(CliUtil::newSortedActiveStateSet)
        ));
  }

  private static TreeSet<ActiveState> newSortedActiveStateSet() {
    return Sets.newTreeSet(ActiveState.PARAMETER_COMPARATOR);
  }

  private static TreeMap<WorkflowId, SortedSet<ActiveState>> newSortedWorkflowIdSet() {
    return Maps.newTreeMap(WorkflowId.KEY_COMPARATOR);
  }

  static String formatTimestamp(long timestamp) {
    return Instant
        .ofEpochMilli(timestamp)
        .atZone(ZoneId.of("UTC"))
        .toLocalDateTime()
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));
  }

  public static String info(Event event) {
    return event.accept(EventInfoVisitor.INSTANCE);
  }

  private enum EventInfoVisitor implements EventVisitor<String> {
    INSTANCE;

    @Override
    public String timeTrigger(WorkflowInstance workflowInstance) {
      return "";
    }

    @Override
    public String triggerExecution(WorkflowInstance workflowInstance, Trigger trigger) {
      return String.format("Trigger id: %s", TriggerUtil.triggerId(trigger));
    }

    @Override
    public String info(WorkflowInstance workflowInstance, Message message) {
      return message.line();
    }

    @Override
    public String created(WorkflowInstance workflowInstance, String executionId, String dockerImage) {
      return String.format("Execution id: %s, Docker image: %s", executionId, dockerImage);
    }

    @Override
    public String dequeue(WorkflowInstance workflowInstance) {
      return "";
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
