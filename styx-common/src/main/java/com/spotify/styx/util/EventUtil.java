/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.util;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventVisitor;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.Trigger;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Utility for getting information about {@link Event}s
 */
public final class EventUtil {

  private EventUtil() {
    throw new UnsupportedOperationException();
  }

  public static String name(Event event) {
    return event.accept(EventNameVisitor.INSTANCE);
  }

  public static String info(Event event) {
    return event.accept(EventInfoVisitor.INSTANCE);
  }

  /**
   * An {@link EventVisitor} for extracting the info of an {@link Event}.
   */
  private enum EventInfoVisitor implements EventVisitor<String> {
    INSTANCE;

    @Override
    public String triggerExecution(WorkflowInstance workflowInstance, Trigger trigger,
        TriggerParameters parameters) {
      return String.format("Trigger id: %s, Parameters: %s", TriggerUtil.triggerId(trigger),
          Json.deterministicStringUnchecked(parameters));
    }

    @Override
    public String info(WorkflowInstance workflowInstance, Message message) {
      return message.line();
    }

    @Override
    public String dequeue(WorkflowInstance workflowInstance, Set<String> resourceIds) {
      return "";
    }

    @Override
    public String started(WorkflowInstance workflowInstance) {
      return "";
    }

    @Override
    public String terminate(WorkflowInstance workflowInstance, Optional<Integer> exitCode) {
      return "Exit code: " + exitCode.map(String::valueOf).orElse("-");
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
    public String submit(WorkflowInstance workflowInstance, ExecutionDescription executionDescription,
        String executionId) {
      return String.format("Execution description: %s, id: %s", executionDescription, executionId);
    }

    @Override
    public String submitted(WorkflowInstance workflowInstance, String executionId, String runnerId) {
      return String.format("Execution id: %s, runner id: %s", executionId, runnerId);
    }
  }

  /**
   * An {@link EventVisitor} for extracting the name of an {@link Event}.
   */
  private enum EventNameVisitor implements EventVisitor<String> {
    INSTANCE;

    @Override
    public String triggerExecution(WorkflowInstance workflowInstance, Trigger trigger,
        TriggerParameters parameters) {
      return "triggerExecution";
    }

    @Override
    public String info(WorkflowInstance workflowInstance, Message message) {
      return "info";
    }

    @Override
    public String dequeue(WorkflowInstance workflowInstance, Set<String> resourceIds) {
      return "dequeue";
    }

    @Override
    public String started(WorkflowInstance workflowInstance) {
      return "started";
    }

    @Override
    public String terminate(WorkflowInstance workflowInstance, Optional<Integer> exitCode) {
      return "terminate";
    }

    @Override
    public String runError(WorkflowInstance workflowInstance, String message) {
      return "runError";
    }

    @Override
    public String success(WorkflowInstance workflowInstance) {
      return "success";
    }

    @Override
    public String retryAfter(WorkflowInstance workflowInstance, long delayMillis) {
      return "retryAfter";
    }

    @Override
    public String stop(WorkflowInstance workflowInstance) {
      return "stop";
    }

    @Override
    public String timeout(WorkflowInstance workflowInstance) {
      return "timeout";
    }

    @Override
    public String halt(WorkflowInstance workflowInstance) {
      return "halt";
    }

    @Override
    public String submit(WorkflowInstance workflowInstance, ExecutionDescription executionDescription,
        String executionId) {
      return "submit";
    }

    @Override
    public String submitted(WorkflowInstance workflowInstance, String executionId, String runnerId) {
      return "submitted";
    }
  }
}
