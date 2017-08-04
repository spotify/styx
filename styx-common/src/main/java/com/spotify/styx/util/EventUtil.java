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
import com.spotify.styx.model.WorkflowInstance;
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
    public String dequeue(WorkflowInstance workflowInstance, Set<String> resources) {
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
    public String submit(WorkflowInstance workflowInstance, ExecutionDescription executionDescription,
        String executionId) {
      return String.format("Execution description: %s, id: %s", executionDescription, executionId);
    }

    @Override
    public String submitted(WorkflowInstance workflowInstance, String executionId) {
      return String.format("Execution id: %s", executionId);
    }
  }

  /**
   * An {@link EventVisitor} for extracting the name of an {@link Event}.
   */
  private enum EventNameVisitor implements EventVisitor<String> {
    INSTANCE;

    @Override
    public String timeTrigger(WorkflowInstance workflowInstance) {
      return "timeTrigger";
    }

    @Override
    public String triggerExecution(WorkflowInstance workflowInstance, Trigger trigger) {
      return "triggerExecution";
    }

    @Override
    public String info(WorkflowInstance workflowInstance, Message message) {
      return "info";
    }

    @Override
    public String dequeue(WorkflowInstance workflowInstance, Set<String> resources) {
      return "dequeue";
    }

    @Override
    public String created(WorkflowInstance workflowInstance, String executionId, String dockerImage) {
      return "created";
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
    public String retry(WorkflowInstance workflowInstance) {
      return "retry";
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
    public String submitted(WorkflowInstance workflowInstance, String executionId) {
      return "submitted";
    }
  }
}
