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

package com.spotify.styx.model.data;

import com.spotify.styx.model.EventVisitor;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.TriggerUtil;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

class WFIExecutionBuilder {

  private List<Trigger> triggerList = new ArrayList<>();
  private List<Execution> executionList = new ArrayList<>();
  private List<ExecStatus> executionStatusList = new ArrayList<>();

  @Nullable private WorkflowInstance currWorkflowInstance;
  @Nullable private String currExecutionId;
  @Nullable private String currTriggerId = "UNKNOWN";
  @Nullable private String currDockerImg;
  @Nullable private String currCommitSha;

  private boolean completed;

  @Nullable private Instant triggerTs;
  @Nullable private Instant eventTs;

  private final EventVisitor visitor = new Reducer();

  private enum Status {
    FAILED,
    HALTED,
    MISSING_DEPS,
    STARTED,
    SUBMITTED,
    SUCCESS,
    TIMEOUT
  }

  private void closeExecution() {
    final Execution execution = Execution.create(
        Optional.ofNullable(currExecutionId),
        Optional.ofNullable(currDockerImg),
        Optional.ofNullable(currCommitSha),
        executionStatusList);
    executionList.add(execution);

    executionStatusList = new ArrayList<>();
    currExecutionId = null;
    currDockerImg = null;
    currCommitSha = null;
  }

  private void closeTrigger() {
    if (!executionStatusList.isEmpty()) {
      closeExecution();
    }

    final Trigger trigger = Trigger.create(currTriggerId, triggerTs, completed, executionList);

    triggerList.add(trigger);
    executionList = new ArrayList<>();
  }

  private class Reducer implements EventVisitor<Void> {

    @Override
    public Void timeTrigger(WorkflowInstance workflowInstance) {
      currWorkflowInstance = workflowInstance;
      completed = false;

      triggerTs = eventTs;
      return null;
    }

    @Override
    public Void triggerExecution(WorkflowInstance workflowInstance, com.spotify.styx.state.Trigger trigger) {
      currWorkflowInstance = workflowInstance;
      completed = false;

      currTriggerId = TriggerUtil.triggerId(trigger);
      triggerTs = eventTs;
      return null;
    }

    @Override
    public Void info(WorkflowInstance workflowInstance, Message message) {
      currWorkflowInstance = workflowInstance;
      return null;
    }

    @Override
    public Void dequeue(WorkflowInstance workflowInstance, Set<String> resourceIds) {
      currWorkflowInstance = workflowInstance;
      return null;
    }

    @Override
    public Void created(WorkflowInstance workflowInstance, String executionId, String dockerImage) {
      currWorkflowInstance = workflowInstance;
      currExecutionId = executionId;
      currDockerImg = dockerImage;

      executionStatusList.add(ExecStatus.create(eventTs, Status.SUBMITTED.toString(),
          Optional.empty()));
      return null;
    }

    @Override
    public Void submit(WorkflowInstance workflowInstance, ExecutionDescription executionDescription,
        String executionId) {
      currWorkflowInstance = workflowInstance;
      currDockerImg = executionDescription.dockerImage();
      if (executionDescription.commitSha().isPresent()) {
        currCommitSha = executionDescription.commitSha().get();
      }
      currExecutionId = executionId;

      return null;
    }

    @Override
    public Void submitted(WorkflowInstance workflowInstance, String executionId) {
      currWorkflowInstance = workflowInstance;
      currExecutionId = executionId;

      executionStatusList.add(ExecStatus.create(eventTs, Status.SUBMITTED.toString(),
          Optional.empty()));
      return null;
    }

    @Override
    public Void started(WorkflowInstance workflowInstance) {
      currWorkflowInstance = workflowInstance;

      executionStatusList.add(ExecStatus.create(eventTs, Status.STARTED.toString(),
          Optional.empty()));
      return null;
    }

    @Override
    public Void terminate(WorkflowInstance workflowInstance, Optional<Integer> exitCode) {
      currWorkflowInstance = workflowInstance;

      final Status status = exitCode.map(c -> {
        if (c == 0) {
          return Status.SUCCESS;
        } else if (c == RunState.MISSING_DEPS_EXIT_CODE) {
          return Status.MISSING_DEPS;
        } else {
          return Status.FAILED;
        }
      }).orElse(Status.FAILED);

      final Optional<String> message;
      if (Status.FAILED == status) {
        message = exitCode
            .map(c -> Optional.of("Exit code: " + c))
            .orElse(Optional.of("Exit code unknown"));
      } else {
        message = Optional.empty();
      }

      executionStatusList.add(ExecStatus.create(eventTs, status.toString(), message));

      closeExecution();
      return null;
    }

    @Override
    public Void runError(WorkflowInstance workflowInstance, String message) {
      currWorkflowInstance = workflowInstance;

      executionStatusList.add(ExecStatus.create(eventTs, Status.FAILED.toString(),
          Optional.ofNullable(message)));

      closeExecution();
      return null;
    }

    @Override
    public Void success(WorkflowInstance workflowInstance) {
      currWorkflowInstance = workflowInstance;
      completed = true;

      closeTrigger();
      return null;
    }

    @Override
    public Void retryAfter(WorkflowInstance workflowInstance, long delayMillis) {
      currWorkflowInstance = workflowInstance;
      return null;
    }

    @Override
    public Void retry(WorkflowInstance workflowInstance) {
      currWorkflowInstance = workflowInstance;
      return null;
    }

    @Override
    public Void stop(WorkflowInstance workflowInstance) {
      currWorkflowInstance = workflowInstance;
      completed = true;

      closeTrigger();
      return null;
    }

    @Override
    public Void timeout(WorkflowInstance workflowInstance) {
      currWorkflowInstance = workflowInstance;

      // we might get timeout before triggerExecution, and in that case we take best effort to
      // set trigger timestamp
      if (triggerTs == null) {
        triggerTs = eventTs;
        return null;
      }

      executionStatusList.add(ExecStatus.create(eventTs, Status.TIMEOUT.toString(),
          Optional.empty()));

      closeExecution();
      return null;
    }

    @Override
    public Void halt(WorkflowInstance workflowInstance) {
      currWorkflowInstance = workflowInstance;
      completed = true;

      executionStatusList.add(ExecStatus.create(eventTs, Status.HALTED.toString(),
          Optional.empty()));

      closeTrigger();
      return null;
    }
  }

  WorkflowInstanceExecutionData executionInfo(Iterable<SequenceEvent> events) {
    for (SequenceEvent sequenceEvent : events) {
      eventTs = Instant.ofEpochMilli(sequenceEvent.timestamp());
      sequenceEvent.event().accept(visitor);
    }

    if (!completed) {
      closeTrigger();
    }

    return WorkflowInstanceExecutionData.create(currWorkflowInstance, triggerList);
  }
}

