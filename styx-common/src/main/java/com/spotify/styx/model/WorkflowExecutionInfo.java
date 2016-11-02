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

package com.spotify.styx.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import java.time.Instant;
import java.util.Comparator;
import java.util.Optional;

/**
 * Instances of WorkflowExecutionInfo holds information about a particular
 * Workflow execution.
 */
@AutoValue
public abstract class WorkflowExecutionInfo {

  public static final Comparator<WorkflowExecutionInfo> WHEN_COMPARATOR =
      (a, b) -> a.when().compareTo(b.when());

  @JsonProperty
  public abstract WorkflowInstance workflowInstance();

  @JsonProperty
  public abstract Instant when();

  @JsonProperty
  public abstract ExecutionStatus executionStatus();

  @JsonProperty
  public abstract Optional<String> executionId();

  public String toKey() {
    return workflowInstance().toKey() + "#" + when().toString();
  }

  @JsonCreator
  public static WorkflowExecutionInfo create(
      @JsonProperty("workflow_instance") WorkflowInstance workflowInstance,
      @JsonProperty("when") Instant when,
      @JsonProperty("execution_status") ExecutionStatus executionStatus,
      @JsonProperty("execution_id") Optional<String> executionId) {
    return new AutoValue_WorkflowExecutionInfo(
        workflowInstance,
        when, executionStatus,
        executionId);
  }

  public static WorkflowExecutionInfo parseKey(
      String key,
      ExecutionStatus executionStatus,
      String executionId) {
    final int lastHashPos = key.lastIndexOf('#');
    if (lastHashPos < 1) {
      throw new IllegalArgumentException("Key must contain a hash '#' sign on position > 0");
    }

    final WorkflowInstance workflowInstance = WorkflowInstance.parseKey(key.substring(0, lastHashPos));
    final Instant when = Instant.parse(key.substring(lastHashPos + 1));
    return create(workflowInstance, when, executionStatus, Optional.of(executionId));
  }
}
