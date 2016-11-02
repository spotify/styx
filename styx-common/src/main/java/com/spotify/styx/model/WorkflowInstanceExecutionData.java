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
import java.util.Comparator;
import java.util.List;

/**
 * Value representing all execution data for a {@link WorkflowInstance}
 */
@AutoValue
public abstract class WorkflowInstanceExecutionData {

  public static final Comparator<WorkflowInstanceExecutionData> COMPARATOR =
      (a, b) -> WorkflowInstance.KEY_COMPARATOR.compare(a.workflowInstance(), b.workflowInstance());

  @JsonProperty
  public abstract WorkflowInstance workflowInstance();

  @JsonProperty
  public abstract List<Trigger> triggers();

  @JsonCreator
  public static WorkflowInstanceExecutionData create(
      @JsonProperty("workflow_instance") WorkflowInstance workflowInstance,
      @JsonProperty("triggers") List<Trigger> triggers) {
    return new AutoValue_WorkflowInstanceExecutionData(workflowInstance, triggers);
  }

  public static WorkflowInstanceExecutionData fromEvents(Iterable<SequenceEvent> events) {
    return new WFIExecutionBuilder().executionInfo(events);
  }
}
