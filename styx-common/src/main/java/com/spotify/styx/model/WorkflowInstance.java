/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2017 Spotify AB
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An instantiation of a {@link Workflow}.
 */
@AutoValue
public abstract class WorkflowInstance {

  public static final Comparator<WorkflowInstance> KEY_COMPARATOR =
      Comparator.comparing(WorkflowInstance::toKey);
  private static final Logger LOG = LoggerFactory.getLogger(WorkflowInstance.class);

  @JsonProperty
  public abstract WorkflowId workflowId();

  /**
   * This property contains the thing needed to turn a general purpose Workflow into something
   * that satisfies a specific workflow instance. Might be for example
   * '-Pdatehour=2016-01-01T08'.
   */
  @JsonProperty
  public abstract String parameter();

  public String toKey() {
    // Used as ID in storage etc. Do not change.
    return workflowId().toKey() + "#" + parameter();
  }

  @JsonCreator
  public static WorkflowInstance create(
      @JsonProperty("workflow_id") WorkflowId workflowId,
      @JsonProperty("parameter") String parameter) {
    return new AutoValue_WorkflowInstance(workflowId, parameter);
  }

  public static WorkflowInstance parseKey(String key) {
    final int lastHashPos = key.lastIndexOf('#');
    if (lastHashPos < 1) {
      LOG.error("Invalid workflow instance key {}", key);
      throw new IllegalArgumentException("Key must contain a hash '#' sign on position > 0");
    }

    final WorkflowId workflowId = WorkflowId.parseKey(key.substring(0, lastHashPos));
    return create(workflowId, key.substring(lastHashPos + 1));
  }

  @Override
  public String toString() {
    // Ensure that the key gets printed when containing objects are logged
    return toKey();
  }
}
