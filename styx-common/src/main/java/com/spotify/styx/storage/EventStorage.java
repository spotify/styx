/*
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
package com.spotify.styx.storage;

import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowInstance;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;

/**
 * Interface for storing events in persistence layer
 */
public interface EventStorage {

  /**
   * Returns all {@link SequenceEvent} for a {@link WorkflowInstance} in time order.
   *
   * @param workflowInstance  The workflow instance to get the events for
   */
  SortedSet<SequenceEvent> readEvents(WorkflowInstance workflowInstance) throws IOException;

  /**
   * Stores an {@link com.spotify.styx.model.Event}.
   *
   * @param sequenceEvent  The event together with its sequence number to be stored
   */
  void writeEvent(SequenceEvent sequenceEvent) throws IOException;

  /**
   * Returns the latest counter from the events of a {@link WorkflowInstance}. The returned
   * Optional is empty if no event is found for the {@link WorkflowInstance} specified.
   *
   * @param workflowInstance  The workflow instance to get the latest counter for
   */
  Optional<Long> getLatestStoredCounter(WorkflowInstance workflowInstance) throws IOException;

  /**
   * Stores information about an active {@link WorkflowInstance} to be tracked.
   *
   * @param workflowInstance  The {@link WorkflowInstance} that entered an active state
   * @param counter           The last processed event count for the {@link WorkflowInstance}
   */
  void writeActiveState(WorkflowInstance workflowInstance, long counter) throws IOException;

  /**
   * Removes a reference to active {@link WorkflowInstance}, to be called when the instance enters
   * a final state in Styx and it shouldn't be tracked anymore.
   *
   * @param workflowInstance  The {@link WorkflowInstance} that entered a final state
   */
  void deleteActiveState(WorkflowInstance workflowInstance) throws IOException;

  /**
   * Return a map of all active {@link WorkflowInstance}s to their last consumed sequence count.
   *
   * A {@link WorkflowInstance} is active if there has been at least one call to
   *
   * {@link #writeActiveState(WorkflowInstance, long)} and no calls to
   * {@link #deleteActiveState(WorkflowInstance)}.
   *
   * @return The map of workflow instances to sequence counts
   */
  Map<WorkflowInstance, Long> readActiveWorkflowInstances() throws IOException;
}
