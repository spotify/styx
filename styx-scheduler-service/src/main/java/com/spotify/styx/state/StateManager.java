/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx.state;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import java.io.Closeable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for driving active {@link RunState} instances by sending them {@link Event}s.
 */
public interface StateManager extends Closeable {

  /**
   * Initializes a {@link RunState} which makes it actively tracked.
   *
   * @param runState The state to initialize
   * @throws IsClosed if the state receiver is closed and can not handle events
   */
  void initialize(RunState runState) throws IsClosed;

  /**
   * Restore a {@link RunState} and track it from the given sequence count.
   *
   * @param runState The state to initialize
   * @param count    The sequence count to restore the state at
   */
  void restore(RunState runState, long count);

  /**
   * Receive an {@link Event} and route it to the corresponding active {@link RunState} based on
   * the {@link Event#workflowInstance()} key of the event.
   *
   * @param event The event to receive
   * @throws IsClosed if the state receiver is closed and can not handle events
   */
  void receive(Event event) throws IsClosed;

  /**
   * Returns the number of current active {@link RunState}.
   */
  long getActiveStatesCount();

  /**
   * Returns the number of queued, unprocessed events. These are events that are sent to
   * {@link #receive(Event)} or {@link #receiveIgnoreClosed(Event)}, and are pending.
   */
  long getQueuedEventsCount();

  /**
   * Returns the number of current active {@link RunState} for a specific {@link WorkflowId}.
   */
  long getActiveStatesCount(WorkflowId workflowId);

  /**
   * Check if a {@link WorkflowInstance} is currently active.
   *
   * @param workflowInstance The {@link WorkflowInstance} to inspect
   * @return A boolean indicating if the {@link WorkflowInstance} is active
   */
  boolean isActiveWorkflowInstance(WorkflowInstance workflowInstance);

  /**
   * Like {@link #receive(Event)} but ignoring the {@link IsClosed} exception.
   *
   * @param event The event to receive
   */
  default void receiveIgnoreClosed(Event event) {
    try {
      receive(event);
    } catch (IsClosed isClosed) {
      LOG.info("Ignored event, state receiver closed", isClosed);
    }
  }

  /**
   * Get the current {@link RunState} of a {@link WorkflowInstance}.
   *
   * @param workflowInstance Workflow instance
   * @return The RunState associated with the workflow instance
   */
  RunState get(WorkflowInstance workflowInstance);

  /**
   * Exception that signals that the {@link StateManager} is in a closed state.
   */
  class IsClosed extends Exception {

  }

  Logger LOG = LoggerFactory.getLogger(StateManager.class);
}
