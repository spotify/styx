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
import com.spotify.styx.util.IsClosedException;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for driving active {@link RunState} instances by sending them {@link Event}s.
 */
public interface StateManager extends Closeable {

  Logger LOG = LoggerFactory.getLogger(StateManager.class);

  /**
   * Triggers a workflow instance to run.
   *
   * @throws IsClosedException if the state receiver is closed and can not handle events
   */
  CompletableFuture<Void> trigger(WorkflowInstance workflowInstance, Trigger trigger) throws IsClosedException;

  /**
   * Receive an {@link Event} and route it to the corresponding active {@link RunState} based on
   * the {@link Event#workflowInstance()} key of the event.
   *
   * @param event The event to receive
   * @throws IsClosedException if the state receiver is closed and can not handle events
   */
  CompletionStage<Void> receive(Event event) throws IsClosedException;

  /**
   * Get a map of all active {@link WorkflowInstance} states.
   */
  Map<WorkflowInstance, RunState> activeStates();

  /**
   * Returns the number of queued, unprocessed events. These are events that are sent to
   * {@link #receive(Event)} or {@link #receiveIgnoreClosed(Event)}, and are pending.
   */
  long getQueuedEventsCount();

  /**
   * Like {@link #receive(Event)} but ignoring the {@link IsClosedException} exception.
   *
   * @param event The event to receive
   */
  default void receiveIgnoreClosed(Event event) {
    try {
      receive(event);
    } catch (IsClosedException isClosedException) {
      LOG.info("Ignored event, state receiver closed", isClosedException);
    }
  }

  /**
   * Get the current {@link RunState} of a {@link WorkflowInstance}.
   *
   * @param workflowInstance Workflow instance
   * @return The RunState associated with the workflow instance
   */
  RunState get(WorkflowInstance workflowInstance);
}
