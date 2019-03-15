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
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.util.IsClosedException;
import java.io.Closeable;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.function.Predicate;
import io.vavr.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for driving active {@link RunState} instances by sending them {@link Event}s.
 */
public interface StateManager extends Closeable {

  Logger LOG = LoggerFactory.getLogger(StateManager.class);

  /**
   * Drive all active instances forward by re-invoking output handlers. Should be called regularly to
   * ensure instances do not get stuck due to ephemeral output handler failure.
   */
  void tick();

  /**
   * Triggers a workflow instance to run.
   *
   * @throws IsClosedException if the state receiver is closed and can not handle events
   */
  CompletionStage<Void> trigger(WorkflowInstance workflowInstance, Trigger trigger,
      TriggerParameters parameters) throws IsClosedException;

  /**
   * Receive an {@link Event} and route it to the corresponding active {@link RunState} based on
   * the {@link Event#workflowInstance()} key of the event.
   *
   * @param event The event to receive
   * @throws IsClosedException if the state receiver is closed and can not handle events
   */
  CompletionStage<Void> receive(Event event) throws IsClosedException;

  /**
   * Receive an {@link Event} and route it to the corresponding active {@link RunState} based on
   * the {@link Event#workflowInstance()} key of the event.
   *
   * @param event   The event to receive
   * @param counter The state counter upon which the event must act upon
   * @throws IsClosedException if the state receiver is closed and can not handle events
   */
  CompletionStage<Void> receive(Event event, long counter) throws IsClosedException;

  /**
   * Get a map of all active {@link WorkflowInstance} states filtered by triggerId.
   */
  Map<WorkflowInstance, RunState> getActiveStatesByTriggerId(String triggerId);

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
   * Like {@link #receive(Event)} but ignoring the {@link IsClosedException} exception.
   *
   * @param event The event to receive
   * @param counter The state counter upon which the event must act upon
   */
  default void receiveIgnoreClosed(Event event, long counter) {
    try {
      receive(event, counter);
    } catch (IsClosedException isClosedException) {
      LOG.info("Ignored event, state receiver closed", isClosedException);
    }
  }

  /**
   * Get a map of all active {@link WorkflowInstance} states.
   */
  Map<WorkflowInstance, RunState> getActiveStates();

  /**
   * Get a partial map of all active {@link WorkflowInstance} states with a predicate that indicates if a workflow
   * instance is unavailable.
   */
  Tuple2<Predicate<WorkflowInstance>, Map<WorkflowInstance, RunState>> getActiveStatesPartial();

  /**
   * Get the current {@link RunState} of a {@link WorkflowInstance}.
   *
   * @param workflowInstance Workflow instance
   * @return The RunState associated with the workflow instance
   */
  Optional<RunState> getActiveState(WorkflowInstance workflowInstance);
}
