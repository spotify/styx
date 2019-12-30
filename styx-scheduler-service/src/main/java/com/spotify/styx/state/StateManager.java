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
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Interface for driving active {@link RunState} instances by sending them {@link Event}s.
 */
public interface StateManager extends EventRouter, Closeable {

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
  void trigger(WorkflowInstance workflowInstance, Trigger trigger,
      TriggerParameters parameters) throws IsClosedException;

  /**
   * Get a map of all active {@link WorkflowInstance} states filtered by triggerId.
   */
  Map<WorkflowInstance, RunState> getActiveStatesByTriggerId(String triggerId);

  /**
   * Get a list of all active {@link WorkflowInstance}s.
   */
  Set<WorkflowInstance> listActiveInstances();

  /**
   * Get a map of all active {@link WorkflowInstance} states.
   */
  Map<WorkflowInstance, RunState> getActiveStates();

  /**
   * Get the current {@link RunState} of a {@link WorkflowInstance}.
   *
   * @param workflowInstance Workflow instance
   * @return The RunState associated with the workflow instance
   */
  Optional<RunState> getActiveState(WorkflowInstance workflowInstance);
}
