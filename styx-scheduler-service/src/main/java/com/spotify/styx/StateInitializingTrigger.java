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

package com.spotify.styx;

import com.google.common.base.Throwables;
import com.spotify.styx.StyxScheduler.StateFactory;
import com.spotify.styx.docker.WorkflowValidator;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.workflow.ParameterUtil;
import java.io.IOException;
import java.time.Instant;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TriggerListener} that initializes a new {@link RunState}
 */
final class StateInitializingTrigger implements TriggerListener {

  private static final Logger LOG = LoggerFactory.getLogger(StateInitializingTrigger.class);

  private final StateFactory stateFactory;
  private final StateManager stateManager;
  private final Storage storage;

  StateInitializingTrigger(StateFactory stateFactory, StateManager stateManager, Storage storage) {
    this.stateFactory = Objects.requireNonNull(stateFactory);
    this.stateManager = Objects.requireNonNull(stateManager);
    this.storage = Objects.requireNonNull(storage);
  }

  @Override
  public void event(Workflow workflow, String triggerId, Instant instant) {
    if (!WorkflowValidator.hasDockerConfiguration(workflow, storage)) {
      LOG.warn("{} has no docker image or args info, skipping", workflow.id());
      return;
    }

    final String parameter = toParameter(workflow.schedule().partitioning(), instant);
    final WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), parameter);
    final RunState initialState = stateFactory.apply(workflowInstance);

    try {
      storage.store(workflowInstance);
    } catch (IOException e) {
      LOG.warn("Could not persist workflow instance", e);
      throw Throwables.propagate(e);
    }

    try {
      stateManager.initialize(initialState);
      stateManager.receive(Event.triggerExecution(workflowInstance, triggerId));
    } catch (StateManager.IsClosed isClosed) {
      LOG.warn("State receiver is closed", isClosed);
    }
  }

  private static String toParameter(Partitioning partitioning, Instant instant) {
    switch (partitioning) {
      case DAYS:
      case WEEKS:
        return ParameterUtil.formatDate(instant);
      case HOURS:
        return ParameterUtil.formatDateHour(instant);
      case MONTHS:
        return ParameterUtil.formatMonth(instant);

      default:
        throw new IllegalArgumentException("Unknown partitioning " + partitioning);
    }
  }
}
