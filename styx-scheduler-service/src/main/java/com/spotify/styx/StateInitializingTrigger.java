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

import static com.spotify.styx.util.ParameterUtil.toParameter;

import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.util.IsClosedException;
import java.time.Instant;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link TriggerListener} that initializes a new {@link RunState}
 */
final class StateInitializingTrigger implements TriggerListener {

  private static final Logger LOG = LoggerFactory.getLogger(StateInitializingTrigger.class);

  private final StateManager stateManager;

  StateInitializingTrigger(StateManager stateManager) {
    this.stateManager = Objects.requireNonNull(stateManager);
  }

  @Override
  public void event(Workflow workflow, Trigger trigger, Instant instant,
                    TriggerParameters parameters) {
    if (workflow.configuration().dockerImage().isEmpty()) {
      LOG.warn("{} has no docker image, skipping", workflow.id());
      return;
    }

    final String parameter = toParameter(workflow.configuration().schedule(), instant);
    final WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), parameter);

    try {
      stateManager.trigger(workflowInstance, trigger, parameters);
    } catch (IsClosedException isClosedException) {
      LOG.warn("State receiver is closed when processing workflow {} for trigger {} at {}",
               workflow, trigger, instant, isClosedException);
      throw new RuntimeException(isClosedException);
    }
  }
}
