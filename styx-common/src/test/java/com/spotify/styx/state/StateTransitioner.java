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

package com.spotify.styx.state;

import com.google.common.collect.Maps;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import java.util.Map;

/**
 * Testing utility for transitioning states.
 */
public class StateTransitioner {

  private final OutputHandler outputHandler;
  private final Map<WorkflowInstance, RunState> states = Maps.newHashMap();

  public StateTransitioner(OutputHandler outputHandler) {
    this.outputHandler = outputHandler;
  }

  public void initialize(RunState runState) {
    states.put(runState.workflowInstance(), runState);
  }

  public void receive(Event event) {
    WorkflowInstance key = event.workflowInstance();
    RunState currentState = states.get(key);

    RunState nextState = currentState.transition(event);
    states.put(key, nextState);

    outputHandler.transitionInto(nextState);
  }

  public RunState get(WorkflowInstance workflowInstance) {
    return states.get(workflowInstance);
  }
}
