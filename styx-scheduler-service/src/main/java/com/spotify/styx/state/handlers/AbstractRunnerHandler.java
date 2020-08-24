/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2020 Spotify AB
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

package com.spotify.styx.state.handlers;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import java.util.function.Predicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract {@link OutputHandler}  starts docker runs on
 * {@link RunState.State#SUBMITTED} transitions
 */
abstract class AbstractRunnerHandler implements OutputHandler {
  private static final Logger LOG = LoggerFactory.getLogger(AbstractRunnerHandler.class);
  private final Predicate<ExecutionDescription> applicableFn;

  protected AbstractRunnerHandler(Predicate<ExecutionDescription> applicableFn) {
    this.applicableFn = applicableFn;
  }

  @Override
  public final void transitionInto(RunState state, EventRouter eventRouter) {
    var stateData = state.data();
    switch (state.state()) {
      case SUBMITTING:
      case SUBMITTED:
      case RUNNING:
        var workflowInstance = state.workflowInstance();
        if (stateData.executionId().isEmpty()) {
          LOG.error("Unable to start procedure. Missing execution id for " + workflowInstance);
          eventRouter.receiveIgnoreClosed(Event.halt(workflowInstance), state.counter());
          return;
        }
        var maybeExecutionDescription = stateData.executionDescription();
        if (maybeExecutionDescription.isEmpty()) {
          LOG.error("Unable to start procedure. Missing execution description for " + workflowInstance);
          eventRouter.receiveIgnoreClosed(Event.halt(workflowInstance), state.counter());
          return;
        }
        if(!applicableFn.test(maybeExecutionDescription.orElseThrow())) {
          return;
        }
    }

    safeTransitionInto(state, eventRouter);
  }

  /**
   * Same as {@link #transitionInto(RunState, EventRouter)} but subclasses can trust that {@link RunState}'s
   * {@link StateData#executionId()} and {@link StateData#executionDescription()} are both present when
   * {@link RunState#state()} is {@link RunState.State#SUBMITTING}, {@link RunState.State#SUBMITTED} or
   * {@link RunState.State#RUNNING}.
   */
  protected abstract void safeTransitionInto(RunState state, EventRouter eventRouter);
}
