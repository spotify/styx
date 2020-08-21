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
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.IsClosedException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlyteRunnerHandler implements OutputHandler {

  private static final Logger LOG = LoggerFactory.getLogger(FlyteRunnerHandler.class);

  @Override
  public void transitionInto(final RunState state, final EventRouter eventRouter) {
    switch (state.state()) {
      case SUBMITTING:
      case SUBMITTED:
      case RUNNING:
        if (state.data().executionDescription().isEmpty()) {
          LOG.error("Unable to start procedure. Missing execution description for " + state.workflowInstance());
          eventRouter.receiveIgnoreClosed(Event.halt(state.workflowInstance()), state.counter());
          return;
        }
        if (state.data().executionId().isEmpty()) {
          LOG.error("Unable to start procedure. Missing execution id for " + state.workflowInstance());
          eventRouter.receiveIgnoreClosed(Event.halt(state.workflowInstance()), state.counter());
          return;
        }
        if(state.data().executionDescription().get().flyteExecConf().isEmpty()) {
          return;
        }
    }
    switch (state.state()) {
      case SUBMITTING:
        LOG.info("Start flyte exec");
        final Event submitted = Event.submitted(state.workflowInstance(), state.data().executionId().get(), "flyte-runner-id");
        try {
          LOG.info("Issue submitted event");
          eventRouter.receive(submitted, state.counter());
        } catch (IsClosedException isClosedException) {
          LOG.warn("Could not emit 'submitted' event", isClosedException);
        }
        break;
      case SUBMITTED:
        final Event started = Event.started(state.workflowInstance());
        try {
          LOG.info("Issue started event");
          eventRouter.receive(started, state.counter());
        } catch (IsClosedException isClosedException) {
          LOG.warn("Could not emit 'started' event", isClosedException);
        }
        break;
      case RUNNING:
        LOG.info("Polling state");
        final Event terminate = Event.terminate(state.workflowInstance(), Optional.of(0));
        try {
          LOG.info("Issue terminate event");
          eventRouter.receive(terminate, state.counter());
        } catch (IsClosedException isClosedException) {
          LOG.warn("Could not emit 'started' event", isClosedException);
        }
        LOG.info("Done");
      default:
        // do nothing
    }
  }
}
