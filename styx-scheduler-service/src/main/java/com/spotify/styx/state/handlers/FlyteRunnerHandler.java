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

import static java.util.Objects.requireNonNull;

import com.spotify.styx.flyte.FlyteRunner;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.IsClosedException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A dummy, for now, {@link OutputHandler} for triggering Flyte's launch plans.
 */
public class FlyteRunnerHandler extends AbstractRunnerHandler {

  public static final String STATIC_RUNNER_ID = "replace-me";
  public static final int STATIC_EXIT_CODE = 0;

  private static final Logger LOG = LoggerFactory.getLogger(FlyteRunnerHandler.class);
  private final FlyteRunner flyteRunner;

  public FlyteRunnerHandler(FlyteRunner flyteRunner) {
    super(desc -> desc.flyteExecConf().isPresent());
    this.flyteRunner = requireNonNull(flyteRunner);
  }

  @Override
  public void safeTransitionInto(final RunState state, final EventRouter eventRouter) {
    switch (state.state()) {
      case SUBMITTING:
        LOG.info("Entered state SUBMITTING for: " + state.workflowInstance());

        final FlyteExecConf flyteExecConf = state.data().executionDescription().orElseThrow().flyteExecConf().orElseThrow();
        final String executionId = state.data().executionId().orElseThrow();

        try {
          LOG.info("running:{}, spec:{}, state:{}", state.workflowInstance(), flyteExecConf, state);
          flyteRunner.createExecution(executionId, flyteExecConf);
        } catch (Exception e) {
          // TODO: Figure out what exceptions to handle
          try {
            final var msg = "Failed to start execution " + state.workflowInstance();
            LOG.error(msg, e);
            eventRouter.receive(Event.runError(state.workflowInstance(), e.getMessage()), state.counter());
          } catch (IsClosedException isClosedException) {
            LOG.warn("Failed to send 'runError' event", isClosedException);
          }
          return;
        }

        // Emit `submitted` _after_ starting execution to ensure that we retry in case of failure.
        final Event submitted = Event.submitted(state.workflowInstance(), executionId,
            STATIC_RUNNER_ID);
        try {
          LOG.info("Issue 'submitted' event for: " + state.workflowInstance());
          eventRouter.receive(submitted, state.counter());
        } catch (IsClosedException isClosedException) {
          LOG.warn("Could not emit 'submitted' event for: " + state.workflowInstance(),
              isClosedException);
        }
        break;
      case SUBMITTED:
        LOG.info("Entered state SUBMITTED for: " + state.workflowInstance());
        final var started = Event.started(state.workflowInstance());
        try {
          LOG.info("Issue 'started' event for: " + state.workflowInstance());
          eventRouter.receive(started, state.counter());
        } catch (IsClosedException isClosedException) {
          LOG.warn("Could not emit 'started' event for: " + state.workflowInstance(),
              isClosedException);
        }
        break;
      case RUNNING:
        LOG.info("Entered state RUNNING for: " + state.workflowInstance());
        final var terminate = Event.terminate(state.workflowInstance(), Optional.of(STATIC_EXIT_CODE));
        try {
          LOG.info("Issue 'terminate' event for: " + state.workflowInstance());
          eventRouter.receive(terminate, state.counter());
        } catch (IsClosedException isClosedException) {
          LOG.warn("Could not emit 'terminate' event for: " + state.workflowInstance(),
              isClosedException);
        }
        break;
      default:
        // do nothing
    }
  }
}
