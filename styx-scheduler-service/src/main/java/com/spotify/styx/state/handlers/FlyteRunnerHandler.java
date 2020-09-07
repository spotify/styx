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

import androidx.annotation.VisibleForTesting;
import com.spotify.styx.flyte.FlyteRunner;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.IsClosedException;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link OutputHandler} for triggering Flyte's launch plans.
 */
public class FlyteRunnerHandler extends AbstractRunnerHandler {

  public static final String STATIC_RUNNER_ID = "replace-me";
  public static final int STATIC_EXIT_CODE = 0;

  private static final Logger LOG = LoggerFactory.getLogger(FlyteRunnerHandler.class);
  private final FlyteRunner flyteRunner;
  private final Function<String, String> styxExecIdToFlyteNameMapper;

  public FlyteRunnerHandler(FlyteRunner flyteRunner) {
    this(flyteRunner, new StyxIdToFlyteExecNameMapper());
  }

  @VisibleForTesting
  FlyteRunnerHandler(FlyteRunner flyteRunner, Function<String, String> styxExecIdToFlyteNameMapper) {
    super(desc -> desc.flyteExecConf().isPresent());
    this.flyteRunner = requireNonNull(flyteRunner);
    this.styxExecIdToFlyteNameMapper = requireNonNull(styxExecIdToFlyteNameMapper);
  }

  @Override
  public void safeTransitionInto(final RunState state, final EventRouter eventRouter) {
    if (!flyteRunner.isEnabled()) {
      var workflowInstance = state.workflowInstance();
      LOG.error("Unable to transition {} into {}. Flyte system is not available", workflowInstance, state.state());
      eventRouter.receiveIgnoreClosed(Event.halt(workflowInstance), state.counter());
      return;
    }

    switch (state.state()) {
      case SUBMITTING:
        transitionSubmitting(state, eventRouter);
        break;
      case SUBMITTED:
        transitionSubmitted(state, eventRouter);
        break;
      case RUNNING:
        transitionRunning(state, eventRouter);
        break;
      default:
        // do nothing
    }
  }

  private void transitionSubmitting(RunState state, EventRouter eventRouter) {
    LOG.info("Entered state SUBMITTING for: " + state.workflowInstance());

    final FlyteExecConf flyteExecConf = state.data().executionDescription().orElseThrow().flyteExecConf().orElseThrow();
    final String executionId = state.data().executionId().orElseThrow();
    final String execName = styxExecIdToFlyteNameMapper.apply(executionId);

    try {
      LOG.info("running:{}, conf:{}, state:{}, flyte exec name:{}",
          state.workflowInstance(), flyteExecConf, state, execName);
      flyteRunner.createExecution(execName, flyteExecConf);
    } catch (Exception e) {
      try {
        final var errMessage = "Failed to start execution for " + state.workflowInstance();
        LOG.error(errMessage, e);
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
  }

  private void transitionSubmitted(RunState state, EventRouter eventRouter) {
    LOG.info("Entered state SUBMITTED for: " + state.workflowInstance());
    final var started = Event.started(state.workflowInstance());
    try {
      LOG.info("Issue 'started' event for: " + state.workflowInstance());
      eventRouter.receive(started, state.counter());
    } catch (IsClosedException isClosedException) {
      LOG.warn("Could not emit 'started' event for: " + state.workflowInstance(),
          isClosedException);
    }
  }

  private void transitionRunning(RunState state, EventRouter eventRouter) {
    LOG.info("Entered state RUNNING for: " + state.workflowInstance());
    final var terminate = Event.terminate(state.workflowInstance(), Optional.of(STATIC_EXIT_CODE));
    try {
      LOG.info("Issue 'terminate' event for: " + state.workflowInstance());
      eventRouter.receive(terminate, state.counter());
    } catch (IsClosedException isClosedException) {
      LOG.warn("Could not emit 'terminate' event for: " + state.workflowInstance(),
          isClosedException);
    }
  }

}
