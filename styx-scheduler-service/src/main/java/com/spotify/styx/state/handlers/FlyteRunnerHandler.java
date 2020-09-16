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
import com.spotify.styx.flyte.FlyteExecutionId;
import com.spotify.styx.flyte.FlyteRunner;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.IsClosedException;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link OutputHandler} for triggering Flyte's launch plans.
 */
public class FlyteRunnerHandler extends AbstractRunnerHandler {

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
      case RUNNING:
        pollingExecution(state, eventRouter);
        break;
      case ERROR:
        cleanUpExecution(state);
      default:
        // do nothing
    }
  }

  private void transitionSubmitting(RunState state, EventRouter eventRouter) {
    LOG.info("Entered state SUBMITTING for: " + state.workflowInstance());

    final FlyteExecConf flyteExecConf = state.data().executionDescription().orElseThrow().flyteExecConf().orElseThrow();
    final String executionId = state.data().executionId().orElseThrow();
    final String execName = styxExecIdToFlyteNameMapper.apply(executionId);

    final String runnerId;
    try {
      LOG.info("running:{}, conf:{}, state:{}, flyte exec name:{}",
          state.workflowInstance(), flyteExecConf, state, execName);
      runnerId = flyteRunner.createExecution(state, execName, flyteExecConf);
    } catch (Exception e) {
      final var errMessage = "Failed to start execution for " + state.workflowInstance();
      LOG.error(errMessage, e);
      sendEventIgnoreClosed(state, eventRouter, "runError",
          Event.runError(state.workflowInstance(), e.getMessage()));
      return;
    }

    // Emit `submitted` _after_ starting execution to ensure that we retry in case of failure.
    final Event submitted = Event.submitted(state.workflowInstance(), executionId, runnerId);
    LOG.info("Issue 'submitted' event for: " + state.workflowInstance());
    sendEventIgnoreClosed(state, eventRouter, "submitted", submitted);
  }

  private void pollingExecution(RunState state, EventRouter eventRouter) {
    LOG.info("Entered state" + state.state().toString() + " for: " + state.workflowInstance());

    try {
      flyteRunner.poll(getExecutionId(state), state);
    } catch (Exception e) {
      final var errMessage = "Failed to poll execution for " + state.workflowInstance();
      LOG.error(errMessage, e);
      sendEventIgnoreClosed(state, eventRouter, "runError",
          Event.runError(state.workflowInstance(), e.getMessage()));
    }
  }

  private void cleanUpExecution(RunState state) {
    flyteRunner.terminateExecution(state, getExecutionId(state));
  }

  private FlyteExecutionId getExecutionId(RunState state) {
    final FlyteExecConf flyteExecConf = state.data().executionDescription().orElseThrow().flyteExecConf().orElseThrow();
    final String executionId = state.data().executionId().orElseThrow();
    final String execName = styxExecIdToFlyteNameMapper.apply(executionId);
    return getFlyteExecutionId(flyteExecConf, execName);
  }

  private FlyteExecutionId getFlyteExecutionId(FlyteExecConf flyteExecConf, String execName) {
    return FlyteExecutionId.create(
        flyteExecConf.referenceId().project(),
        flyteExecConf.referenceId().domain(),
        execName);
  }

  private void sendEventIgnoreClosed(RunState state, EventRouter eventRouter, String eventType, Event event) {
    try {
      eventRouter.receive(event, state.counter());
    } catch (IsClosedException isClosedException) {
      LOG.warn("Could not emit '" + eventType + "' event for: " + state.workflowInstance(),
          isClosedException);
    }
  }
}
