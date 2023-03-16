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

import com.spotify.styx.flyte.FlyteExecutionId;
import com.spotify.styx.flyte.FlyteRunner;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link OutputHandler} for triggering Flyte's launch plans.
 */
public class FlyteRunnerHandler extends AbstractRunnerHandler {

  private static final Logger LOG = LoggerFactory.getLogger(FlyteRunnerHandler.class);
  static final String STYX_WORKFLOW_INSTANCE_ANNOTATION = "styx-workflow-instance";
  static final String STYX_EXECUTION_ID_ANNOTATION = "styx-execution-id";

  private final FlyteRunner flyteRunner;

  public FlyteRunnerHandler(FlyteRunner flyteRunner) {
    super(desc -> desc.flyteExecConf().isPresent());
    this.flyteRunner = requireNonNull(flyteRunner);
  }

  @Override
  public void safeTransitionInto(final RunState state, final EventRouter eventRouter) {
    if (!flyteRunner.isEnabled()) {
      var workflowInstance = state.workflowInstance();
      LOG.error("Unable to transition {} into {}. Flyte system is not available", workflowInstance, state.state());
      // halt the execution if we have not; ERROR state is driven by halt event
      if (state.state() != RunState.State.ERROR) {
        eventRouter.receiveIgnoreClosed(Event.halt(workflowInstance), state.counter());
      }
      return;
    }

    switch (state.state()) {
      case SUBMITTING:
        transitionSubmitting(state, eventRouter);
        break;
      case SUBMITTED:
      case RUNNING:
        pollingExecution(state);
        break;
      case ERROR:
      case FAILED:
        cleanUpExecution(state);
        break;
      default:
        // do nothing
    }
  }

  private void transitionSubmitting(RunState state, EventRouter eventRouter) {
    LOG.info("Entered state SUBMITTING for: " + state.workflowInstance());

    final FlyteExecConf flyteExecConf = state.data().executionDescription().orElseThrow().flyteExecConf().orElseThrow();
    final String executionId = state.data().executionId().orElseThrow();
    final String execName = state.data().executionDescription().orElseThrow().flyteExecutionId().orElseThrow();

    final String runnerId;
    try {
      LOG.info("running:{}, conf:{}, state:{}, flyte exec name:{}",
          state.workflowInstance(), flyteExecConf, state, execName);
      runnerId = flyteRunner.createExecution(state, execName, flyteExecConf);
    } catch (Exception e) {
      final var errMessage = "Failed to start execution for " + state.workflowInstance();
      LOG.error(errMessage, e);
      eventRouter.receiveIgnoreClosed(Event.runError(state.workflowInstance(), e.getMessage()), state.counter());
      return;
    }

    // Emit `submitted` _after_ starting execution to ensure that we retry in case of failure.
    final Event submitted = Event.submitted(state.workflowInstance(), executionId, runnerId);
    LOG.info("Issue 'submitted' event for: " + state.workflowInstance());
    eventRouter.receiveIgnoreClosed(submitted, state.counter());
  }

  private void pollingExecution(RunState state) {
    LOG.info("Entered state " + state.state().toString() + " for: " + state.workflowInstance());
    flyteRunner.poll(getExecutionId(state), state);
  }

  private void cleanUpExecution(RunState state) {
    flyteRunner.terminateExecution(state, getExecutionId(state));
  }

  private FlyteExecutionId getExecutionId(RunState state) {
    final FlyteExecConf flyteExecConf = state.data().executionDescription().orElseThrow().flyteExecConf().orElseThrow();
    final String execName = state.data().executionDescription().orElseThrow().flyteExecutionId().orElseThrow();
    return getFlyteExecutionId(flyteExecConf, execName);
  }

  private FlyteExecutionId getFlyteExecutionId(FlyteExecConf flyteExecConf, String execName) {
    return FlyteExecutionId.create(
        flyteExecConf.referenceId().project(),
        flyteExecConf.referenceId().domain(),
        execName);
  }
}
