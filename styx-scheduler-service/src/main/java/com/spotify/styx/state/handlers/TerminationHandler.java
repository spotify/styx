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

package com.spotify.styx.state.handlers;

import bsh.EvalError;
import bsh.Interpreter;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.RetryUtil;
import com.spotify.styx.util.TriggerUtil;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link OutputHandler} that manages scheduling generation of {@link Event}s
 * as a response to the {@link RunState.State#TERMINATED} and {@link RunState.State#FAILED} states.
 */
public class TerminationHandler implements OutputHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TerminationHandler.class);

  // Retry cost is vaguely related to a max time period we're going to keep retrying a state.
  // See the different costs for failures and missing dependencies in RunState
  static final double MAX_RETRY_COST = 50.0;
  private static final int MISSING_DEPS_EXIT_CODE = 20;
  private static final int FAIL_FAST_EXIT_CODE = 50;
  public static final int MISSING_DEPS_RETRY_DELAY_MINUTES = 10;

  private final RetryUtil retryUtil;
  private final Storage storage;
  private final StateManager stateManager;

  public TerminationHandler(RetryUtil retryUtil, Storage storage, StateManager stateManager) {
    this.retryUtil = Objects.requireNonNull(retryUtil, "retryUtil");
    this.storage = Objects.requireNonNull(storage, "storage");
    this.stateManager = Objects.requireNonNull(stateManager, "stateManager");
  }

  @Override
  public void transitionInto(RunState state) {
    switch (state.state()) {
      case TERMINATED:
        if (state.data().lastExit().map(v -> v.equals(0)).orElse(false)) {
          stateManager.receiveIgnoreClosed(Event.success(state.workflowInstance()), state.counter());
        } else {
          checkRetry(state);
        }
        break;

      case FAILED:
        checkRetry(state);
        break;

      default:
        // do nothing
    }
  }

  private void checkRetry(RunState state) {
    final WorkflowInstance workflowInstance = state.workflowInstance();

    if (state.data().retryCost() < MAX_RETRY_COST) {
      final Optional<Integer> exitCode = state.data().lastExit();
      if (shouldFailFast(state, exitCode)) {
        stateManager.receiveIgnoreClosed(Event.stop(workflowInstance), state.counter());
      } else {
        final long delayMillis;
        if (isMissingDependency(exitCode)) {
          delayMillis = Duration.ofMinutes(MISSING_DEPS_RETRY_DELAY_MINUTES).toMillis();
        } else {
          delayMillis = retryUtil.calculateDelay(state.data().consecutiveFailures()).toMillis();
        }
        stateManager.receiveIgnoreClosed(Event.retryAfter(workflowInstance, delayMillis), state.counter());
      }
    } else {
      stateManager.receiveIgnoreClosed(Event.stop(workflowInstance), state.counter());
    }
  }

  private static boolean isMissingDependency(Optional<Integer> exitCode) {
    return exitCode.map(c -> c == MISSING_DEPS_EXIT_CODE).orElse(false);
  }

  private boolean shouldFailFast(RunState state, Optional<Integer> exitCode) {
    if (exitCode.isPresent() && exitCode.orElseThrow() == FAIL_FAST_EXIT_CODE) {
      return true;
    }

    final Optional<Workflow> workflow;
    try {
      workflow = storage.workflow(state.workflowInstance().workflowId());
    } catch (IOException e) {
      LOG.warn("Failed to read workflow  {}", state.workflowInstance().toKey(), e);
      return false;
    }

    if (workflow.isEmpty()) {
      return true;
    }

    return workflow.orElseThrow().configuration().retryCondition()
        .map(s -> !retryConditionMet(state, exitCode, s))
        .orElse(false);
  }

  static boolean retryConditionMet(RunState state,
                                   Optional<Integer> exitCode,
                                   String retryCondition) {
    var interpreter = new Interpreter();
    try {
      interpreter.set("exitCode", exitCode.orElse(null));
      interpreter.set("tries", state.data().tries());
      interpreter.set("triggerType", state.data().trigger().map(TriggerUtil::triggerType).orElse(null));
      interpreter.set("consecutiveFailures", state.data().consecutiveFailures());
      final Object result = interpreter.eval(retryCondition);
      return result instanceof Boolean && (boolean) result;
    } catch (EvalError evalError) {
      return false;
    }
  }
}
