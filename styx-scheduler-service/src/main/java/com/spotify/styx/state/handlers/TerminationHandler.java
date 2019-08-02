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

import static java.lang.Boolean.TRUE;

import bsh.BshClassManager;
import bsh.EvalError;
import bsh.Interpreter;
import bsh.NameSpace;
import bsh.Primitive;
import bsh.UtilEvalError;
import com.google.common.annotations.VisibleForTesting;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.RetryUtil;
import com.spotify.styx.util.TriggerUtil;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.PropertyPermission;
import java.util.UUID;
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
  private final Interpreter interpreter;
  private final BshClassManager bcm;

  public TerminationHandler(RetryUtil retryUtil, Storage storage, StateManager stateManager) {
    this.retryUtil = Objects.requireNonNull(retryUtil, "retryUtil");
    this.storage = Objects.requireNonNull(storage, "storage");
    this.stateManager = Objects.requireNonNull(stateManager, "stateManager");
    interpreter = new Interpreter();
    bcm = BshClassManager.createClassManager(interpreter);
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

  @VisibleForTesting
  boolean retryConditionMet(RunState state,
                            Optional<Integer> exitCode,
                            String retryCondition) {
    return AccessController
        .doPrivileged((PrivilegedAction<Boolean>) () -> retryConditionMet0(state, exitCode, retryCondition), null,
            // minimum permission required for BeanShell to function
            new PropertyPermission("saveClasses", "read"));
  }

  private boolean retryConditionMet0(RunState state,
                            Optional<Integer> exitCode,
                            String retryCondition) {
    var ns = new NameSpace(bcm, state.data().executionId().orElseGet(() -> String.valueOf(UUID.randomUUID())));
    try {
      ns.setVariable("exitCode", exitCodeOrPrimitiveNull(exitCode), false);
      ns.setVariable("tries", state.data().tries(), false);
      ns.setVariable("triggerType", triggerTypeOrPrimitiveNull(state.data().trigger()), false);
      ns.setVariable("consecutiveFailures", state.data().consecutiveFailures(), false);
      return TRUE.equals(interpreter.eval(retryCondition, ns));
    } catch (EvalError | UtilEvalError e) {
      LOG.debug("Failed to evaluate retry condition `{}`", retryCondition, e);
      return false;
    }
  }

  private static Object exitCodeOrPrimitiveNull(Optional<Integer> exitCode) {
    if (exitCode.isPresent()) {
      return exitCode.orElseThrow();
    } else {
      return Primitive.NULL;
    }
  }

  private static Object triggerTypeOrPrimitiveNull(Optional<Trigger> trigger) {
    if (trigger.isPresent()) {
      return TriggerUtil.triggerType(trigger.orElseThrow());
    } else {
      return Primitive.NULL;
    }
  }
}
