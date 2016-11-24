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

import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import java.time.Duration;
import java.util.Objects;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link OutputHandler} that manages scheduling generation of {@link Event}s
 * as a response to the {@link RunState.State#TERMINATED} and {@link RunState.State#FAILED} states.
 */
public class TerminationHandler implements OutputHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TerminationHandler.class);
  private static final Random RANDOM = new Random();

  // Retry cost is vaguely related to a max time period we're going to keep retrying a state.
  // See the different costs for failures and missing dependencies in RunState
  public static final double MAX_RETRY_COST = 50.0;
  public static final int MISSING_DEPS_EXIT_CODE = 20;
  public static final int MISSING_DEPS_RETRY_DELAY_MINUTES = 10;

  private final Duration baseDelay;
  private final int maxExponent;
  private final StateManager stateManager;

  public TerminationHandler(
      Duration baseDelay,
      int maxExponent,
      StateManager stateManager) {
    this.baseDelay = Objects.requireNonNull(baseDelay);
    this.maxExponent = maxExponent;
    this.stateManager = Objects.requireNonNull(stateManager);
  }

  @Override
  public void transitionInto(RunState state) {
    switch (state.state()) {
      case TERMINATED:
        if (state.data().lastExit() == 0) {
          stateManager.receiveIgnoreClosed(Event.success(state.workflowInstance()));
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
      final long delayMillis;
      if (state.data().lastExit() == MISSING_DEPS_EXIT_CODE) {
        delayMillis = Duration.ofMinutes(MISSING_DEPS_RETRY_DELAY_MINUTES).toMillis();
      } else {
        delayMillis = calculateDelay(state).toMillis();
      }
      stateManager.receiveIgnoreClosed(Event.retryAfter(workflowInstance, delayMillis));
    } else {
      stateManager.receiveIgnoreClosed(Event.stop(workflowInstance));
    }
  }

  private Duration calculateDelay(RunState state) {
    final int tries = (state.data().tries() < maxExponent) ? state.data().tries() : maxExponent;
    final int multiplier = Math.max(1, RANDOM.nextInt(1 << tries));
    final Duration delay = baseDelay.multipliedBy(multiplier);

    final String instanceKey = state.workflowInstance().toKey();
    LOG.info("{} scheduling retry #{} in {}", instanceKey, state.data().tries(), delay);

    return delay;
  }
}
