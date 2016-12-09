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

package com.spotify.styx;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.TimeoutConfig;
import com.spotify.styx.util.Time;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for making decisions on how to make further progress on states in
 * the {@link StateManager}. The general operation is such that each time the {@link #tick()}
 * method is called, the scheduler will inspect all the active states in {@link StateManager} and
 * determine if any of them should receive new events.
 *
 * <p>Currently the scheduler only cares about states that either have timed out according to
 * the {@link TimeoutConfig}), or should be retried according to their
 * {@link StateData#retryDelayMillis()}.
 */
public class Scheduler {

  private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);

  private final Time time;
  private final TimeoutConfig ttls;
  private final StateManager stateManager;

  public Scheduler(Time time, TimeoutConfig ttls, StateManager stateManager) {
    this.time = Objects.requireNonNull(time);
    this.ttls = Objects.requireNonNull(ttls);
    this.stateManager = Objects.requireNonNull(stateManager);
  }

  public void tick() {
    for (Map.Entry<WorkflowInstance, RunState> entry : stateManager.activeStates().entrySet()) {
      final WorkflowInstance key = entry.getKey();
      final RunState state = entry.getValue();

      if (hasTimedOut(state)) {
        LOG.info("Found stale state, triggering timeout for {}", state);
        stateManager.receiveIgnoreClosed(Event.timeout(key));
      }

      else if (shouldRetry(state)) {
        LOG.info("{} triggering retry #{}", key.toKey(), state.data().tries());
        stateManager.receiveIgnoreClosed(Event.retry(key));
      }

      else if (shouldTrigger(state)) {
        LOG.info("Triggering {}", key.toKey());
        stateManager.receiveIgnoreClosed(Event.dequeue(key));
      }
    }
  }

  private boolean shouldRetry(RunState runState) {
    if (runState.state() != RunState.State.QUEUED
        || runState.data().tries() == 0) {
      return false;
    }

    final Instant now = time.get();
    final Instant deadline = Instant
        .ofEpochMilli(runState.timestamp())
        .plusMillis(runState.data().retryDelayMillis());

    return !deadline.isAfter(now);
  }

  private boolean hasTimedOut(RunState runState) {
    if (runState.state().isTerminal()) {
      return false;
    }

    final Instant now = time.get();
    final Instant deadline = Instant
        .ofEpochMilli(runState.timestamp())
        .plus(ttls.ttlOf(runState.state()));

    return !deadline.isAfter(now);
  }

  private boolean shouldTrigger(RunState runState) {
    return runState.state() == RunState.State.QUEUED
           && runState.data().tries() == 0;
  }
}
