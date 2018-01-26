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

package com.spotify.styx.state;

import static com.spotify.styx.util.FutureUtil.exceptionallyCompletedFuture;

import com.google.common.collect.ImmutableMap;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState.State;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;

/**
 * An implementation of {@link StateManager} that process all events synchronously on the thread
 * that calls {@link #receive(Event)}.
 *
 * This class is not thread safe.
 */
public class SyncStateManager implements StateManager {

  private final Map<WorkflowInstance, RunState> states = new ConcurrentHashMap<>();

  @Override
  public CompletableFuture<Void> trigger(WorkflowInstance workflowInstance, Trigger trigger) {
    final RunState runState = RunState.create(workflowInstance, State.QUEUED, StateData.newBuilder()
        .trigger(trigger)
        .build());
    states.put(workflowInstance, runState);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletionStage<Void> receive(Event event) {
    WorkflowInstance key = event.workflowInstance();
    if (!states.containsKey(key)) {
      return exceptionallyCompletedFuture(
          new IllegalArgumentException(
              "Received event for unknown workflow instance: " + event));
    }
    final RunState currentState = states.get(key);
    final RunState nextState;
    try {
      nextState = currentState.transition(event);
    } catch (IllegalStateException e) {
      return exceptionallyCompletedFuture(new IllegalStateException(e.getMessage()));
    }

    if (nextState.state().isTerminal()) {
      states.remove(key);
    } else {
      states.put(key, nextState);
    }

    nextState.outputHandler().transitionInto(nextState);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public Map<WorkflowInstance, RunState> activeStates() {
    final ImmutableMap.Builder<WorkflowInstance, RunState> builder = ImmutableMap.builder();
    states.entrySet().forEach(entry -> builder.put(entry.getKey(), entry.getValue()));
    return builder.build();
  }

  @Override
  public long getQueuedEventsCount() {
    return 0; // synchronous event handling, no queue
  }

  @Override
  public RunState get(WorkflowInstance workflowInstance) {
    return states.get(workflowInstance);
  }

  public int activeStatesSize() {
    return states.size();
  }

  @Override
  public void close() {
    // nop
  }
}
