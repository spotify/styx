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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toMap;

import com.spotify.futures.CompletableFutures;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.PersistentWorkflowInstanceState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.TransactionException;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import javaslang.Tuple;
import javaslang.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link StateManager} that has an internal queue for all the incoming
 * {@link Event}s that are sent to {@link #receive(Event)}.
 *
 * <p>The events are all processed on an injected {@link Executor}, but sequentially per
 * {@link WorkflowInstance}. This allows event processing to scale across many separate workflow
 * instances while guaranteeing that each state machine progresses sequentially.
 *
 * <p>All {@link #outputHandler} transitions are also executed on the injected
 * {@link Executor}.
 */
public class QueuedStateManager implements StateManager {

  private static final Logger LOG = LoggerFactory.getLogger(QueuedStateManager.class);

  private static final long NO_EVENTS_PROCESSED = -1L;

  private static final Duration SHUTDOWN_GRACE_PERIOD = Duration.ofSeconds(5);

  private final Time time;

  private final ExecutorService outputHandlerExecutor;
  private final Storage storage;
  private final BiConsumer<SequenceEvent, RunState> eventConsumer;
  private final Executor eventConsumerExecutor;
  private final OutputHandler outputHandler;

  private volatile boolean running = true;

  public QueuedStateManager(
      Time time,
      ExecutorService outputHandlerExecutor,
      Storage storage,
      BiConsumer<SequenceEvent, RunState> eventConsumer,
      Executor eventConsumerExecutor,
      OutputHandler outputHandler) {
    this.time = Objects.requireNonNull(time);
    this.outputHandlerExecutor = Objects.requireNonNull(outputHandlerExecutor);
    this.storage = Objects.requireNonNull(storage);
    this.eventConsumer = Objects.requireNonNull(eventConsumer);
    this.eventConsumerExecutor = Objects.requireNonNull(eventConsumerExecutor);
    this.outputHandler = Objects.requireNonNull(outputHandler);
  }

  @Override
  public CompletionStage<Void> trigger(WorkflowInstance workflowInstance, Trigger trigger) throws IsClosedException {
    ensureRunning();

    final long nextCounter;
    try {
      final long counter = storage.getLatestStoredCounter(workflowInstance).orElse(NO_EVENTS_PROCESSED);
      nextCounter = counter + 1;
    } catch (IOException e) {
      return CompletableFutures.exceptionallyCompletedFuture(e);
    }

    // TODO: optional retry on transaction conflict

    return CompletableFuture.supplyAsync(() -> {

      // Write active state to datastore
      final RunState runState = RunState.create(workflowInstance, State.QUEUED, StateData.newBuilder()
          .trigger(trigger)
          .build(), time.get());
      try {
        storage.runInTransaction(tx -> {
          final Optional<Workflow> workflow = tx.workflow(workflowInstance.workflowId());
          if (!workflow.isPresent()) {
            throw new IllegalArgumentException("Workflow not found: " + workflowInstance.workflowId().toKey());
          }
          return tx.insertActiveState(workflowInstance, PersistentWorkflowInstanceState.of(runState, nextCounter));
        });
      } catch (TransactionException e) {
        if (e.isAlreadyExists()) {
          throw new IllegalStateException("Workflow instance is already triggered: " + workflowInstance.toKey());
        } else {
          throw new RuntimeException(e);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      final Event event = Event.triggerExecution(workflowInstance, trigger);
      final SequenceEvent sequenceEvent = SequenceEvent.create(event, nextCounter, runState.timestamp());

      emitEvent(sequenceEvent, runState);

      return null;
    }, outputHandlerExecutor);
  }

  @Override
  public CompletionStage<Void> receive(Event event) throws IsClosedException {
    ensureRunning();

    LOG.debug("Event {}", event);

    // TODO: optional retry on transaction conflict

    // TODO: run on striped executor to get event execution in order.

    return CompletableFuture.supplyAsync(() -> {
      final Tuple2<SequenceEvent, RunState> next;

      // Perform transactional state transition
      try {
        next = storage.runInTransaction(tx -> {

          // Read active state from datastore
          final Optional<PersistentWorkflowInstanceState> persistentState =
              tx.activeState(event.workflowInstance());
          if (!persistentState.isPresent()) {
            String message = "Received event for unknown workflow instance: " + event;
            LOG.warn(message);
            throw new IllegalArgumentException(message);
          }

          // Transition to next state
          final RunState runState = RunState.create(event.workflowInstance(), persistentState.get().state(),
              persistentState.get().data(), time.get());
          final RunState nextRunState;
          try {
            nextRunState = runState.transition(event);
          } catch (IllegalStateException e) {
            // TODO: illegal state transitions might become common as multiple scheduler instances concurrently
            //       consume events from k8s.
            LOG.warn("Illegal state transition", e);
            throw e;
          }

          // Write new state to datastore (or remove it if terminal)
          final long nextCounter = persistentState.get().counter() + 1;
          if (nextRunState.state().isTerminal()) {
            tx.deleteActiveState(event.workflowInstance());
          } else {
            final PersistentWorkflowInstanceState nextPersistentState =
                PersistentWorkflowInstanceState.of(nextRunState, nextCounter);
            tx.updateActiveState(event.workflowInstance(), nextPersistentState);
          }

          final SequenceEvent sequenceEvent = SequenceEvent.create(event, nextCounter, nextRunState.timestamp());

          return Tuple.of(sequenceEvent, nextRunState);
        });
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      emitEvent(next._1, next._2);

      return null;
    }, outputHandlerExecutor);
  }

  private void emitEvent(SequenceEvent sequenceEvent, RunState runState) {

    System.err.println(sequenceEvent);

    // Write event to bigtable
    try {
      storage.writeEvent(sequenceEvent);
    } catch (IOException e) {
      LOG.warn("Error writing event {}", sequenceEvent, e);
    }

    // Publish event
    try {
      eventConsumerExecutor.execute(() -> eventConsumer.accept(sequenceEvent, runState));
    } catch (Exception e) {
      LOG.warn("Error while consuming event {}", sequenceEvent, e);
    }

    // Execute output handler(s)
    try {
      outputHandler.transitionInto(runState);
    } catch (Throwable e) {
      LOG.warn("Output handler threw", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<WorkflowInstance, RunState> activeStates() {
    final Map<WorkflowInstance, PersistentWorkflowInstanceState> states;
    try {
      states = storage
          .readActiveWorkflowInstances();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return states.entrySet().stream()
        .collect(toMap(
            Entry::getKey,
            e -> RunState.create(
                e.getKey(), e.getValue().state(), e.getValue().data(), e.getValue().timestamp())));
  }

  @Override
  public RunState get(WorkflowInstance workflowInstance) {
    try {
      return storage.readActiveWorkflowInstance(workflowInstance)
          .map(state -> RunState.create(workflowInstance, state.state(), state.data(), state.timestamp()))
          .orElse(null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    if (!running) {
      return;
    }
    running = false;

    outputHandlerExecutor.shutdown();
    try {
      if (!outputHandlerExecutor.awaitTermination(SHUTDOWN_GRACE_PERIOD.toMillis(), MILLISECONDS)) {
        throw new IOException("Graceful shutdown failed, event loop did not finish within grace period");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  private void ensureRunning() throws IsClosedException {
    if (!running) {
      throw new IsClosedException();
    }
  }
}
