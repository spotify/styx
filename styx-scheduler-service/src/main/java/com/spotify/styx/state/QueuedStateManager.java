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

import com.google.common.annotations.VisibleForTesting;
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
import eu.javaspecialists.tjsn.concurrency.stripedexecutor.StripedExecutorService;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
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

  private final LongAdder queuedEvents = new LongAdder();

  private final Time time;
  private final StripedExecutorService eventTransitionExecutor;
  private final Storage storage;
  private final BiConsumer<SequenceEvent, RunState> eventConsumer;
  private final Executor eventConsumerExecutor;
  private final OutputHandler outputHandler;

  private volatile boolean running = true;

  public QueuedStateManager(
      Time time,
      StripedExecutorService eventTransitionExecutor,
      Storage storage,
      BiConsumer<SequenceEvent, RunState> eventConsumer,
      Executor eventConsumerExecutor,
      OutputHandler outputHandler) {
    this.time = Objects.requireNonNull(time);
    this.storage = Objects.requireNonNull(storage);
    this.eventConsumer = Objects.requireNonNull(eventConsumer);
    this.eventConsumerExecutor = Objects.requireNonNull(eventConsumerExecutor);
    this.eventTransitionExecutor = Objects.requireNonNull(eventTransitionExecutor);
    this.outputHandler = Objects.requireNonNull(outputHandler);
  }

  @Override
  public CompletionStage<Void> trigger(WorkflowInstance workflowInstance, Trigger trigger)
      throws IsClosedException {
    ensureRunning();
    LOG.debug("Trigger {}", workflowInstance);

    // TODO: optional retry on transaction conflict

    return CompletableFuture.runAsync(() -> initialize(workflowInstance)).thenCompose((ignore) -> {
      final Event event = Event.triggerExecution(workflowInstance, trigger);
      try {
        return receive(event);
      } catch (IsClosedException isClosedException) {
        LOG.warn("Failed to send 'triggerExecution' event", isClosedException);
        // Best effort attempt to rollback the creation of the NEW state
        try {
          storage.deleteActiveState(workflowInstance);
        } catch (IOException e) {
          LOG.warn("Failed to remove dangling NEW state for: {}", workflowInstance);
        }
        throw new RuntimeException(isClosedException);
      }
    });
  }

  @Override
  public CompletableFuture<Void> receive(Event event) throws IsClosedException {
    ensureRunning();
    LOG.debug("Event {}", event);

    // TODO: optional retry on transaction conflict

    queuedEvents.increment();
    return Striping.supplyAsyncStriped(() ->
        transition(event), event.workflowInstance(), eventTransitionExecutor)
        .thenAccept((tuple) -> postTransition(tuple._1, tuple._2));
  }

  private void initialize(WorkflowInstance workflowInstance) {
    // Write active state to datastore
    final RunState runState = RunState.create(workflowInstance, State.NEW, time.get());

    final long counter;
    try {
      counter = storage.getLatestStoredCounter(workflowInstance).orElse(NO_EVENTS_PROCESSED);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try {
      storage.runInTransaction(tx -> {
        final Optional<Workflow> workflow = tx.workflow(workflowInstance.workflowId());
        if (!workflow.isPresent()) {
          throw new IllegalArgumentException(
              "Workflow not found: " + workflowInstance.workflowId().toKey());
        }
        return tx.insertActiveState(workflowInstance,
            PersistentWorkflowInstanceState.of(runState, counter));
      });
    } catch (TransactionException e) {
      if (e.isAlreadyExists()) {
        throw new IllegalStateException(
            "Workflow instance is already triggered: " + workflowInstance.toKey());
      } else if (e.isConflict()) {
        LOG.debug(
            "Transactional conflict, abort triggering Workflow instance: " + workflowInstance
                .toKey());
        throw new RuntimeException(e);
      } else {
        throw new RuntimeException(e);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Tuple2<SequenceEvent, RunState> transition(Event event) {
    queuedEvents.decrement();
    try {
      return storage.runInTransaction(tx -> {

        // Read active state from datastore
        final Optional<PersistentWorkflowInstanceState> persistentState =
            tx.activeState(event.workflowInstance());
        if (!persistentState.isPresent()) {
          String message = "Received event for unknown workflow instance: " + event;
          LOG.warn(message);
          throw new IllegalArgumentException(message);
        }

        // Transition to next state
        final RunState runState =
            RunState.create(event.workflowInstance(), persistentState.get().state(),
                persistentState.get().data(), time.get());
        final RunState nextRunState;
        try {
          nextRunState = runState.transition(event);
        } catch (IllegalStateException e) {
          // TODO: illegal state transitions might become common as multiple scheduler
          //       instances concurrently consume events from k8s.
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

        final SequenceEvent sequenceEvent =
            SequenceEvent.create(event, nextCounter, nextRunState.timestamp());

        return Tuple.of(sequenceEvent, nextRunState);
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void postTransition(SequenceEvent sequenceEvent, RunState runState) {
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
    outputHandler.transitionInto(runState);
  }

  @Override
  public Map<WorkflowInstance, RunState> activeStates() {
    final Map<WorkflowInstance, PersistentWorkflowInstanceState> states;
    try {
      states = storage.readActiveWorkflowInstances();
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

    eventTransitionExecutor.shutdown();
    try {
      if (!eventTransitionExecutor
          .awaitTermination(SHUTDOWN_GRACE_PERIOD.toMillis(), MILLISECONDS)) {
        throw new IOException(
            "Graceful shutdown failed, event loop did not finish within grace period");
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException(e);
    }
  }

  @VisibleForTesting
  void ensureRunning() throws IsClosedException {
    if (!running) {
      throw new IsClosedException();
    }
  }

  public Long queuedEvents() {
    return queuedEvents.sum();
  }
}
