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

import static com.spotify.styx.state.StateUtil.isConsumingResources;
import static com.spotify.styx.util.MDCUtil.withMDC;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.spotify.styx.MessageUtil;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import com.spotify.styx.storage.TransactionException;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.CounterCapacityException;
import com.spotify.styx.util.EventUtil;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.ShardedCounter;
import com.spotify.styx.util.Time;
import eu.javaspecialists.tjsn.concurrency.stripedexecutor.StripedExecutorService;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiConsumer;
import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.control.Try;
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

  private static final Logger DEFAULT_LOG = LoggerFactory.getLogger(QueuedStateManager.class);
  private final Logger log;

  private static final long NO_EVENTS_PROCESSED = -1L;

  private final LongAdder queuedEvents = new LongAdder();

  private final Time time;
  private final StripedExecutorService eventProcessingExecutor;
  private final Storage storage;
  private final BiConsumer<SequenceEvent, RunState> eventConsumer;
  private final Executor eventConsumerExecutor;
  private final OutputHandler outputHandler;
  private final ShardedCounter shardedCounter;

  private volatile boolean running = true;

  public QueuedStateManager(
      Time time,
      StripedExecutorService eventProcessingExecutor,
      Storage storage,
      BiConsumer<SequenceEvent, RunState> eventConsumer,
      Executor eventConsumerExecutor,
      OutputHandler outputHandler,
      ShardedCounter shardedCounter) {
    this(time, eventProcessingExecutor, storage, eventConsumer, eventConsumerExecutor, outputHandler, shardedCounter,
        DEFAULT_LOG);
  }

  public QueuedStateManager(
      Time time,
      StripedExecutorService eventProcessingExecutor,
      Storage storage,
      BiConsumer<SequenceEvent, RunState> eventConsumer,
      Executor eventConsumerExecutor,
      OutputHandler outputHandler,
      ShardedCounter shardedCounter,
      Logger logger) {
    this.time = Objects.requireNonNull(time);
    this.storage = Objects.requireNonNull(storage);
    this.eventConsumer = Objects.requireNonNull(eventConsumer);
    this.eventConsumerExecutor = Objects.requireNonNull(eventConsumerExecutor);
    this.eventProcessingExecutor = Objects.requireNonNull(eventProcessingExecutor);
    this.outputHandler = Objects.requireNonNull(outputHandler);
    this.shardedCounter = Objects.requireNonNull(shardedCounter);
    this.log = Objects.requireNonNull(logger, "logger");
  }

  @Override
  public CompletionStage<Void> trigger(WorkflowInstance workflowInstance, Trigger trigger, TriggerParameters parameters)
      throws IsClosedException {
    ensureRunning();
    log.debug("Trigger {}", workflowInstance);

    // TODO: optional retry on transaction conflict

    return CompletableFuture.runAsync(() -> initialize(workflowInstance), withMDC()).thenCompose((ignore) -> {
      final Event event = Event.triggerExecution(workflowInstance, trigger, parameters);
      try {
        return receive(event);
      } catch (IsClosedException isClosedException) {
        log.warn("Failed to send 'triggerExecution' event", isClosedException);
        // Best effort attempt to rollback the creation of the NEW state
        try {
          storage.deleteActiveState(workflowInstance);
        } catch (IOException e) {
          log.warn("Failed to remove dangling NEW state for: {}", workflowInstance, e);
        }
        throw new RuntimeException(isClosedException);
      }
    });
  }

  @Override
  public CompletableFuture<Void> receive(Event event) throws IsClosedException {
    return receive(event, Long.MAX_VALUE);
  }

  @Override
  public CompletableFuture<Void> receive(Event event, long expectedCounter) throws IsClosedException {
    ensureRunning();
    log.info("Received event {}", event);

    // TODO: optional retry on transaction conflict

    queuedEvents.increment();
    return Striping.supplyAsyncStriped(() ->
        transition(event, expectedCounter), event.workflowInstance(), eventProcessingExecutor)
        .thenAccept((tuple) -> postTransition(tuple._1, tuple._2));
  }

  private void initialize(WorkflowInstance workflowInstance) {
    // Write active state to datastore

    final long counter;
    try {
      counter = storage.getLatestStoredCounter(workflowInstance).orElse(NO_EVENTS_PROCESSED);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    final RunState runState = RunState.create(workflowInstance, State.NEW, time.get(), counter);
    try {
      storage.runInTransaction(tx -> {
        final Optional<Workflow> workflow = tx.workflow(workflowInstance.workflowId());
        if (!workflow.isPresent()) {
          throw new IllegalArgumentException(
              "Workflow not found: " + workflowInstance.workflowId().toKey());
        }
        return tx.writeActiveState(workflowInstance, runState);
      });
    } catch (TransactionException e) {
      if (e.isAlreadyExists()) {
        throw new AlreadyInitializedException("Workflow instance is already triggered: " + workflowInstance);
      } else if (e.isConflict()) {
        log.debug("Transaction conflict when triggering workflow instance. Aborted: {}",
            workflowInstance);
        throw new RuntimeException(e);
      } else {
        log.debug("Transaction failure when triggering workflow instance: {}: {}",
            workflowInstance, e.getMessage(), e);
        throw new RuntimeException(e);
      }
    } catch (Exception e) {
      log.debug("Failure when triggering workflow instance: {}: {}", workflowInstance, e.getMessage(), e);
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  private Tuple2<SequenceEvent, RunState> transition(Event event, long expectedCounter) {
    queuedEvents.decrement();
    try {
      return storage.runInTransaction(tx -> {

        // Read active state from datastore
        final Optional<RunState> currentRunState =
            tx.readActiveState(event.workflowInstance());
        if (!currentRunState.isPresent()) {
          String message = "Received event for unknown workflow instance: " + event;
          log.warn(message);
          throw new IllegalArgumentException(message);
        }

        // Verify counters for in-order event processing
        verifyCounter(event, expectedCounter, currentRunState.get());
        log.info("Received event (verified) {}", event);

        final RunState nextRunState;
        try {
          nextRunState = currentRunState.get().transition(event, time);
        } catch (IllegalStateException e) {
          // TODO: illegal state transitions might become common as multiple scheduler
          //       instances concurrently consume events from k8s.
          log.warn("Illegal state transition", e);
          throw e;
        }

        // Resource limiting occurs by throwing here, or by failing the commit with a conflict.
        updateResourceCounters(tx, event, currentRunState.get(), nextRunState);

        // Write new state to datastore (or remove it if terminal)
        if (nextRunState.state().isTerminal()) {
          tx.deleteActiveState(event.workflowInstance());
        } else {
          tx.updateActiveState(event.workflowInstance(), nextRunState);
        }

        final SequenceEvent sequenceEvent =
            SequenceEvent.create(event, nextRunState.counter(), nextRunState.timestamp());

        return Tuple.of(sequenceEvent, nextRunState);
      });
    } catch (TransactionException e) {
      if (e.isConflict()) {
        log.debug("Transaction conflict during workflow instance transition. Aborted: {}, counter={}",
            event, expectedCounter);
        throw new RuntimeException(e);
      } else {
        log.debug("Transaction failure during workflow instance transition: {}, counter={}",
            event, expectedCounter, e);
        throw new RuntimeException(e);
      }
    } catch (Exception e) {
      log.debug("Failure during workflow instance transition: {}, counter={}",
          event, expectedCounter, e);
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  private void updateResourceCounters(StorageTransaction tx, Event event,
                                      RunState currentRunState, RunState nextRunState) throws IOException {
    // increment counters if event is dequeue
    if (isDequeue(event) && nextRunState.data().resourceIds().isPresent()) {
      tryUpdatingCounter(currentRunState, tx, nextRunState.data().resourceIds().get());
    }

    // decrement counters if transitioning from a state that consumes resources
    // to a state that doesn't consume any resources
    if (isConsumingResources(currentRunState.state())
        && !isConsumingResources(nextRunState.state())) {
      if (nextRunState.data().resourceIds().isPresent()) {
        for (String resource : nextRunState.data().resourceIds().get()) {
          tx.updateCounter(shardedCounter, resource, -1);
        }
      } else {
        log.error("Resource ids are missing for {} when transitioning from {} to {}.",
            nextRunState.workflowInstance(), currentRunState, nextRunState);
      }
    }
  }

  private void tryUpdatingCounter(RunState runState,
                                  StorageTransaction tx,
                                  Set<String> resourceIds) {
    final Set<Tuple2<String, Try<Void>>> failedTries = resourceIds.stream()
        .map(resource -> Tuple.of(resource, Try.run(() ->
            tx.updateCounter(shardedCounter, resource, 1))))
        .filter(x -> x._2.isFailure())
        .collect(toSet());
    final List<String> depletedResourceIds = failedTries.stream()
        .filter(x -> x._2.getCause() instanceof CounterCapacityException)
        .map(x -> x._1)
        // Sort resource ids to get deterministic message contents
        .sorted()
        .collect(toList());

    if (!depletedResourceIds.isEmpty()) {
      MessageUtil.emitResourceLimitReachedMessage(this, runState, depletedResourceIds);
    }

    if (!failedTries.isEmpty()) {
      final List<String> failedResources = failedTries.stream()
          .map(x -> x._1)
          .sorted()
          .collect(toList());
      final RuntimeException exception = new RuntimeException(
          "Failed to update resource counter for workflow instance: "
              + runState.workflowInstance() + ": " + failedResources);
      failedTries.stream()
          .map(x -> x._2.getCause())
          .forEach(exception::addSuppressed);
      throw exception;
    }
  }

  private boolean isDequeue(Event event) {
    return EventUtil.name(event).equals("dequeue");
  }

  private void verifyCounter(Event event, long expectedCounter, RunState currentRunState) {
    if (expectedCounter == Long.MAX_VALUE) {
      return;
    }

    final long currentCounter = currentRunState.counter();
    if (currentCounter > expectedCounter) {
      final String message = "Stale event encountered. Expected counter is "
                             + expectedCounter  + " but current counter is "
                             + currentCounter + ". Discarding event " + event;
      log.debug(message);
      throw new StaleEventException(message);
    } else if (currentCounter < expectedCounter) {
      // This should never happen
      final String message = "Unexpected current counter is less than last observed one for "
                             + currentRunState;
      log.error(message);
      throw new RuntimeException(message);
    }
  }

  private void postTransition(SequenceEvent sequenceEvent, RunState runState) {
    // Write event to bigtable
    try {
      storage.writeEvent(sequenceEvent);
    } catch (IOException e) {
      log.warn("Error writing event {}", sequenceEvent, e);
    }

    // Publish event
    try {
      eventConsumerExecutor.execute(withMDC(() -> eventConsumer.accept(sequenceEvent, runState)));
    } catch (Exception e) {
      log.warn("Error while consuming event {}", sequenceEvent, e);
    }

    // Execute output handler(s)
    outputHandler.transitionInto(runState);
  }

  @Override
  public Map<WorkflowInstance, RunState> getActiveStates() {
    try {
      return storage.readActiveStates();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Map<WorkflowInstance, RunState> getActiveStatesByTriggerId(String triggerId) {
    try {
      return storage.readActiveStatesByTriggerId(triggerId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Optional<RunState> getActiveState(WorkflowInstance workflowInstance) {
    try {
      return storage.readActiveState(workflowInstance);
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
