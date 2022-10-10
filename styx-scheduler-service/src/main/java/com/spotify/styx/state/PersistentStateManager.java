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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.spotify.futures.CompletableFutures;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import javaslang.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link StateManager} that persists {@link RunState}s and performs transactional
 * state transitions using {@link Storage}.
 *
 * <p>The events are all processed on an injected {@link Executor}, but sequentially per
 * {@link WorkflowInstance}. This allows event processing to scale across many separate workflow
 * instances while guaranteeing that each state machine progresses sequentially.
 *
 * <p>All {@link #outputHandler} transitions are also executed on the injected
 * {@link Executor}.
 */
public class PersistentStateManager implements StateManager {

  private static final Logger DEFAULT_LOG = LoggerFactory.getLogger(PersistentStateManager.class);
  private final Logger log;

  private static final long NO_EVENTS_PROCESSED = -1L;

  private final Time time;
  private final ExecutorService executor;
  private final Storage storage;
  private final BiConsumer<SequenceEvent, RunState> eventConsumer;
  private final Executor eventConsumerExecutor;
  private final OutputHandler outputHandler;
  private final ShardedCounter shardedCounter;

  private volatile boolean running = true;

  public PersistentStateManager(
      Time time,
      ExecutorService executor,
      Storage storage,
      BiConsumer<SequenceEvent, RunState> eventConsumer,
      Executor eventConsumerExecutor,
      OutputHandler outputHandler,
      ShardedCounter shardedCounter) {
    this(time, executor, storage, eventConsumer, eventConsumerExecutor, outputHandler, shardedCounter,
        DEFAULT_LOG);
  }

  PersistentStateManager(
      Time time,
      ExecutorService executor,
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
    this.executor = Objects.requireNonNull(executor);
    this.outputHandler = Objects.requireNonNull(outputHandler);
    this.shardedCounter = Objects.requireNonNull(shardedCounter);
    this.log = Objects.requireNonNull(logger, "logger");
  }

  @Override
  public void tick() {
    var shuffledInstances = new ArrayList<>(Try.of(storage::listActiveInstances).get());
    Collections.shuffle(shuffledInstances);
    var futures = shuffledInstances.stream()
        .map(instance -> CompletableFuture.runAsync(() -> tickInstance(instance), executor))
        .collect(toList());
    CompletableFutures.allAsList(futures).join();
  }

  private void tickInstance(WorkflowInstance instance) {
    try {
      var stateOpt = storage.readActiveState(instance);
      stateOpt.ifPresent(this::tickInstance);
    } catch (Exception e) {
      log.error("Error ticking instance: {}", instance, e);
    }
  }

  private void tickInstance(RunState state) {
    log.info("Ticking instance: {}: #{} {}", state.workflowInstance(), state.counter(), state.state());
    try {
      outputHandler.transitionInto(state, this);
    } catch (StateTransitionConflictException e) {
      log.debug("State transition conflict when ticking instance: {}", state.workflowInstance(), e);
    } catch (CounterCapacityException e) {
      log.debug("Counter capacity exhausted when ticking instance: {}", state.workflowInstance(), e);
    } catch (IllegalArgumentException e) {
      log.debug("Illegal argument when ticking instance: {}", state.workflowInstance(), e);
    }
  }

  @Override
  public void trigger(WorkflowInstance workflowInstance, Trigger trigger, TriggerParameters parameters)
      throws IsClosedException {
    ensureRunning();
    log.debug("Trigger {}", workflowInstance);

    // Skip the NEW state and Write the QUEUED state directly in order to avoid needing to deal with the
    // possibility of getting stuck in the NEW state.

    var counter = latestEventCounter(workflowInstance);
    var initialRunState = RunState.create(workflowInstance, State.NEW, time.get(), counter);
    var event = Event.triggerExecution(workflowInstance, trigger, parameters);
    var runState = initialRunState.transition(event, time);
    try {
      storage.runInTransactionWithRetries(tx -> {
        final Optional<Workflow> workflow = tx.workflow(workflowInstance.workflowId());
        if (workflow.isEmpty()) {
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

    postTransition(event, runState);
  }

  private long latestEventCounter(WorkflowInstance workflowInstance) {
    try {
      return storage.getLatestStoredCounter(workflowInstance).orElse(NO_EVENTS_PROCESSED);
    } catch (IOException e1) {
      throw new RuntimeException(e1);
    }
  }

  @Override
  public void receive(Event event) throws IsClosedException {
    receive(event, Long.MAX_VALUE);
  }

  @Override
  public void receive(Event event, long expectedCounter) throws IsClosedException {
    ensureRunning();
    log.info("Received event {}", event);

    var maybeNextRunState = transition(event, expectedCounter);
    maybeNextRunState.ifPresent(nextRunState -> postTransition(event, nextRunState));
  }

  private Optional<RunState> transition(Event event, long expectedCounter) {
    try {
      return storage.runInTransactionWithRetries(tx -> transition0(tx, event, expectedCounter));
    } catch (Throwable e) {
      log.debug("Failed workflow instance transition: {}, counter={}", event, expectedCounter, e);
      Throwables.throwIfUnchecked(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Transition the workflow instance state in the storage based on the {@code event} passed.
   * @param tx the current open transaction in the storage
   * @param event the event causing the transition. It contains the workflow id inside.
   * @param expectedCounter expected counter used for event sorting
   * @return If the workflow instance is no longer active, then an @{link Optional::empty} is returned, otherwise
   * the transition will be applied and the new {@link RunState} will be returned wrapped in an {@link Optional::of}
   * @throws IOException if problems reading the sctive state or updating the new state
   */
  private Optional<RunState> transition0(StorageTransaction tx, Event event, long expectedCounter)
      throws IOException {

    // Read active state from datastore
    var currentRunStateOpt = tx.readActiveState(event.workflowInstance());

    if (currentRunStateOpt.isEmpty()) {
      var message = "Received event for unknown workflow instance: " + event;
      log.warn(message);
      return Optional.empty();
    }
    var currentRunState = currentRunStateOpt.orElseThrow();

    // Verify counters for in-order event processing
    verifyCounter(event, expectedCounter, currentRunState);
    log.info("Received event (verified) {}", event);

    // Compute next state
    var nextRunState = nextRunState(event, currentRunState);

    // Resource limiting occurs by throwing here, or by failing the commit with a conflict.
    // TODO: emit info message in this transaction instead
    updateResourceCounters(tx, event, currentRunState, nextRunState);

    // Write new state to datastore (or remove it if terminal)
    if (nextRunState.state().isTerminal()) {
      tx.deleteActiveState(event.workflowInstance());
    } else {
      tx.updateActiveState(event.workflowInstance(), nextRunState);
    }

    return Optional.of(nextRunState);
  }

  private RunState nextRunState(Event event, RunState runState) {
    try {
      return runState.transition(event, time);
    } catch (IllegalStateException e) {
      log.warn("Illegal state transition", e);
      throw e;
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
                                  Set<String> resourceIds) throws IOException {

    var depletedResourceIds = new ArrayList<String>();
    for (var resourceId : resourceIds) {
      try {
        tx.updateCounter(shardedCounter, resourceId, 1);
      } catch (CounterCapacityException e) {
        depletedResourceIds.add(resourceId);
      }
    }

    if (!depletedResourceIds.isEmpty()) {
      var message = MessageUtil.emitResourceLimitReachedMessage(this, runState, depletedResourceIds);
      throw new CounterCapacityException("Failed to update resource counter for workflow instance: " +
                                         runState.workflowInstance() + ": " + message.line());
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
      throw new StateTransitionConflictException(message);
    } else if (currentCounter < expectedCounter) {
      // This should never happen
      final String message = "Unexpected current counter is less than last observed one for "
                             + currentRunState;
      log.error(message);
      throw new RuntimeException(message);
    }
  }

  private void postTransition(Event event, RunState runState) {
    var sequenceEvent = SequenceEvent.create(event, runState.counter(), runState.timestamp());

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
    try {
      outputHandler.transitionInto(runState, this);
    } catch (StateTransitionConflictException e) {
      log.debug("State transition conflict when invoking output handler: {}", runState.workflowInstance(), e);
    }
  }

  @Override
  public Set<WorkflowInstance> listActiveInstances() {
    return Try.of(storage::listActiveInstances).get();
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
}
