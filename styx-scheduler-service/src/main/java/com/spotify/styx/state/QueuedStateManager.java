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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
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
 * <p>All {@link RunState#outputHandler()} transitions are also executed on the injected
 * {@link Executor}.
 */
public class QueuedStateManager implements StateManager {

  private static final Logger LOG = LoggerFactory.getLogger(QueuedStateManager.class);

  static final String DISPATCHER_THREAD_NAME = "styx-event-dispatcher";
  static final int EVENT_QUEUE_SIZE = 1024;
  static final int POLL_TIMEOUT_MILLIS = 10;
  static final int SHUTDOWN_GRACE_PERIOD_SECONDS = 5;
  static final long NO_EVENTS_PROCESSED = -1L;

  private final Time time;
  private final Executor workerPool;
  private final Storage storage;

  private final ConcurrentMap<WorkflowInstance, InstanceState> states = Maps.newConcurrentMap();

  private final Thread dispatcherThread;
  private final CountDownLatch closedLatch = new CountDownLatch(1);
  private final Object signal = new Object();
  private AtomicInteger activeEvents = new AtomicInteger(0);
  private volatile boolean running = true;

  public QueuedStateManager(Time time, Executor workerPool, Storage storage) {
    this.time = Objects.requireNonNull(time);
    this.workerPool = Objects.requireNonNull(workerPool);
    this.storage = Objects.requireNonNull(storage);

    this.dispatcherThread = new Thread(this::dispatch);
    dispatcherThread.setName(DISPATCHER_THREAD_NAME);
    dispatcherThread.start();
  }

  @Override
  public void initialize(RunState runState) throws IsClosed {
    ensureRunning();

    final WorkflowInstance workflowInstance = runState.workflowInstance();
    if (states.containsKey(workflowInstance)) {
      throw new AlreadyInitializedException("RunState initialization called on active instance "
                                            + workflowInstance.toKey());
    }

    final long counter;
    try {
      counter = storage.getLatestStoredCounter(workflowInstance).orElse(NO_EVENTS_PROCESSED);
      storeActivation(workflowInstance, counter);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    states.computeIfAbsent(workflowInstance, (wfi) -> new InstanceState(wfi, runState, counter + 1));
  }

  @Override
  public void restore(RunState runState, long count) {
    final WorkflowInstance workflowInstance = runState.workflowInstance();
    if (states.containsKey(workflowInstance)) {
      throw new RuntimeException("RunState initialization called on active instance "
                                 + workflowInstance.toKey());
    }

    states.computeIfAbsent(workflowInstance, (wfi) -> new InstanceState(wfi, runState, count + 1));
  }

  @Override
  public void receive(Event event) throws IsClosed {
    ensureRunning();

    final InstanceState state = states.get(event.workflowInstance());
    if (state == null) {
      LOG.warn("Received event for unknown workflow instance: {}", event);
      return;
    }

    state.transition(event);
    signalDispatcher();
  }

  @Override
  public Map<WorkflowInstance, RunState> activeStates() {
    return states.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, (entry) -> entry.getValue().runState));
  }

  @Override
  public long getActiveStatesCount() {
    return states.size();
  }

  @Override
  public long getQueuedEventsCount() {
    return states.values().stream()
        .mapToInt(queue -> queue.queue.size())
        .sum();
  }

  @Override
  public long getActiveStatesCount(WorkflowId workflowId) {
    return states
        .keySet()
        .stream()
        .filter(workflowInstance -> workflowInstance.workflowId().equals(workflowId))
        .count();
  }

  @Override
  public boolean isActiveWorkflowInstance(WorkflowInstance workflowInstance) {
    return states.containsKey(workflowInstance);
  }

  @Override
  public RunState get(WorkflowInstance workflowInstance) {
    final InstanceState instanceState = states.get(workflowInstance);
    return instanceState != null ? instanceState.runState : null;
  }

  @Override
  public void close() throws IOException {
    if (!running) {
      return;
    }
    running = false;

    LOG.info("Shutting down, waiting for queued events to process");

    try {
      if (!closedLatch.await(SHUTDOWN_GRACE_PERIOD_SECONDS, TimeUnit.SECONDS)) {
        dispatcherThread.interrupt();
        throw new IOException(
            "Graceful shutdown failed, event loop did not finish within grace period");
      }
    } catch (InterruptedException e) {
      dispatcherThread.interrupt();
      throw new IOException(e);
    }

    LOG.info("Shutdown was clean, {} events left in queue", getQueuedEventsCount());
  }

  /**
   * Dispatch loop, continuously running on {@link #dispatcherThread}. Mainly calling
   * {@link InstanceState#mutexPoll()} on all active states.
   *
   * <p>The dispatch loop will make a call to {@link #waitForSignal()} between each loop. It does
   * so to prevent a busy spin that would hog up a full core. The signalling on the other hand
   * eliminates a systematic latency in event processing.
   */
  private void dispatch() {
    while (running || getQueuedEventsCount() > 0) {
      states.values().forEach(InstanceState::mutexPoll);
      waitForSignal();
    }

    closedLatch.countDown();
  }

  /**
   * Sends a signal to the dispatcher thread in order to unblock it's timed wait. Calling this
   * method is not crucial for the dispatcher thread to do its work, but will unblock it in case
   * it's waiting.
   *
   * <p>This should be called from all methods that receive a new event.
   */
  private void signalDispatcher() {
    synchronized (signal) {
      signal.notifyAll();
    }
  }

  /**
   * Block (up to {@link #POLL_TIMEOUT_MILLIS} on a call to {@link #signalDispatcher()}.
   */
  private void waitForSignal() {
    synchronized (signal) {
      try {
        signal.wait(POLL_TIMEOUT_MILLIS);
      } catch (InterruptedException ignored) {
      }
    }
  }

  private void ensureRunning() throws IsClosed {
    if (!running) {
      throw new IsClosed();
    }
  }

  /**
   * Process a state at a given counter position.
   *
   * <p>Processing a state mean that the event and counter positions that caused the transition
   * will be persisted, and the {@link OutputHandler} of the state is called.
   *
   * <p>This method is only called from within a {@link InstanceState#enqueue(Runnable)} block
   * which means there will only be at most one concurrent call for each {@link InstanceState}.
   *
   * @param state            The current state that is being processed
   * @param counterPosition  The counter position at which the state transitioned
   * @param event            The event that transitioned the state
   */
  private void process(RunState state, long counterPosition, Event event) {
    final WorkflowInstance key = state.workflowInstance();
    final SequenceEvent sequenceEvent = SequenceEvent.create(
        event,
        counterPosition,
        time.get().toEpochMilli());

    try {
      storeEvent(sequenceEvent);

      if (state.state().isTerminal()) {
        states.remove(key); // racy when states are re-initialized concurrent with termination
        storeDeactivation(key);
      } else {
        storeActivation(key, counterPosition);
      }

      activeEvents.incrementAndGet();
      workerPool.execute(() -> {
        try {
          state.outputHandler().transitionInto(state);
        } catch (Throwable e) {
          LOG.warn("Output handler threw", e);
        } finally {
          activeEvents.decrementAndGet();
        }
      });
    } catch (IOException e) {
      LOG.error("Failed to read/write from/to Storage", e);
    }
  }

  private void storeEvent(SequenceEvent sequenceEvent) throws IOException {
    storage.writeEvent(sequenceEvent);
  }

  private void storeActivation(WorkflowInstance workflowInstance, long lastProcessedCount) throws IOException {
    storage.writeActiveState(workflowInstance, lastProcessedCount);
  }

  private void storeDeactivation(WorkflowInstance workflowInstance) throws IOException {
    storage.deleteActiveState(workflowInstance);
  }

  @VisibleForTesting
  boolean awaitIdle(long timeoutMillis) {
    final long t0 = time.get().toEpochMilli();
    while (activeEvents.get() > 0 && (time.get().toEpochMilli() - t0) < timeoutMillis) {
      Thread.yield();
    }

    return (time.get().toEpochMilli() - t0) < timeoutMillis;
  }

  private class InstanceState {

    final WorkflowInstance workflowInstance;
    final Queue<Runnable> queue = new LinkedBlockingQueue<>(EVENT_QUEUE_SIZE);
    final Semaphore mutex = new Semaphore(1);

    volatile RunState runState;
    volatile long counter;

    InstanceState(WorkflowInstance workflowInstance, RunState runState, long counter) {
      this.workflowInstance = workflowInstance;
      this.runState = runState;
      this.counter = counter;
    }

    /**
     * Transition the state and counter with the given {@link Event}. Then {@link #enqueue(Runnable)}
     * a call to {@link #process(RunState, long, Event)} for persisting and acting on the
     * transitioned state.
     *
     * <p>This method is synchronized so it can make local field updates safely. It is only
     * synchronized on the inner {@link InstanceState} object, so transitions for different
     * {@link WorkflowInstance}s can run concurrently.
     *
     * @param event  The event to use in the transition
     */
    synchronized void transition(Event event) {
      LOG.debug("Event {} -> {}", event, this);

      final RunState nextState;
      try {
        nextState = runState.transition(event);
      } catch (IllegalStateException e) {
        LOG.warn("Illegal state transition", e);
        return;
      }

      final long currentCount = counter++;
      runState = nextState;

      enqueue(() -> process(nextState, currentCount, event));
    }

    void enqueue(Runnable transition) {
      if (queue.offer(transition)) {
        activeEvents.incrementAndGet();
      } else {
        throw new RuntimeException("Transition queue for " + workflowInstance.toKey() + " is full");
      }
    }

    /**
     * Poll the next {@link Runnable} off the {@link #queue} and invoke it on the
     * {@link #workerPool}, or do nothing if the queue is empty.
     *
     * <p>The whole operation is guarded with a mutex, so concurrent calls are safe. Only one
     * queued {@link Runnable} will be invoked at any point time, effectively making the queue
     * consumed in a synchronized fashion.
     *
     * <p>After each invocation has completed, the task on the {@link #workerPool} will call
     * {@code mutexPoll()} again to ensure immediate consequent consumption of the queue.
     */
    void mutexPoll() {
      if (queue.isEmpty()) {
        return;
      }

      if (mutex.tryAcquire()) {
        try {
          // poll and invoke on executor pool
          workerPool.execute(() -> {
            try {
              final Runnable poll = queue.poll();
              if (poll != null) {
                try {
                  invoke(poll);
                } finally {
                  activeEvents.decrementAndGet();
                }
              }
            } finally {
              mutex.release();
            }

            // continue to consume queue
            mutexPoll();
          });
        } catch (Throwable e) {
          LOG.error("Failed to submit event worker task", e);
          mutex.release();
        }
      }
    }

    void invoke(Runnable transition) {
      try {
        transition.run();
      } catch (Throwable e) {
        LOG.warn("Exception in event runnable for {}", workflowInstance.toKey(), e);
      }
    }
  }
}
