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
import com.spotify.styx.storage.EventStorage;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
public class QueuedStateManager implements StateManager, StaleStateReaper, StateRetrier {

  private static final Logger LOG = LoggerFactory.getLogger(QueuedStateManager.class);

  static final String DISPATCHER_THREAD_NAME = "styx-event-dispatcher";
  static final int EVENT_QUEUE_SIZE = 1024;
  static final int POLL_TIMEOUT_MILLIS = 10;
  static final int SHUTDOWN_GRACE_PERIOD_SECONDS = 5;
  static final long NO_EVENTS_PROCESSED = -1L;

  private final TimeoutConfig ttls;
  private final Time time;
  private final Executor workerPool;
  private final EventStorage storage;

  private final ConcurrentMap<WorkflowInstance, InstanceState> states = Maps.newConcurrentMap();

  private final Thread dispatcherThread;
  private final CountDownLatch closedLatch = new CountDownLatch(1);
  private final Object signal = new Object();
  private AtomicInteger activeEvents = new AtomicInteger(0);
  private volatile boolean running = true;

  public QueuedStateManager(
      TimeoutConfig ttls,
      Time time,
      Executor workerPool,
      EventStorage storage) {
    this.ttls = Objects.requireNonNull(ttls);
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

    state.enqueue(() -> transition(state, event));
    signalDispatcher();
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

  @Override
  public void triggerTimeouts() {
    if (!running) {
      return;
    }

    states.entrySet().stream()
        .filter(entry -> hasTimedOut(entry.getValue().runState))
        .forEach(entry -> entry.getValue().enqueue(() -> {
          // check again from transition loop
          RunState currentState = states.get(entry.getKey()).runState;
          if (hasTimedOut(currentState)) {
            LOG.info("Found stale state, triggering timeout for {}", currentState);
            try {
              receive(Event.timeout(entry.getKey()));
            } catch (IsClosed ignored) {
            }
          }
        }));
  }

  @Override
  public void triggerRetries() {
    if (!running) {
      return;
    }

    states.entrySet().stream()
        .filter(entry -> shouldRetry(entry.getValue().runState))
        .forEach(entry -> entry.getValue().enqueue(() -> {
          final WorkflowInstance key = entry.getKey();
          // check again from transition loop
          RunState currentState = states.get(key).runState;
          if (shouldRetry(currentState)) {
            LOG.info("{} triggering retry #{}", key.toKey(), currentState.data().tries());
            try {
              receive(Event.retry(key));
            } catch (IsClosed ignored) {
            }
          }
        }));
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
   * Transition a state with the given event.
   *
   * <p>This method is only called from within a {@link InstanceState#enqueue(Runnable)} block
   * which means there will only be at most one concurrent call for each {@link InstanceState}.
   *
   * @param state  The state to transition
   * @param event  The event to transition the state with
   */
  private void transition(InstanceState state, Event event) {
    LOG.debug("Event {} -> {}", event, state);
    try {
      final RunState nextState = state.runState.transition(event);
      final WorkflowInstance key = state.workflowInstance;

      final long currentCount = state.counter;
      final SequenceEvent sequenceEvent = SequenceEvent.create(
          event,
          currentCount,
          time.get().toEpochMilli());
      storeEvent(sequenceEvent);

      if (nextState.state().isTerminal()) {
        states.remove(key); // racy when states are re-initialized concurrent with termination
        storeDeactivation(key);
      } else {
        state.runState = nextState;
        state.counter++;
        storeActivation(key, currentCount);
      }

      activeEvents.incrementAndGet();
      workerPool.execute(() -> {
        try {
          nextState.outputHandler().transitionInto(nextState);
        } catch (Throwable e) {
          LOG.warn("Output handler threw", e);
        } finally {
          activeEvents.decrementAndGet();
        }
      });
    } catch (IllegalStateException e) {
      LOG.warn("Illegal state transition", e);
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

  private boolean hasTimedOut(RunState runState) {
    if (runState.state().isTerminal()) {
      return false;
    }

    final long ageMillis = time.get().toEpochMilli() - runState.timestamp();
    final long ttlMillis = ttls.ttlOf(runState.state()).toMillis();
    return ageMillis >= ttlMillis;
  }

  private boolean shouldRetry(RunState runState) {
    if (runState.state() != RunState.State.AWAITING_RETRY) {
      return false;
    }

    final long ageMillis = time.get().toEpochMilli() - runState.timestamp();
    final long retryDelayMillis = runState.data().retryDelayMillis();

    return ageMillis >= retryDelayMillis;
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
