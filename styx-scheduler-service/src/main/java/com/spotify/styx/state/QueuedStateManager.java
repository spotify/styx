/*
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
import com.google.common.collect.Maps;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.storage.EventStorage;
import com.spotify.styx.util.Time;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of {@link StateManager} that has an internal queue for all the incoming
 * {@link Event}s that are sent to {@link #receive(Event)}.
 *
 * The events are processed sequentially on a single thread.
 *
 * This implementation also takes an {@link ExecutorService} as a dependency. This executor is used
 * to run all {@link RunState#outputHandler()} transitions.
 */
public class QueuedStateManager implements StateManager, StaleStateReaper, StateRetrier {

  private static final Logger LOG = LoggerFactory.getLogger(QueuedStateManager.class);

  static final String EVENT_POLLER_THREAD_NAME = "styx-event-poller";
  static final int EVENT_QUEUE_SIZE = 1024;
  static final int POLL_TIMEOUT_MILLIS = 10;
  static final int SHUTDOWN_GRACE_PERIOD_SECONDS = 5;
  static final long NO_EVENTS_PROCESSED = -1L;

  private final TimeoutConfig ttls;
  private final Time time;
  private final ExecutorService outputWorkers;
  private final EventStorage storage;

  private final Map<WorkflowInstance, RunState> activeStates = Maps.newHashMap();
  private final Map<WorkflowInstance, Long> eventCounters = Maps.newHashMap();
  private final BlockingQueue<Runnable> eventQueue = new LinkedBlockingQueue<>(EVENT_QUEUE_SIZE);

  private final Thread pollerThread;
  private final CountDownLatch closedLatch = new CountDownLatch(1);
  private volatile boolean running = true;
  private volatile int activeEvents = 0;

  public QueuedStateManager(
      TimeoutConfig ttls,
      Time time,
      ExecutorService outputWorkers,
      EventStorage storage) {
    this.ttls = Objects.requireNonNull(ttls);
    this.time = Objects.requireNonNull(time);
    this.outputWorkers = Objects.requireNonNull(outputWorkers);
    this.storage = Objects.requireNonNull(storage);

    this.pollerThread = new Thread(this::eventLoop);
    pollerThread.setName(EVENT_POLLER_THREAD_NAME);
    pollerThread.start();
  }


  @Override
  public void initialize(RunState runState) throws IsClosed {
    ensureRunning();
    enqueue(() -> {
      try {
        if (activeStates.containsKey(runState.workflowInstance())) {
          LOG.error("RunState initialization called on active instance");
          return;
        }

        final long counter = storage.getLatestStoredCounter(
            runState.workflowInstance()).orElse(NO_EVENTS_PROCESSED);
        storeActivation(runState.workflowInstance(), counter);
        activeStates.put(runState.workflowInstance(), runState);
        eventCounters.put(runState.workflowInstance(), counter + 1);
      } catch (IOException e) {
        LOG.error("Failed to read/write from/to BigTable", e);
      }
    });
  }

  @Override
  public void restore(RunState runState, long count) {
    enqueue(() -> {
      activeStates.put(runState.workflowInstance(), runState);
      eventCounters.put(runState.workflowInstance(), count + 1);
    });
  }

  @Override
  public void receive(Event event) throws IsClosed {
    ensureRunning();
    enqueue(() -> {
      RunState currentState = activeStates.get(event.workflowInstance());
      if (currentState == null) {
        LOG.warn("Received event for unknown workflow instance: {}", event);
        return;
      }

      transition(currentState, event);
    });
  }

  @Override
  public long getActiveStatesCount() {
    return activeStates.size();
  }

  @Override
  public long getQueuedEventsCount() {
    return eventQueue.size();
  }

  @Override
  public long getActiveStatesCount(WorkflowId workflowId) {
    return activeStates
        .keySet()
        .stream()
        .filter(workflowInstance -> workflowInstance.workflowId().equals(workflowId))
        .count();
  }

  @Override
  public boolean isActiveWorkflowInstance(WorkflowInstance workflowInstance) {
    return activeStates.containsKey(workflowInstance);
  }

  @Override
  public RunState get(WorkflowInstance workflowInstance) {
    return activeStates.get(workflowInstance);
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
        pollerThread.interrupt();
        throw new IOException(
            "Graceful shutdown failed, event loop did not finish within grace period");
      }
    } catch (InterruptedException e) {
      pollerThread.interrupt();
      throw new IOException(e);
    }

    LOG.info("Shutdown was clean, {} event left in queue", eventQueue.size());
  }

  @Override
  public void triggerTimeouts() {
    if (!running) {
      return;
    }

    activeStates.keySet().stream()
        .filter(key -> hasTimedOut(activeStates.get(key)))
        .forEach(key -> enqueue(() -> {
          // check again from transition loop
          RunState currentState = activeStates.get(key);
          if (hasTimedOut(currentState)) {
            LOG.info("Found stale state, triggering timeout for {}", currentState);
            try {
              receive(Event.timeout(key));
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

    activeStates.keySet().stream()
        .filter(key -> shouldRetry(activeStates.get(key)))
        .forEach(key -> enqueue(() -> {
          // check again from transition loop
          RunState currentState = activeStates.get(key);
          if (shouldRetry(currentState)) {
            LOG.info("{} triggering retry #{}", key.toKey(), currentState.tries());
            try {
              receive(Event.retry(key));
            } catch (IsClosed ignored) {
            }
          }
        }));
  }

  private void eventLoop() {
    while (running || !eventQueue.isEmpty()) {
      Runnable runnable = null;
      try {
        runnable = eventQueue.poll(POLL_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        if (runnable != null) {
          runnable.run();
        }
      } catch (InterruptedException e) {
        LOG.warn("Event poller loop got interrupted, events might been dropped", e);
        break;
      } catch (Exception e) {
        LOG.warn("Exception in event loop run", e);
      } finally {
        if (runnable != null) {
          --activeEvents;
        }
      }
    }

    closedLatch.countDown();
  }

  private void ensureRunning() throws IsClosed {
    if (!running) {
      throw new IsClosed();
    }
  }

  private void enqueue(Runnable transition) {
    ++activeEvents;
    if (!eventQueue.offer(transition)) {
      --activeEvents;
      throw new RuntimeException("Transition queue is full");
    }
  }

  private void transition(RunState state, Event event) {
    LOG.debug("Event {} -> {}", event, state);
    try {
      final RunState nextState = state.transition(event);
      final WorkflowInstance key = state.workflowInstance();

      final long currentCount = eventCounters.get(event.workflowInstance());
      final SequenceEvent sequenceEvent = SequenceEvent.create(
          event,
          currentCount,
          time.get().toEpochMilli());
      storeEvent(sequenceEvent);

      if (nextState.state().isTerminal()) {
        activeStates.remove(key);
        eventCounters.remove(key);
        storeDeactivation(key);
      } else {
        activeStates.put(key, nextState);
        eventCounters.compute(key, (ignore, counter) -> counter + 1);
        storeActivation(key, currentCount);
      }

      ++activeEvents;
      outputWorkers.execute(() -> {
        try {
          nextState.outputHandler().transitionInto(nextState);
        } catch (Throwable e) {
          LOG.warn("Output handler threw", e);
        } finally {
          --activeEvents;
        }
      });
    } catch (IllegalStateException e) {
      LOG.warn("Illegal state transition", e);
    } catch (IOException e) {
      LOG.error("Failed to read/write from/to BigTable", e);
    }
  }

  private void storeEvent(SequenceEvent sequenceEvent) throws IOException {
    storage.writeEvent(sequenceEvent);
  }

  private void storeActivation(WorkflowInstance workflowInstance, long lastProcessedCount)
      throws IOException {
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
    final long retryDelayMillis = runState.retryDelayMillis();

    return ageMillis >= retryDelayMillis;
  }

  @VisibleForTesting
  boolean awaitIdle(long timeoutMillis) {
    final long t0 = time.get().toEpochMilli();
    while (activeEvents > 0 && (time.get().toEpochMilli() - t0) < timeoutMillis) {
      Thread.yield();
    }

    return (time.get().toEpochMilli() - t0) < timeoutMillis;
  }
}
