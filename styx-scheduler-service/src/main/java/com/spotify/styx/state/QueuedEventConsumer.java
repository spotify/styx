/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.publisher.EventInterceptor;
import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single threaded asynchronous event consumer queue. It requires a {@link EventInterceptor}
 * implementation to act upon the queued events.
 */
public class QueuedEventConsumer implements EventConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(QueuedEventConsumer.class);

  private static final int EVENT_QUEUE_SIZE = 1000;
  private static final long POLL_TIMEOUT_MILLIS = 100;
  private final CountDownLatch closedLatch = new CountDownLatch(1);
  private static final String DISPATCHER_THREAD_NAME = "styx-event-consumer-dispatcher";
  private static final int SHUTDOWN_GRACE_PERIOD_SECONDS = 5;

  private final Thread dispatcherThread;
  private final Object signal = new Object();
  private final EventInterceptor eventInterceptor;
  private final Queue<SequenceEvent> queue = new LinkedBlockingQueue<>(EVENT_QUEUE_SIZE);

  private volatile boolean running = true;

  public QueuedEventConsumer(EventInterceptor eventReceiver) {
    this.eventInterceptor = Objects.requireNonNull(eventReceiver);
    this.dispatcherThread = new Thread(this::dispatch);
    dispatcherThread.setName(DISPATCHER_THREAD_NAME);
    dispatcherThread.start();
  }

  @Override
  public void processedEvent(SequenceEvent sequenceEvent) throws IsClosed {
    if (!running) {
      throw new IsClosed();
    }
    if (!queue.offer(sequenceEvent)) {
      LOG.warn("Events queue is full, skipping {}", sequenceEvent.event());
    } else {
      signalDispatcher();
    }
  }

  private void dispatch() {
    while (running || queueSize() > 0) {
      if (queueSize() > 0) {
        try {
          eventInterceptor.interceptedEvent(queue.remove());
        } catch (NoSuchElementException e) {
          throw new RuntimeException("The queue should not be empty");
        } catch (Exception e) {
          LOG.warn("Received exception from event interceptor, {}", e);
        }
      } else {
        waitForSignal();
      }
    }
    closedLatch.countDown();
  }

  @VisibleForTesting
  int queueSize() {
    return queue.size();
  }

  private void signalDispatcher() {
    synchronized (signal) {
      signal.notifyAll();
    }
  }

  private void waitForSignal() {
    synchronized (signal) {
      try {
        signal.wait(POLL_TIMEOUT_MILLIS);
      } catch (InterruptedException ignored) {
      }
    }
  }

  @Override
  public void close() throws IOException {
    if (!running) {
      return;
    }
    running = false;

    LOG.info("Shutting down, waiting for queued events to be consumed");

    try {
      if (!closedLatch.await(SHUTDOWN_GRACE_PERIOD_SECONDS, TimeUnit.SECONDS)) {
        dispatcherThread.interrupt();
        LOG.warn("Graceful shutdown failed, {} events left in queue", queueSize());
        throw new IOException(
            "Graceful shutdown failed, event loop did not finish within grace period");
      }
    } catch (InterruptedException e) {
      dispatcherThread.interrupt();
      throw new IOException(e);
    }

    LOG.info("Shutdown was clean, {} events left in queue", queueSize());
  }
}
