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
import com.spotify.styx.publisher.EventConsumer;
import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Single threaded asynchronous event consumer queue. It requires a {@link EventConsumer}
 * implementation to act upon the queued events.
 */
public class QueuedEventConsumer implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(QueuedEventConsumer.class);
  private static final int SHUTDOWN_GRACE_PERIOD_SECONDS = 5;

  private final EventConsumer eventConsumer;
  private final ThreadPoolExecutor executor;

  public QueuedEventConsumer(EventConsumer eventConsumer) {
    this.eventConsumer = Objects.requireNonNull(eventConsumer);
    this.executor = new ThreadPoolExecutor( 1, 1, 0L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
  }

  void enqueue(SequenceEvent sequenceEvent) throws IsClosed {
    if (executor.isTerminating() || executor.isShutdown()) {
      throw new IsClosed();
    }
    try {
      executor.execute(() -> eventConsumer.event(sequenceEvent));
    } catch (Exception e) {
      LOG.warn("Exception while consuming event {}: {}", sequenceEvent.event(), e);
    }
  }

  @VisibleForTesting
  int queueSize() {
    return executor.getQueue().size();
  }

  @Override
  public void close() throws IOException {
    if (executor.isTerminating() || executor.isShutdown()) {
      return;
    }

    executor.shutdown();
    LOG.info("Shutting down, waiting for queued events to be consumed");
    try {
      if (!executor.awaitTermination(SHUTDOWN_GRACE_PERIOD_SECONDS, TimeUnit.SECONDS)) {
        executor.shutdownNow();
        LOG.warn("Graceful shutdown failed, {} events left in queue", queueSize());
        throw new IOException(
            "Graceful shutdown failed, event loop did not finish within grace period");
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      throw new IOException(e);
    }
    LOG.info("Shutdown was clean, {} events left in queue", queueSize());
  }

  /**
   * Exception that signals that the {@link QueuedEventConsumer} is in a closed state.
   */
  class IsClosed extends Exception {
  }
}
