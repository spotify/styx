/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
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

package com.spotify.styx.util;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.google.common.io.Closer;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CloserUtil {

  static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);

  private CloserUtil() {
    throw new UnsupportedOperationException();
  }

  private static final Logger log = LoggerFactory.getLogger(CloserUtil.class);

  public static <T extends ExecutorService> T register(Closer closer, T executorService, String name) {
    return register(closer, executorService, name, DEFAULT_TIMEOUT);
  }

  public static <T extends ExecutorService> T register(Closer closer, T executorService, String name,
      Duration timeout) {
    closer.register(closeable(executorService, name, timeout));
    return executorService;
  }

  public static Closeable closeable(ExecutorService executor, String name) {
    return closeable(executor, name, DEFAULT_TIMEOUT);
  }

  public static Closeable closeable(ExecutorService executor, String name, Duration timeout) {
    return () -> close(executor, name, timeout);
  }

  private static void close(ExecutorService executor, String name, Duration timeout) {
    log.debug("Shutting down executor: {}", name);
    executor.shutdown();
    try {
      executor.awaitTermination(timeout.toNanos(), NANOSECONDS);
    } catch (InterruptedException ignored) {
      log.warn("Interrupted");
      Thread.currentThread().interrupt();
    }
    final List<Runnable> runnables = executor.shutdownNow();
    if (!runnables.isEmpty()) {
      log.warn("{} task(s) in {} did not execute", runnables.size(), name);
    }
  }
}
