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
package com.spotify.styx.monitoring;

import com.spotify.styx.util.FnWithException;
import com.spotify.styx.util.RunnableWithException;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;

class MeteredBase {

  protected final Stats stats;
  protected final Time time;

  MeteredBase(Stats stats, Time time) {
    this.stats = Objects.requireNonNull(stats);
    this.time = Objects.requireNonNull(time);
  }

  protected void timedDocker(String operation, Runnable runnable) {
    final Instant t0 = time.get();
    runnable.run();
    final Instant t1 = time.get();

    final long durationMillis = t0.until(t1, ChronoUnit.MILLIS);
    stats.dockerOperation(operation, durationMillis);
  }

  protected String timedDocker(String operation, FnWithException<String, IOException> runnable)
      throws IOException {

    final Instant t0 = time.get();
    String executionId = runnable.apply();
    final Instant t1 = time.get();

    final long durationMillis = t0.until(t1, ChronoUnit.MILLIS);
    stats.dockerOperation(operation, durationMillis);
    return executionId;
  }

  protected void timedStorage(String operation, RunnableWithException<IOException> runnable)
      throws IOException {

    final Instant t0 = time.get();
    runnable.run();
    final Instant t1 = time.get();

    final long durationMillis = t0.until(t1, ChronoUnit.MILLIS);
    stats.storageOperation(operation, durationMillis);
  }

  protected <T> T timedStorage(String operation, FnWithException<T, IOException> runnable)
      throws IOException {

    final Instant t0 = time.get();
    final T val = runnable.apply();
    final Instant t1 = time.get();

    final long durationMillis = t0.until(t1, ChronoUnit.MILLIS);
    stats.storageOperation(operation, durationMillis);

    return val;
  }
}
