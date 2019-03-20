/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

package com.spotify.styx;

import static com.spotify.styx.util.GuardedRunnable.runGuarded;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;

public class ScheduledExecutionUtil {

  static final double JITTER = 0.5;

  private ScheduledExecutionUtil() {
    throw new UnsupportedOperationException();
  }

  /**
   * Schedule a runnable to execute at randomized intervals with a mean value of {@param interval}.
   * The random intervals are values from the uniform distribution interval * [1.0 - JITTER, 1.0 + JITTER].
   *
   * @param runnable The {@link Runnable} to schedule.
   * @param exec     The {@link ScheduledExecutorService} to schedule the runnable on.
   * @param interval The mean scheduled execution interval.
   */
  public static void scheduleWithJitter(Runnable runnable, ScheduledExecutorService exec, Duration interval) {
    final double jitter = ThreadLocalRandom.current().nextDouble(1.0 - JITTER, 1.0 + JITTER);
    final long delayMillis = (long) (jitter * interval.toMillis());
    exec.schedule(() -> {
      runGuarded(runnable);
      scheduleWithJitter(runnable, exec, interval);
    }, delayMillis, MILLISECONDS);
  }
}
