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

import static com.spotify.styx.ScheduledExecutionUtil.JITTER;
import static com.spotify.styx.ScheduledExecutionUtil.scheduleWithJitter;
import static com.spotify.styx.util.ClassEnforcer.assertNotInstantiable;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.Test;

public class ScheduledExecutionUtilTest {

  private static final DeterministicScheduler SCHEDULER = new DeterministicScheduler();

  private static final Duration INTERVAL = Duration.ofSeconds(10);

  @Test
  public void shouldNotBeConstructable() throws ReflectiveOperationException {
    assertTrue(assertNotInstantiable(ScheduledExecutionUtil.class));
  }

  @Test
  public void shouldScheduleWithJitter() {
    // Set up a runnable that records execution timings
    var executionTimes = new ArrayList<Integer>();
    var time = new AtomicInteger();
    Runnable runnable = () -> executionTimes.add(time.get());

    // Schedule the runnable on an executor we can control
    scheduleWithJitter(runnable, SCHEDULER, INTERVAL);

    // Simulate running it for 10k seconds
    for (int i = 0; i < 10000; i++) {
      time.set(i);
      SCHEDULER.tick(1, SECONDS);
    }

    // Calculate timing statistics
    var statistics = IntStream.range(0, executionTimes.size() - 1)
        .map(i -> executionTimes.get(i + 1) - executionTimes.get(i))
        .summaryStatistics();

    assertThat(statistics.getAverage(), is(closeTo(INTERVAL.getSeconds(), 3)));
    assertThat((double) statistics.getMin(), is(
        both(greaterThanOrEqualTo(INTERVAL.getSeconds() * (1.0 - JITTER)))
            .and(lessThanOrEqualTo(INTERVAL.getSeconds() * 1.0))));
    assertThat((double) statistics.getMax(), is(
        both(greaterThanOrEqualTo(INTERVAL.getSeconds() * 1.0))
            .and(lessThanOrEqualTo(INTERVAL.getSeconds() * (1.0 + JITTER)))));
  }
}
