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
package com.spotify.styx;

import com.spotify.styx.model.DataEndpoint;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.util.Time;
import com.spotify.styx.workflow.ParameterUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ScheduledExecutorService;

import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MILLIS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Schedules periodic triggers for a {@link com.spotify.styx.schedule.model.ScheduleDefinition}.
 */
class TickTock implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(TickTock.class);

  private static final int DAYS_IN_ONE_WEEK = 7;
  private static final String NATURAL_TRIGGER_ID = "natural";


  private final Workflow workflow;
  private final TriggerListener triggerListener;
  private final ScheduledExecutorService exec;
  private final Time time;

  private volatile boolean running = false;

  TickTock(
      Workflow workflow,
      TriggerListener triggerListener,
      Time time,
      ScheduledExecutorService exec) {
    this.workflow = requireNonNull(workflow);
    this.exec = requireNonNull(exec);
    this.time = requireNonNull(time);
    this.triggerListener = requireNonNull(triggerListener);
  }

  Workflow workflow() {
    return workflow;
  }

  void start() {
    if (running) {
      return;
    }
    running = true;

    final DataEndpoint dataEndpoint = workflow.schedule();
    final Duration duration;
    final Instant nextTriggeredInstant;

    switch (dataEndpoint.partitioning()) {
      case DAYS:
        nextTriggeredInstant = time.get().truncatedTo(DAYS);
        duration = Duration.ofDays(1);
        break;

      case HOURS:
        nextTriggeredInstant = time.get().truncatedTo(HOURS);
        duration = Duration.ofHours(1);
        break;

      case WEEKS:
        Instant instant = time.get();
        //a proper timezone needs to be specified
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        int daysToSubtract = dateTime.getDayOfWeek().getValue();
        dateTime = dateTime.minusDays(daysToSubtract - 1);
        Instant resultInstant = dateTime.toInstant(ZoneOffset.UTC);
        nextTriggeredInstant = resultInstant.truncatedTo(ChronoUnit.DAYS);

        duration = Duration.ofDays(DAYS_IN_ONE_WEEK);
        break;

      default:
        throw new IllegalStateException("Illegal partitioning: " + dataEndpoint.partitioning());
    }

    LOG.info(
        "Schedule {} : {} will trigger every {} seconds",
        dataEndpoint.id(), duration, duration.getSeconds());

    final Instant triggerInstant = nextTriggeredInstant.plus(duration);
    defer(workflow, triggerInstant, trigger(workflow, nextTriggeredInstant, duration));
  }

  /**
   * Defers execution of a task to a given {@link Instant}.
   *
   * @param workflow      The workflow that is scheduled
   * @param instant       The instant defer to
   * @param task          The task to defer
   */
  private void defer(Workflow workflow, Instant instant, Runnable task) {
    if (!running) {
      return;
    }

    final long untilNext = time.get().until(instant, MILLIS);
    final long delayMillis = Math.max(untilNext, 0);

    LOG.info(
        "Deferring {} to {}, due in {} s",
        workflow.schedule().id(),
        ParameterUtil.formatDateTime(instant),
        SECONDS.convert(delayMillis, MILLISECONDS));

    exec.schedule(task, delayMillis, MILLISECONDS);
  }

  private Runnable trigger(Workflow workflow, Instant instant, Duration duration) {
    return () -> {
      if (!running) {
        return;
      }

      final Instant nextTriggeredInstant = instant.plus(duration);
      final Instant triggerInstant = nextTriggeredInstant.plus(duration);

      try {
        logTrigger(workflow, instant, nextTriggeredInstant);
        triggerListener.event(workflow, NATURAL_TRIGGER_ID, instant);
      } catch (Exception e) {
        LOG.info("Trigger threw exception", e);
      }

      defer(workflow, triggerInstant, trigger(workflow, nextTriggeredInstant, duration));
    };
  }

  @Override
  public void close() throws IOException {
    LOG.info("Closing TickTock for {}", workflow);
    running = false;
  }

  private void logTrigger(Workflow workflow, Instant instant, Instant nextInstant) {
    LOG.info(
        "trig {} @ {}, off by {} ms, next is {}",
        workflow.schedule().id(),
        ParameterUtil.formatDateTime(instant),
        nextInstant.until(time.get(), ChronoUnit.MILLIS),
        ParameterUtil.formatDateTime(nextInstant));
  }

}
