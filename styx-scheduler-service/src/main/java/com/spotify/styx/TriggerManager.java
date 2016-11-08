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

package com.spotify.styx;

import static com.spotify.styx.StyxScheduler.guard;
import static com.spotify.styx.workflow.ParameterUtil.decrementInstant;
import static com.spotify.styx.workflow.ParameterUtil.incrementInstant;
import static com.spotify.styx.workflow.ParameterUtil.truncateInstant;
import static java.util.Objects.requireNonNull;

import com.google.common.base.Throwables;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Triggers natural executions for {@link Workflow}s.
 */
public class TriggerManager {

  private static final Logger LOG = LoggerFactory.getLogger(TriggerManager.class);

  private static final int INITIAL_DELAY_SECONDS = 1;
  private static final int TICK_INTERVAL_SECONDS = 1;

  private final ScheduledExecutorService executor;
  private final TriggerListener triggerListener;
  private final Time time;
  private final Storage storage;

  public TriggerManager(ScheduledExecutorService exec,
                        TriggerListener triggerListener,
                        Time time,
                        Storage storage) {
    this.executor = requireNonNull(exec);
    this.triggerListener = requireNonNull(triggerListener);
    this.time = requireNonNull(time);
    this.storage = requireNonNull(storage);
  }

  public void start() {
    executor.scheduleAtFixedRate(
        guard(this::tick),
        INITIAL_DELAY_SECONDS,
        TICK_INTERVAL_SECONDS,
        TimeUnit.SECONDS);
  }

  void tick() {
    Map<Workflow, Optional<Instant>> map;
    Set<WorkflowId> enabled;
    try {
      map = storage.workflowsWithNextNaturalTrigger();
      enabled = storage.enabled();
    } catch (IOException e) {
      LOG.warn("Couldn't fetch workflows to trigger, skipping this run.");
      return;
    }

    Instant now = time.get();
    map.entrySet().forEach(entry -> {
      Workflow workflow = entry.getKey();
      Partitioning partitioning = workflow.schedule().partitioning();
      Instant naturalTrigger = entry.getValue().orElse(truncateInstant(now, partitioning));

      if (naturalTrigger.isAfter(now)) {
        return;
      }

      if (enabled.contains(workflow.id())) {
        try {
          triggerListener.event(
              workflow,
              "natural-trigger",
              decrementInstant(naturalTrigger, partitioning));
        } catch (AlreadyInitializedException e) {
          LOG.warn("{}", e.getMessage());
        }
      }

      Instant nextNaturalTrigger = incrementInstant(naturalTrigger, partitioning);
      try {
        storage.updateNextNaturalTrigger(workflow.id(), nextNaturalTrigger);
      } catch (IOException e) {
        LOG.error(
            "Sent trigger for workflow {}, but didn't succeed storing next scheduled run {}.",
            workflow.id(), nextNaturalTrigger);
        throw Throwables.propagate(e);
      }
    });
  }
}
