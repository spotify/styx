/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2017 Spotify AB
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

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static com.spotify.styx.util.TimeUtil.nextInstant;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillBuilder;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerUtil;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Triggers backfill executions for {@link Workflow}s.
 */
class BackfillTriggerManager {

  private static final Logger LOG = LoggerFactory.getLogger(BackfillTriggerManager.class);

  private static final String TICK_TYPE = UPPER_CAMEL.to(
      LOWER_UNDERSCORE, BackfillTriggerManager.class.getSimpleName());

  private final TriggerListener triggerListener;
  private final Storage storage;
  private final StateManager stateManager;
  private final WorkflowCache workflowCache;
  private final Stats stats;
  private final Time time;

  BackfillTriggerManager(StateManager stateManager,
                         WorkflowCache workflowCache, Storage storage,
                         TriggerListener triggerListener,
                         Stats stats,
                         Time time) {
    this.stateManager = Objects.requireNonNull(stateManager);
    this.workflowCache = Objects.requireNonNull(workflowCache);
    this.storage = Objects.requireNonNull(storage);
    this.triggerListener = Objects.requireNonNull(triggerListener);
    this.stats = Objects.requireNonNull(stats);
    this.time = Objects.requireNonNull(time);
  }

  void tick() {
    final Instant t0 = time.get();

    final List<Backfill> backfills;
    try {
      backfills = storage.backfills(false);
    } catch (IOException e) {
      LOG.warn("Failed to get backfills", e);
      return;
    }

    final Map<String, Long> backfillStates = getBackfillStates();
    backfills.forEach(backfill -> triggerBackfill(backfill, backfillStates));

    final long durationMillis = t0.until(time.get(), ChronoUnit.MILLIS);
    stats.recordTickDuration(TICK_TYPE, durationMillis);
  }

  private void triggerBackfill(Backfill backfill, Map<String, Long> backfillStates) {
    final Optional<Workflow> workflowOpt = workflowCache.workflow(backfill.workflowId());
    if (!workflowOpt.isPresent()) {
      LOG.warn("workflow not found for backfill, skipping rest of triggers: {}", backfill);
      final BackfillBuilder builder = backfill.builder();
      builder.halted(true);
      storeBackfill(builder.build());
      return;
    }

    final Workflow workflow = workflowOpt.get();

    final int remainingCapacity =
        backfill.concurrency() - backfillStates.getOrDefault(backfill.id(), 0L).intValue();

    Instant partition = backfill.nextTrigger();

    for (int i = 0; i < remainingCapacity && partition.isBefore(backfill.end()); i++) {
      try {
        final CompletableFuture<Void> processed = triggerListener
            .event(workflow, Trigger.backfill(backfill.id()), partition)
            .toCompletableFuture();
        // Wait for the trigger execution to complete before proceeding to the next partition
        processed.get();
      } catch (AlreadyInitializedException e) {
        LOG.warn("tried to trigger backfill for already active state [{}]: {}",
                 partition, backfill);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        LOG.error("failed to trigger backfill for state [{}]: {}",
                  partition, backfill);
        throw new RuntimeException(e);
      }

      partition = nextInstant(partition, backfill.schedule());
      storeBackfill(backfill.builder()
                        .nextTrigger(partition)
                        .build());
    }

    if (partition.equals(backfill.end())) {
      storeBackfill(backfill.builder()
                        .nextTrigger(backfill.end())
                        .allTriggered(true)
                        .build());
    }
  }

  private Map<String, Long> getBackfillStates() {
    final List<InstanceState> activeStates = stateManager.getActiveStates().entrySet().stream()
        .map(entry -> InstanceState.create(entry.getKey(), entry.getValue()))
        .collect(toList());

    return activeStates.stream()
        .map(state -> state.runState().data().trigger())
        .filter(Optional::isPresent)
        .map(Optional::get)
        .filter(TriggerUtil::isBackfill)
        .collect(groupingBy(
            TriggerUtil::triggerId,
            HashMap::new,
            counting()));
  }

  private void storeBackfill(Backfill backfill) {
    try {
      storage.storeBackfill(backfill);
    } catch (IOException e) {
      LOG.warn("Failed to store updated backfill", e);
    }
  }
}
