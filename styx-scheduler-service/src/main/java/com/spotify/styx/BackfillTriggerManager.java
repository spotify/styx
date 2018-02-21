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
import static com.spotify.styx.util.GuardedRunnable.guard;
import static com.spotify.styx.util.TimeUtil.nextInstant;

import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillBuilder;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerUtil;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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

    Collections.shuffle(backfills);

    backfills.forEach(backfill -> guard(() -> triggerBackfill(backfill)).run());

    final long durationMillis = t0.until(time.get(), ChronoUnit.MILLIS);
    stats.recordTickDuration(TICK_TYPE, durationMillis);
  }

  private void triggerBackfill(Backfill backfill) {
    final Optional<Workflow> workflowOpt = workflowCache.workflow(backfill.workflowId());
    if (!workflowOpt.isPresent()) {
      LOG.warn("workflow not found for backfill, skipping rest of triggers: {}", backfill);
      final BackfillBuilder builder = backfill.builder();
      builder.halted(true);
      storeBackfill(builder.build());
      return;
    }

    final Workflow workflow = workflowOpt.get();

    while (true) {
      try {
        if (!storage.runInTransaction(tx -> tryTriggerAndProgress(tx, backfill.id(), workflow))) {
          break;
        }
      } catch (IOException e) {
        // transaction failed, yield
        break;
      }
    }
  }

  private Boolean tryTriggerAndProgress(StorageTransaction tx,
                                        String id, Workflow workflow) throws IOException {
    final Backfill momentBackfill =
        storage.backfill(id).orElseThrow(RuntimeException::new);

    final Instant partition = momentBackfill.nextTrigger();
    final int remainingCapacity =
        momentBackfill.concurrency() - getNumberOfBackfillStates(momentBackfill.id());

    if (remainingCapacity < 1 || !partition.isBefore(momentBackfill.end())) {
      return false;
    }

    try {
      final CompletableFuture<Void> processed = triggerListener
          .event(workflow, Trigger.backfill(momentBackfill.id()), partition)
          .toCompletableFuture();
      // Wait for the trigger execution to complete before proceeding to the next partition
      processed.get();
    } catch (AlreadyInitializedException ignored) {
      // just encountered an ad-hoc trigger or it is handled by another scheduler instance
    } catch (Exception e) {
      // whatever, can be failed to trigger, can be transaction conflict
      throw new RuntimeException(e);
    }

    final Instant nextPartition = nextInstant(partition, momentBackfill.schedule());
    tx.store(momentBackfill.builder()
                      .nextTrigger(nextPartition)
                      .build());

    if (nextPartition.equals(momentBackfill.end())) {
      tx.store(momentBackfill.builder()
                           .nextTrigger(momentBackfill.end())
                           .allTriggered(true)
                           .build());
      return false;
    }

    return true;
  }

  // TODO: push down the filter to datastore
  private int getNumberOfBackfillStates(String id) {
    return stateManager.activeStates().entrySet().stream()
        .map(entry -> entry.getValue().data().trigger())
        .filter(Optional::isPresent)
        .map(Optional::get)
        .filter(trigger -> id.equals(TriggerUtil.triggerId(trigger)))
        .mapToInt(x -> 1)
        .sum();
  }

  private void storeBackfill(Backfill backfill) {
    try {
      storage.storeBackfill(backfill);
    } catch (IOException e) {
      LOG.warn("Failed to store updated backfill", e);
    }
  }
}
