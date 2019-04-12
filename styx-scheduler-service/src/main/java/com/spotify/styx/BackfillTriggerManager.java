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
import static com.spotify.styx.util.ExceptionUtil.findCause;
import static com.spotify.styx.util.GuardedRunnable.guard;
import static com.spotify.styx.util.TimeUtil.instantsInRange;
import static com.spotify.styx.util.TimeUtil.instantsInReversedRange;
import static com.spotify.styx.util.TimeUtil.nextInstant;
import static com.spotify.styx.util.TimeUtil.previousInstant;

import com.google.common.annotations.VisibleForTesting;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.Time;
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Triggers backfill executions for {@link Workflow}s.
 */
class BackfillTriggerManager {

  private static final Logger LOG = LoggerFactory.getLogger(BackfillTriggerManager.class);

  private static final String TICK_TYPE = UPPER_CAMEL.to(
      LOWER_UNDERSCORE, BackfillTriggerManager.class.getSimpleName());

  private static Consumer<List<Backfill>> DEFAULT_SHUFFLER = Collections::shuffle;

  private static final Tracer tracer = Tracing.getTracer();

  private final TriggerListener triggerListener;
  private final Storage storage;
  private final StateManager stateManager;
  private final Stats stats;
  private final Time time;
  private final Consumer<List<Backfill>> shuffler;

  BackfillTriggerManager(StateManager stateManager,
                         Storage storage,
                         TriggerListener triggerListener,
                         Stats stats,
                         Time time) {
    this(stateManager, storage, triggerListener, stats, time, DEFAULT_SHUFFLER);
  }

  @VisibleForTesting
  BackfillTriggerManager(StateManager stateManager,
                         Storage storage,
                         TriggerListener triggerListener,
                         Stats stats,
                         Time time,
                         Consumer<List<Backfill>> shuffler) {
    this.stateManager = Objects.requireNonNull(stateManager);
    this.storage = Objects.requireNonNull(storage);
    this.triggerListener = Objects.requireNonNull(triggerListener);
    this.stats = Objects.requireNonNull(stats);
    this.time = Objects.requireNonNull(time);
    this.shuffler = Objects.requireNonNull(shuffler);
  }

  void tick() {
    try (Scope ss = tracer.spanBuilder("Styx.BackfillTriggerManager.tick")
        .setRecordEvents(true)
        .setSampler(Samplers.alwaysSample())
        .startScopedSpan()) {
      tick0();
    }
  }

  void tick0() {
    final Instant t0 = time.get();

    final List<Backfill> backfills;
    try {
      backfills = storage.backfills(false);
    } catch (IOException e) {
      LOG.warn("Failed to get backfills", e);
      return;
    }

    shuffler.accept(backfills);

    backfills.forEach(backfill -> guard(() -> triggerAndProgress(backfill)).run());

    final long durationMillis = t0.until(time.get(), ChronoUnit.MILLIS);
    stats.recordTickDuration(TICK_TYPE, durationMillis);

    tracer.getCurrentSpan().addAnnotation("processed",
        Map.of("backfills", AttributeValue.longAttributeValue(backfills.size())));
  }

  private void triggerAndProgress(Backfill backfill) {
    // Do not include all backfill spans in parent tick span to avoid it growing too big
    tracer.spanBuilderWithExplicitParent("Styx.BackfillTriggerManager.triggerAndProgress", null)
        .startSpanAndRun(() -> triggerAndProgress0(backfill));
  }

  private void triggerAndProgress0(Backfill backfill) {
    final Optional<Workflow> workflowOpt;
    try {
      workflowOpt = storage.workflow(backfill.workflowId());
    } catch (IOException e) {
      LOG.warn("Failed to read workflow {}", backfill.workflowId(), e);
      return;
    }

    if (!workflowOpt.isPresent()) {
      LOG.debug("workflow not found for backfill {}, halt it.", backfill);
      storeBackfill(backfill.builder().halted(true).build());
      return;
    }

    final Workflow workflow = workflowOpt.get();

    // this is best effort because querying active states is not strongly consistent so the
    // initial remaining capacity may already be wrong
    final int remainingCapacity =
        backfill.concurrency() - stateManager.getActiveStatesByTriggerId(backfill.id()).size();

    if (remainingCapacity < 1) {
      LOG.debug("No capacity left for backfill {}", backfill);
      return;
    }

    final Instant initialNextTrigger = backfill.nextTrigger();
    while (true) {
      try {
        var shouldContinue = storage.runInTransactionWithRetries(tx ->
            triggerNextPartitionAndProgress(tx, backfill.id(), workflow,
                initialNextTrigger, remainingCapacity, backfill.reverse()));
        if (!shouldContinue) {
          break;
        }
      } catch (IOException e) {
        // if progressing the backfill fails, yield
        LOG.debug("Failure while trying to progress backfill {}", backfill, e);
        break;
      }
    }
  }

  @VisibleForTesting
  boolean triggerNextPartitionAndProgress(StorageTransaction tx,
                                          String id,
                                          Workflow workflow,
                                          Instant initialNextTrigger,
                                          int remainingCapacity,
                                          boolean reversed) throws IOException {
    final Backfill backfill = tx.backfill(id).orElseThrow(() ->
        new RuntimeException("Error while fetching backfill " + id));

    if (backfill.halted()) {
      LOG.debug("Backfill {} halted", backfill);
      return false;
    }

    final Instant nextTrigger = backfill.nextTrigger();

    if (capacityReached(backfill, initialNextTrigger, nextTrigger,
        remainingCapacity, reversed)) {
      LOG.debug("Capacity reached for backfill {}", backfill);
      return false;
    }

    if (isAllTriggered(backfill, nextTrigger)) {
      LOG.debug("Backfill {} all triggered", backfill);
      tx.store(backfill.builder()
          .allTriggered(true)
          .build());
      return false;
    }

    try {
      // race conditions:
      //   - the other scheduler instance has triggered partition, but hasn't committed the
      //     transaction, and the workflow instance has finished very quickly
      //   - this scheduler reads initialNextTrigger before the other scheduler commits the
      //     transaction, so it will move on to trigger the same partition again, but we still
      //     don't violate concurrency limit in this case
      final TriggerParameters parameters = backfill.triggerParameters().orElse(TriggerParameters.zero());
      triggerListener.event(workflow, Trigger.backfill(backfill.id()), nextTrigger, parameters);
    } catch (Exception e) {
      if (findCause(e, AlreadyInitializedException.class) != null) {
        // just encountered an ad-hoc trigger or it has been triggered by another scheduler instance
        // do not propagate the exception but instead letting transaction decide what to do
        LOG.debug("{} already triggered for backfill {}", nextTrigger, backfill, e);
      } else {
        // can be interrupted, we give up this backfill and retry in next tick
        LOG.debug("Failed to trigger {} for backfill {}", nextTrigger, backfill, e);
        throw new RuntimeException(e);
      }
    }

    final Instant nextPartition = getNextPartition(backfill, nextTrigger, reversed);
    tx.store(backfill.builder()
                      .nextTrigger(nextPartition)
                      .build());

    if (isAllTriggered(backfill, nextPartition)) {
      tx.store(backfill.builder()
                   .nextTrigger(backfill.reverse()
                                ? previousInstant(backfill.start(), backfill.schedule())
                                : backfill.end())
                           .allTriggered(true)
                           .build());
      LOG.debug("Backfill {} all triggered", backfill);
      return false;
    }
    return true;
  }

  private boolean isAllTriggered(Backfill backfill, Instant nextTrigger) {
    return nextTrigger.equals(exclusiveEndTrigger(backfill));
  }

  private Instant exclusiveEndTrigger(Backfill backfill) {
    if (backfill.reverse()) {
      return previousInstant(backfill.start(), backfill.schedule());
    } else {
      return backfill.end();
    }
  }

  private void storeBackfill(Backfill backfill) {
    try {
      storage.storeBackfill(backfill);
    } catch (IOException e) {
      LOG.warn("Failed to store updated backfill {}", backfill.id(), e);
    }
  }

  private static Instant getNextPartition(Backfill backfill,
                                   Instant nextTrigger,
                                   boolean reversed) {
    if (reversed) {
      return previousInstant(nextTrigger, backfill.schedule());
    } else {
      return nextInstant(nextTrigger, backfill.schedule());
    }
  }

  private static boolean capacityReached(Backfill backfill, Instant initialNextTrigger,
                                  Instant nextTrigger, int remainingCapacity,
                                  boolean reversed) {
    if (reversed) {
      return instantsInReversedRange(initialNextTrigger, nextTrigger,
          backfill.schedule()).size() >= remainingCapacity;
    } else {
      return instantsInRange(initialNextTrigger, nextTrigger,
          backfill.schedule()).size() >= remainingCapacity;
    }
  }
}
