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
import javaslang.control.Try;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Triggers backfill executions for {@link Workflow}s.
 */
class BackfillTriggerManager {

  private static final Logger LOG = LoggerFactory.getLogger(BackfillTriggerManager.class);

  private static final String TICK_TYPE = UPPER_CAMEL.to(
      LOWER_UNDERSCORE, BackfillTriggerManager.class.getSimpleName());

  /**
   * The maximum number of instances to trigger for a single backfill in a tick.
   */
  private static final int MAX_INSTANCES_TICK_TRIGGER = 10;

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
        .startSpanAndRun(() -> Try.run(() -> triggerAndProgress0(backfill)).get());
  }

  private void triggerAndProgress0(Backfill backfill) throws IOException {

    // Check how many instances for this backfill are currently active
    // Note: getActiveStatesByTriggerId is eventually consistent and may not return all triggered instances.
    var alreadyActiveInstances = stateManager.getActiveStatesByTriggerId(backfill.id()).size();

    // Look up the workflow. Halts the backfill if the workflow does not exist.
    var workflowOpt = readBackfillWorkflowOrHalt(backfill.id());
    if (workflowOpt.isEmpty()) {
      return;
    }
    var workflow = workflowOpt.orElseThrow();

    // Trigger a limited number of instances. Limit here to avoid starvation (if this is a big backfill)
    // and to limit the risk of alreadyActiveInstances going stale (because of concurrent triggering) while
    // triggering instances.
    for (int i = 0; i < MAX_INSTANCES_TICK_TRIGGER; i++) {
      // The total number of triggered instances, including the ones that were already active
      var activeInstances = alreadyActiveInstances + i;

      // Attempt to trigger an instance
      var shouldContinue = storage.runInTransactionWithRetries(tx ->
          triggerNextInstanceAndProgress(tx, backfill.id(), workflow, activeInstances));

      if (!shouldContinue) {
        break;
      }
    }
  }

  private Optional<Workflow> readBackfillWorkflowOrHalt(String backfillId) throws IOException {
    return storage.runInTransactionWithRetries(tx -> {

      // Read the backfill workflow ID in this transaction to ensure consistency
      var backfillOpt = tx.backfill(backfillId);
      if (backfillOpt.isEmpty()) {
        LOG.debug("backfill not found: {}", backfillId);
        return Optional.empty();
      }
      var backfill = backfillOpt.orElseThrow();

      var workflowOpt = tx.workflow(backfill.workflowId());
      if (workflowOpt.isEmpty()) {
        LOG.debug("workflow not found for backfill, halting: {}", backfill);
        tx.store(backfill.builder().halted(true).build());
      }

      return workflowOpt;
    });
  }

  @VisibleForTesting
  boolean triggerNextInstanceAndProgress(StorageTransaction tx, String backfillId, Workflow workflow,
                                         int triggeredInstances) throws IOException {

    // Re-read backfill for each trigger to provide stronger consistency in case of concurrent updates.
    var backfillOpt = tx.backfill(backfillId);
    if (backfillOpt.isEmpty()) {
      return false;
    }
    var backfill = backfillOpt.orElseThrow();

    // Do not trigger if the backfill was halted
    if (backfill.halted()) {
      LOG.debug("Backfill halted: {}", backfill);
      return false;
    }

    // Do not trigger if the concurrency limit has been reached.
    if (triggeredInstances >= backfill.concurrency()) {
      LOG.debug("Capacity reached for backfill: {}", backfill);
      return false;
    }

    // The instant of the instance to trigger
    final Instant nextTrigger = backfill.nextTrigger();

    // Do not trigger if the backfill is already completely triggered
    if (isAllTriggered(backfill, nextTrigger)) {
      LOG.debug("Backfill already completely triggered: {}", backfill);
      // Mark the backfill as completed if not already marked
      if (!backfill.allTriggered()) {
        tx.store(backfill.builder().allTriggered(true).build());
      }
      return false;
    }

    try {
      // TODO: trigger instance using this transaction to avoid races
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

    // Move the backfill triggering "cursor" forward to the next instance
    var nextTriggerInstant = nextTriggerInstant(backfill, nextTrigger);
    var nextBackfill = backfill.builder()
        .nextTrigger(nextTriggerInstant)
        .allTriggered(isAllTriggered(backfill, nextTriggerInstant))
        .build();

    tx.store(nextBackfill);

    if (nextBackfill.allTriggered()) {
      LOG.debug("Backfill all triggered: {}", backfill.id());
    }

    return !nextBackfill.allTriggered();
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

  private static Instant nextTriggerInstant(Backfill backfill,
                                            Instant trigger) {
    if (backfill.reverse()) {
      return previousInstant(trigger, backfill.schedule());
    } else {
      return nextInstant(trigger, backfill.schedule());
    }
  }
}
