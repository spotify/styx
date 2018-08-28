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

import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.CaseFormat.UPPER_CAMEL;
import static com.spotify.styx.util.CloserUtil.register;
import static com.spotify.styx.util.ExceptionUtil.findCause;
import static com.spotify.styx.util.GuardedRunnable.guard;
import static com.spotify.styx.util.ParameterUtil.toParameter;
import static com.spotify.styx.util.TimeUtil.nextInstant;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerInstantSpec;
import io.grpc.Context;
import io.opencensus.common.Scope;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import java.io.Closeable;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Triggers natural executions for {@link Workflow}s.
 */
class TriggerManager implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(TriggerManager.class);

  private static final String TICK_TYPE = UPPER_CAMEL.to(LOWER_UNDERSCORE,
                                                         TriggerManager.class.getSimpleName());
  private static final int TRIGGER_CONCURRENCY = 32;

  private static final Tracer tracer = Tracing.getTracer();

  private final Closer closer = Closer.create();

  private final TriggerListener triggerListener;
  private final Time time;
  private final Storage storage;
  private final Stats stats;
  private final Executor executor;

  TriggerManager(TriggerListener triggerListener,
                 Time time,
                 Storage storage,
                 Stats stats) {
    this.triggerListener = requireNonNull(triggerListener);
    this.time = requireNonNull(time);
    this.storage = requireNonNull(storage);
    this.stats = requireNonNull(stats);
    final ForkJoinPool forkJoinPool = register(closer, new ForkJoinPool(TRIGGER_CONCURRENCY), "trigger-manager");
    this.executor = Context.currentContextExecutor(forkJoinPool);
  }

  void tick() {
    try (Scope ss = tracer.spanBuilder("Styx.TriggerManager.tick")
        .setRecordEvents(true)
        .setSampler(Samplers.alwaysSample())
        .startScopedSpan()) {
      tick0();
    }
  }

  void tick0() {
    final Instant t0 = time.get();

    try {
      if (!storage.config().globalEnabled()) {
        LOG.info("Triggering has been disabled globally.");
        return;
      }
    } catch (IOException e) {
      LOG.warn("Couldn't fetch global enabled status, skipping this run", e);
      return;
    }

    final Map<Workflow, TriggerInstantSpec> canBeTriggeredWorkflows;
    final Set<WorkflowId> enabledWorkflows;
    try {
      canBeTriggeredWorkflows = storage.workflowsWithNextNaturalTrigger();
      enabledWorkflows = storage.enabled();
    } catch (IOException e) {
      LOG.warn("Couldn't fetch workflows to trigger, skipping this run", e);
      return;
    }

    final Instant now = time.get();
    canBeTriggeredWorkflows.entrySet().stream()
        .filter(entry -> now.isAfter(entry.getValue().offsetInstant()))
        .map(entry -> CompletableFuture.runAsync(
            tryTriggering(entry.getKey(), entry.getValue(), enabledWorkflows), executor))
        .collect(toList()) // collect here to trigger in parallel
        .forEach(CompletableFuture::join);

    final long durationMillis = t0.until(time.get(), ChronoUnit.MILLIS);
    stats.recordTickDuration(TICK_TYPE, durationMillis);
  }

  private Runnable tryTriggering(Workflow workflow,
                             TriggerInstantSpec instantSpec,
                             Set<WorkflowId> enabledWorkflows) {
    return guard(() -> {
      if (enabledWorkflows.contains(workflow.id())) {
        try {
          final CompletionStage<Void> processed = triggerListener.event(
              workflow,
              Trigger.natural(),
              instantSpec.instant(),
              TriggerParameters.zero());
          // Wait for the event to be processed before proceeding to the next trigger
          processed.toCompletableFuture().get();
        } catch (Exception e) {
          final WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(),
              toParameter(workflow.configuration().schedule(), instantSpec.instant()));

          if (findCause(e, AlreadyInitializedException.class) != null) {
            LOG.debug("{} already triggered", workflowInstance, e);
            // move on to update next natural trigger
          } else {
            LOG.debug("Failed to trigger {}", workflowInstance, e);
            return;
          }
        }

        stats.recordNaturalTrigger();
      }

      final Schedule schedule = workflow.configuration().schedule();
      final Instant nextTrigger = nextInstant(instantSpec.instant(), schedule);
      final Instant nextWithOffset = workflow.configuration().subtractOffset(nextTrigger);
      final TriggerInstantSpec nextSpec = TriggerInstantSpec.create(nextWithOffset, nextTrigger);

      try {
        storage.updateNextNaturalTrigger(workflow.id(), nextSpec);
      } catch (IOException e) {
        LOG.error(
            "Sent trigger for workflow {}, but didn't succeed storing next scheduled run {}.",
            workflow.id(), nextTrigger);
        throw Throwables.propagate(e);
      }
    });
  }

  @Override
  public void close() throws IOException {
    closer.close();
  }
}
