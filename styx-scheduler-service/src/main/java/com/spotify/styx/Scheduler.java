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
import static com.spotify.styx.state.StateUtil.hasTimedOut;
import static com.spotify.styx.state.StateUtil.workflowResources;
import static com.spotify.styx.storage.Storage.GLOBAL_RESOURCE_ID;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicLongMap;
import com.google.common.util.concurrent.RateLimiter;
import com.spotify.futures.CompletableFutures;
import com.spotify.styx.WorkflowExecutionGate.ExecutionBlocker;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StaleEventException;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.TimeoutConfig;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.ShardedCounter;
import com.spotify.styx.util.Time;
import io.grpc.Context;
import io.norberg.automatter.AutoMatter;
import io.opencensus.common.Scope;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class is responsible for making decisions on how to make further progress on states in
 * the {@link StateManager}. The general operation is such that each time the {@link #tick()}
 * method is called, the scheduler will inspect all the active states in {@link StateManager} and
 * determine if any of them should receive new events.
 *
 * <p>Currently the scheduler only cares about states that are in the {@link State#QUEUED} state or
 * have timed out according to the {@link TimeoutConfig}).
 *
 * <p>For all Queued states that are eligible for execution, the scheduler will determine which
 * ones to dequeue, while ensuring that the {@link Resource}s associated with each respective
 * {@link Workflow} is not exceeded. It will try to dequeue workflow instances fairly by globally
 * randomizing the dequeue order on each {@link #tick()}.
 */
public class Scheduler {

  private static final Logger LOG = LoggerFactory.getLogger(Scheduler.class);

  private static final String TICK_TYPE = UPPER_CAMEL.to(LOWER_UNDERSCORE,
      Scheduler.class.getSimpleName());

  private static final Tracer tracer = Tracing.getTracer();

  private final Time time;
  private final TimeoutConfig ttls;
  private final StateManager stateManager;
  private final Storage storage;
  private final WorkflowResourceDecorator resourceDecorator;
  private final Stats stats;
  private final RateLimiter dequeueRateLimiter;
  private final WorkflowExecutionGate gate;
  private final ShardedCounter shardedCounter;
  private final Executor executor;
  private final List<OutputHandler> outputHandlers;

  public Scheduler(Time time, TimeoutConfig ttls, StateManager stateManager, Storage storage,
      WorkflowResourceDecorator resourceDecorator, Stats stats, RateLimiter dequeueRateLimiter,
      WorkflowExecutionGate gate, ShardedCounter shardedCounter, Executor executor,
      List<OutputHandler> outputHandlers) {
    this.time = Objects.requireNonNull(time);
    this.ttls = Objects.requireNonNull(ttls);
    this.stateManager = Objects.requireNonNull(stateManager);
    this.storage = Objects.requireNonNull(storage);
    this.resourceDecorator = Objects.requireNonNull(resourceDecorator);
    this.stats = Objects.requireNonNull(stats);
    this.dequeueRateLimiter = Objects.requireNonNull(dequeueRateLimiter, "dequeueRateLimiter");
    this.gate = Objects.requireNonNull(gate, "gate");
    this.shardedCounter = Objects.requireNonNull(shardedCounter, "shardedCounter");
    this.executor = Context.currentContextExecutor(Objects.requireNonNull(executor, "executor"));
    this.outputHandlers = Objects.requireNonNull(outputHandlers, "outputHandlers");
  }

  void tick() {
    try (Scope ss = tracer.spanBuilder("Styx.Scheduler.tick")
        .setRecordEvents(true)
        .setSampler(Samplers.alwaysSample())
        .startScopedSpan()) {
      tick0();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void tick0() throws IOException {
    var t0 = time.get();

    var config = storage.config();
    var activeStates = stateManager.listActiveStates();
    var activeWorkflows = activeStates.stream()
        .map(WorkflowInstance::workflowId)
        .collect(toSet());

    var ctx = new SchedulingContextBuilder()
        .config(config)
        .resources(resources(config))
        .workflows(storage.workflows(activeWorkflows))
        .currentResourceDemand(AtomicLongMap.create())
        .instances(activeStates)
        .build();

    var message = String.format("Instances: active=%d", activeStates.size());
    LOG.info(message);
    tracer.getCurrentSpan().addAnnotation(message);

    processExecutingInstances(ctx);

    // Note: not a strongly consistent number, so the graphed value can be imprecise or show
    // exceeded limit even if the real usage never exceeded the limit.
    updateResourceStats(ctx.resources(), ctx.currentResourceUsage());

    ctx.currentResourceDemand().asMap().forEach(stats::recordResourceDemanded);

    final long durationMillis = t0.until(time.get(), ChronoUnit.MILLIS);
    stats.recordTickDuration(TICK_TYPE, durationMillis);
  }

  private Map<String, Resource> resources(StyxConfig config) throws IOException {
    return Stream.concat(
        config.globalConcurrency().stream().map(concurrency -> Resource.create(GLOBAL_RESOURCE_ID, concurrency)),
        storage.resources().stream()
    ).collect(toMap(Resource::id, identity()));
  }

  private void processExecutingInstances(SchedulingContext ctx) {
    var futures = ctx.instances().stream()
        .map(instance -> CompletableFuture.runAsync(() -> processExecutingInstance(ctx, instance), executor))
        .collect(toList());
    try {
      CompletableFutures.allAsList(futures).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Run the instance state machine forward until it stops.
   */
  private void processExecutingInstance(SchedulingContext ctx, WorkflowInstance instance) {
    var runStateOpt = runState(instance);
    if (runStateOpt.isEmpty()) {
      return;
    }
    var runState = runStateOpt.orElseThrow();
    while (true) {
      var event = findTransition(ctx, runState);
      if (event.isEmpty()) {
        instanceResourceRefs(ctx, runState)
            .forEach(ctx.currentResourceUsage()::incrementAndGet);
        return;
      }
      try {
        runState = stateManager.receive(event.get(), runState.counter()).toCompletableFuture()
            .get(30, TimeUnit.SECONDS);
      } catch (InterruptedException | TimeoutException e) {
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        if (e.getCause() instanceof StaleEventException) {
          // We lost a race and someone else made a state transition, back out
          return;
        }
        throw new RuntimeException(e);
      } catch (IsClosedException e) {
        return;
      }
    }
  }

  private Optional<RunState> runState(WorkflowInstance instance) {
    Optional<RunState> runState;
    try {
      runState = storage.readActiveState(instance);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return runState;
  }

  private Optional<Event> findTransition(SchedulingContext ctx, RunState state) {
    // TODO: have handlers register their state interests and avoid having to loop through all of them
    // TODO: we really want just one event here.

    var workflowOpt = Optional.ofNullable(
        ctx.workflows().get(state.workflowInstance().workflowId()));

    // TODO: Timeout should just be a state handler?
    if (hasTimedOut(workflowOpt, state, time.get(), ttls.ttlOf(state.state()))) {
      return Optional.of(getTimeout(state));
    }

    // TODO: Dequeueing should just be a state handler instead?
    if (state.state() == State.QUEUED) {
      if (isDueForDequeue(state)) {
        return dequeueInstance(ctx, state);
      } else {
        return Optional.empty();
      }
    }

    var events = outputHandlers.stream()
        .flatMap(h -> h.transitionInto(state).stream())
        .collect(toList());
    if (events.size() > 1) {
      LOG.warn("Got more than one event, discarding");
    }
    return events.stream().findFirst();
  }

  private void updateResourceStats(Map<String, Resource> resources,
                                   AtomicLongMap<String> currentResourceUsage) {
    resources.values().forEach(r -> stats.recordResourceConfigured(r.id(), r.concurrency()));
    currentResourceUsage.asMap().forEach(stats::recordResourceUsed);
    Sets.difference(resources.keySet(), currentResourceUsage.asMap().keySet())
        .forEach(r -> stats.recordResourceUsed(r, 0));
  }

  private Optional<Event> dequeueInstance(SchedulingContext ctx, RunState state) {
    try {
      return tracer.spanBuilder("dequeueInstance").startSpanAndCall(() ->
          dequeueInstance0(ctx, state));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private Optional<Event> dequeueInstance0(SchedulingContext ctx, RunState state) {

    LOG.debug("Evaluating instance for dequeue: {}", state.workflowInstance());

    var instanceResourceRefs = instanceResourceRefs(ctx, state);

    var unknownResources = instanceResourceRefs.stream()
        .filter(resourceRef -> !ctx.resources().containsKey(resourceRef))
        .collect(toSet());

    if (!unknownResources.isEmpty()) {
      return Optional.of(Event.runError(state.workflowInstance(),
          String.format("Referenced resources not found: %s", unknownResources)));
    }

    instanceResourceRefs.forEach(ctx.currentResourceDemand()::incrementAndGet);

    // Check resource limits. This is racy and can give false positives but the transactional
    // checking happens later. This is just intended to avoid spinning on exhausted resources.
    final List<String> depletedResources = instanceResourceRefs.stream()
        .filter(resourceId -> limitReached(resourceId, ctx.resourceExhaustedCache()))
        .sorted()
        .collect(toList());
    if (!depletedResources.isEmpty()) {
      LOG.debug("Resource limit reached for instance, not dequeueing: {}: exhausted resources={}",
          state.workflowInstance(), depletedResources);
      MessageUtil.emitResourceLimitReachedMessage(stateManager, state, depletedResources);
      return Optional.empty();
    }

    // Check for execution blocker
    if (ctx.config().executionGatingEnabled()) {
      Optional<ExecutionBlocker> blocker = Optional.empty();
      try {
        blocker = gate.executionBlocker(state.workflowInstance())
            .toCompletableFuture().get(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException | TimeoutException e) {
        LOG.warn("Failed to check execution blocker for {}, assuming there is no blocker",
            state.workflowInstance(), e);
      }

      if (blocker.isPresent()) {
        LOG.debug("Dequeue rescheduled: {}: {}", state.workflowInstance(), blocker.get());
        return Optional.of(Event.retryAfter(state.workflowInstance(), blocker.get().delay().toMillis()));
      }
    }

    double sleepingTimeSeconds = dequeueRateLimiter.acquire();
    if (sleepingTimeSeconds > 0.0001) {
      final double sleepingTimeMillis = sleepingTimeSeconds * 1000;
      final String message = "Dequeue rate limited and slept for " + sleepingTimeMillis + " ms";
      LOG.debug(message, sleepingTimeMillis);
      tracer.getCurrentSpan().addAnnotation(message);
    }

    // Racy: some resources may have been removed (become unknown) by now; in that case the
    // counters code during dequeue will treat them as unlimited...
    return Optional.of(getDequeue(state, instanceResourceRefs));
  }

  private Set<String> instanceResourceRefs(SchedulingContext ctx, RunState state) {
    var workflowOpt = Optional.ofNullable(
        ctx.workflows().get(state.workflowInstance().workflowId()));

    var workflowResourceRefs = workflowResources(ctx.config().globalConcurrency().isPresent(), workflowOpt);

    return workflowOpt
        .map(workflow -> resourceDecorator.decorateResources(
            state, workflow.configuration(), workflowResourceRefs))
        .orElse(workflowResourceRefs);
  }

  private boolean limitReached(final String resourceId, ConcurrentMap<String, Boolean> resourceExhaustedCache) {
    return resourceExhaustedCache.computeIfAbsent(resourceId, k -> {
      try {
        return !shardedCounter.counterHasSpareCapacity(resourceId);
      } catch (RuntimeException | IOException e) {
        LOG.warn("Failed to check resource counter limit", e);
        return false;
      }
    });
  }

  private boolean isDueForDequeue(RunState runState) {
    final Instant now = time.get();
    final Instant deadline = Instant
        .ofEpochMilli(runState.timestamp())
        .plusMillis(runState.data().retryDelayMillis().orElse(0L));

    return !deadline.isAfter(now);
  }

  private Event getDequeue(RunState state, Set<String> resourceIds) {
    final WorkflowInstance workflowInstance = state.workflowInstance();

    if (state.data().tries() == 0) {
      LOG.info("Executing {}", workflowInstance);
    } else {
      LOG.info("Executing {}, retry #{}", workflowInstance, state.data().tries());
    }
    return Event.dequeue(workflowInstance, resourceIds);
  }

  private Event getTimeout(RunState runState) {
    LOG.info("Found stale state {} since {} for workflow {}; Issuing a timeout",
        runState.state(), Instant.ofEpochMilli(runState.timestamp()), runState.workflowInstance());
    return Event.timeout(runState.workflowInstance());
  }

  @AutoMatter
  // TODO: TickContext
  interface SchedulingContext {

    StyxConfig config();

    Map<String, Resource> resources();

    Map<WorkflowId, Workflow> workflows();

    List<WorkflowInstance> instances();

    AtomicLongMap<String> currentResourceDemand();

    AtomicLongMap<String> currentResourceUsage();

    ConcurrentMap<String, Boolean> resourceExhaustedCache();
  }
}
