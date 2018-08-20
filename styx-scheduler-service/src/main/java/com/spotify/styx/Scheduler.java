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
import static com.spotify.styx.state.StateUtil.getActiveInstanceStates;
import static com.spotify.styx.state.StateUtil.getResourceUsage;
import static com.spotify.styx.state.StateUtil.getTimedOutInstances;
import static com.spotify.styx.state.StateUtil.workflowResources;
import static com.spotify.styx.storage.Storage.GLOBAL_RESOURCE_ID;
import static java.util.Collections.emptySet;
import static java.util.Comparator.comparingLong;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.RateLimiter;
import com.spotify.styx.WorkflowExecutionGate.ExecutionBlocker;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.InstanceState;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.TimeoutConfig;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.ShardedCounter;
import com.spotify.styx.util.Time;
import io.grpc.Context;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
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

  public Scheduler(Time time, TimeoutConfig ttls, StateManager stateManager, Storage storage,
      WorkflowResourceDecorator resourceDecorator, Stats stats, RateLimiter dequeueRateLimiter,
      WorkflowExecutionGate gate, ShardedCounter shardedCounter, Executor executor) {
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
  }

  void tick() {
    try (Scope ss = tracer.spanBuilder("Styx.Scheduler.tick")
        .setRecordEvents(true)
        .setSampler(Samplers.alwaysSample())
        .startScopedSpan()) {
      tick0();
    }
  }

  private void tick0() {
    final Instant t0 = time.get();

    final Map<String, Resource> resources;
    final Optional<Long> globalConcurrency;
    final StyxConfig config;
    try {
      resources = storage.resources().stream().collect(toMap(Resource::id, identity()));
      config = storage.config();
      globalConcurrency = config.globalConcurrency();
    } catch (IOException e) {
      LOG.warn("Failed to get resource limits", e);
      return;
    }

    globalConcurrency.ifPresent(
        concurrency ->
            resources.put(GLOBAL_RESOURCE_ID,
                Resource.create(GLOBAL_RESOURCE_ID, concurrency)));

    // TODO: have a separate less-frequent tick move instances from cold to hot queue
    final List<WorkflowInstance> activeInstances = storage.readActiveWorkflowInstanceQueue(HOT);
    final Map<WorkflowInstance, RunState> activeStatesMap = storage.readActiveStates(activeInstances);
    final List<InstanceState> activeStates = getActiveInstanceStates(activeStatesMap);

    // TODO: handle timed out instances on a separate less-frequent tick
    final Set<WorkflowInstance> timedOutInstances = getTimedOutInstances(activeStates, time.get(), ttls);

    final Map<WorkflowId, Workflow> workflows = getWorkflows(activeStates);

    final Map<WorkflowId, Set<String>> workflowResourceReferences =
        activeStates.parallelStream()
            .map(InstanceState::workflowInstance)
            .map(WorkflowInstance::workflowId)
            .distinct()
            .collect(toMap(
                workflowId -> workflowId,
                workflowId -> workflowResources(globalConcurrency.isPresent(),
                    Optional.ofNullable(workflows.get(workflowId)))));

    // Note: not a strongly consistent number, so the graphed value can be imprecise or show
    // exceeded limit even if the real usage never exceeded the limit.
    final Map<String, Long> currentResourceUsage =
        getResourceUsage(globalConcurrency.isPresent(), activeStates, timedOutInstances,
            resourceDecorator, workflows);

    // this reflects resource usage since last tick, so a couple of minutes delay
    updateResourceStats(resources, currentResourceUsage);

    final List<InstanceState> eligibleInstances =
        activeStates.parallelStream()
            .filter(entry -> !timedOutInstances.contains(entry.workflowInstance()))
            .filter(entry -> shouldExecute(entry.runState()))
            .sorted(comparingLong(i -> i.runState().timestamp()))
            .collect(toList());

    timedOutInstances.forEach(wfi -> this.sendTimeout(wfi, activeStatesMap.get(wfi)));

    dequeueInstances(config, resources, workflowResourceReferences,
        workflows, eligibleInstances);

    final long durationMillis = t0.until(time.get(), ChronoUnit.MILLIS);
    stats.recordTickDuration(TICK_TYPE, durationMillis);
  }

  private void updateResourceStats(Map<String, Resource> resources,
                                   Map<String, Long> currentResourceUsage) {
    resources.values().forEach(r -> stats.recordResourceConfigured(r.id(), r.concurrency()));
    currentResourceUsage.forEach(stats::recordResourceUsed);
    Sets.difference(resources.keySet(), currentResourceUsage.keySet())
        .forEach(r -> stats.recordResourceUsed(r, 0));
  }

  private Map<WorkflowId, Workflow> getWorkflows(final List<InstanceState> activeStates) {
    final Set<WorkflowId> workflowIds = activeStates.stream()
        .map(activeState -> activeState.workflowInstance().workflowId())
        .collect(toSet());
    return storage.workflows(workflowIds);
  }

  private void dequeueInstances(
      StyxConfig config,
      Map<String, Resource> resources,
      Map<WorkflowId, Set<String>> workflowResourceReferences,
      Map<WorkflowId, Workflow> workflows,
      List<InstanceState> eligibleInstances) {

    final Map<WorkflowInstance, CompletableFuture<Void>> futures = eligibleInstances.stream()
        .collect(toMap(
            InstanceState::workflowInstance,
            instanceState -> CompletableFuture.runAsync(() ->
                dequeueInstance(config, resources, workflowResourceReferences,
                    Optional.ofNullable(workflows.get(instanceState.workflowInstance().workflowId())),
                    instanceState), executor)));

    futures.forEach((instance, future) -> {
      try {
        future.get();
      } catch (InterruptedException e) {
        LOG.warn("Interrupted", e);
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException e) {
        LOG.error("Failed to process instance for dequeue: " + instance, e);
      }
    });
  }

  private void dequeueInstance(StyxConfig config, Map<String, Resource> resources,
      Map<WorkflowId, Set<String>> workflowResourceReferences,
      Optional<Workflow> workflowOpt, InstanceState instanceState) {
    tracer.spanBuilder("dequeueInstance").startSpanAndRun(() ->
        dequeueInstance0(config, resources, workflowResourceReferences, workflowOpt, instanceState));
  }

  private void dequeueInstance0(final StyxConfig config, Map<String, Resource> resources,
      Map<WorkflowId, Set<String>> workflowResourceReferences,
      Optional<Workflow> workflowOpt,
      InstanceState instanceState) {

    final Set<String> workflowResourceRefs =
        workflowResourceReferences.getOrDefault(instanceState.workflowInstance().workflowId(), emptySet());

    final Set<String> instanceResourceRefs = workflowOpt
            .map(workflow -> resourceDecorator.decorateResources(
                instanceState.runState(), workflow.configuration(), workflowResourceRefs))
            .orElse(workflowResourceRefs);

    final Set<String> unknownResources = instanceResourceRefs.stream()
        .filter(resourceRef -> !resources.containsKey(resourceRef))
        .collect(toSet());

    if (!unknownResources.isEmpty()) {
      stateManager.receiveIgnoreClosed(
          Event.runError(instanceState.workflowInstance(),
              String.format("Referenced resources not found: %s", unknownResources)),
          instanceState.runState().counter());
      return;
    }

    // Check resource limits. This is racy and can give false positives but the transactional
    // checking happens later. This is just intended to avoid spinning on exhausted resources.
    final List<String> depletedResources = instanceResourceRefs.stream()
        .filter(this::limitReached)
        .sorted()
        .collect(toList());
    if (!depletedResources.isEmpty()) {
      LOG.debug("Resource limit reached for instance, not dequeueing: {}: exhausted resources={}",
          instanceState.workflowInstance(), depletedResources);
      final RunState runState = instanceState.runState();
      stateManager.receiveIgnoreClosed(
          Event.retryAfter(runState.workflowInstance(), TimeUnit.MINUTES.toMillis(5)), runState.counter());
      final Message message = Message.info(
          String.format("Resource limit reached for: %s", depletedResources));
      if (!runState.data().message().map(message::equals).orElse(false)) {
        stateManager.receiveIgnoreClosed(Event.info(runState.workflowInstance(), message), runState.counter() + 1);
      }
      return;
    }

    // Check for execution blocker
    if (config.executionGatingEnabled()) {
      Optional<ExecutionBlocker> blocker = Optional.empty();
      try {
        blocker = gate.executionBlocker(instanceState.workflowInstance())
            .toCompletableFuture().get(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      } catch (ExecutionException | TimeoutException e) {
        LOG.warn("Failed to check execution blocker for {}, assuming there is no blocker",
            instanceState.workflowInstance(), e);
      }

      if (blocker.isPresent()) {
        stateManager.receiveIgnoreClosed(Event.retryAfter(instanceState.workflowInstance(),
            blocker.get().delay().toMillis()),
            instanceState.runState().counter());
        LOG.debug("Dequeue rescheduled: {}: {}", instanceState.workflowInstance(), blocker.get());
        return;
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
    sendDequeue(instanceState, instanceResourceRefs);
  }

  private boolean limitReached(final String resourceId) {
    try {
      return !shardedCounter.counterHasSpareCapacity(resourceId);
    } catch (RuntimeException e) {
      LOG.warn("Failed to check resource counter limit", e);
      return false;
    }
  }

  private boolean shouldExecute(RunState runState) {
    if (runState.state() != State.QUEUED) {
      return false;
    }

    final Instant now = time.get();
    final Instant deadline = Instant
        .ofEpochMilli(runState.timestamp())
        .plusMillis(runState.data().retryDelayMillis().orElse(0L));

    return !deadline.isAfter(now);
  }

  private void sendDequeue(InstanceState instanceState, Set<String> resourceIds) {
    final WorkflowInstance workflowInstance = instanceState.workflowInstance();
    final RunState state = instanceState.runState();

    if (state.data().tries() == 0) {
      LOG.info("Executing {}", workflowInstance);
    } else {
      LOG.info("Executing {}, retry #{}", workflowInstance, state.data().tries());
    }
    stateManager.receiveIgnoreClosed(Event.dequeue(workflowInstance, resourceIds),
        instanceState.runState().counter());
  }

  private void sendTimeout(WorkflowInstance workflowInstance, RunState runState) {
    LOG.info("Found stale state {} since {} for workflow {}; Issuing a timeout",
        runState.state(), Instant.ofEpochMilli(runState.timestamp()), workflowInstance);
    stateManager.receiveIgnoreClosed(Event.timeout(workflowInstance), runState.counter());
  }
}
