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
import static com.spotify.styx.state.StateUtil.workflowResources;
import static com.spotify.styx.storage.Storage.GLOBAL_RESOURCE_ID;
import static java.util.Comparator.comparing;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AtomicLongMap;
import com.google.common.util.concurrent.RateLimiter;
import com.spotify.futures.CompletableFutures;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.LimitsResource;
import com.spotify.styx.model.LimitsResourceBuilder;
import com.spotify.styx.model.RequestsResource;
import com.spotify.styx.model.RequestsResourceBuilder;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.StateTransitionConflictException;
import com.spotify.styx.state.StateUtil;
import com.spotify.styx.state.TimeoutConfig;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.CounterCapacityException;
import com.spotify.styx.util.ShardedCounter;
import com.spotify.styx.util.Time;
import io.grpc.Context;
import io.opencensus.common.Scope;
import io.opencensus.trace.AttributeValue;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
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
  private static final long RANDOMIZED_DELAY_BASE = Duration.ofMinutes(5).toMillis();

  private final Time time;
  private final StateManager stateManager;
  private final Storage storage;
  private final WorkflowResourceDecorator resourceDecorator;
  private final Stats stats;
  private final RateLimiter dequeueRateLimiter;
  private final ShardedCounter shardedCounter;
  private final Executor executor;
  private final Logger log;
  private final Shuffler shuffler;

  Scheduler(Time time, StateManager stateManager, Storage storage,
            WorkflowResourceDecorator resourceDecorator, Stats stats, RateLimiter dequeueRateLimiter,
            ShardedCounter shardedCounter, Executor executor) {
    this(time, stateManager, storage, resourceDecorator, stats, dequeueRateLimiter, shardedCounter, executor,
        LoggerFactory.getLogger(Scheduler.class), Shuffler.DEFAULT);
  }

  @VisibleForTesting
  Scheduler(Time time, StateManager stateManager, Storage storage,
            WorkflowResourceDecorator resourceDecorator, Stats stats, RateLimiter dequeueRateLimiter,
            ShardedCounter shardedCounter, Executor executor, Logger log, Shuffler shuffler) {
    this.time = Objects.requireNonNull(time);
    this.stateManager = Objects.requireNonNull(stateManager);
    this.storage = Objects.requireNonNull(storage);
    this.resourceDecorator = Objects.requireNonNull(resourceDecorator);
    this.stats = Objects.requireNonNull(stats);
    this.dequeueRateLimiter = Objects.requireNonNull(dequeueRateLimiter, "dequeueRateLimiter");
    this.shardedCounter = Objects.requireNonNull(shardedCounter, "shardedCounter");
    this.executor = Context.currentContextExecutor(Objects.requireNonNull(executor, "executor"));
    this.log = Objects.requireNonNull(log, "log");
    this.shuffler = Objects.requireNonNull(shuffler, "shuffler");
  }

  void tick() {
    try (Scope ignored = tracer.spanBuilder("Styx.Scheduler.tick")
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
      config = storage.config();
      globalConcurrency = config.globalConcurrency();
      resources = storage.resources().stream().collect(toMap(Resource::id, identity()));
    } catch (IOException e) {
      log.warn("Failed to read from storage", e);
      return;
    }

    final LimitsResource limitsResource = new LimitsResourceBuilder().memory("1Gi").cpu(1D).build();
    final RequestsResource requestsResource = new RequestsResourceBuilder().memory("1Gi").cpu(1D).build();

    globalConcurrency.ifPresent(
        concurrency ->
            resources.put(GLOBAL_RESOURCE_ID,
                Resource.create(GLOBAL_RESOURCE_ID, concurrency,
                    requestsResource, limitsResource))); // FIXME

    var activeInstances = stateManager.listActiveInstances();
    var workflows = new ConcurrentHashMap<WorkflowId, Optional<Workflow>>();

    // Note: not a strongly consistent number, so the graphed value can be imprecise or show
    // exceeded limit even if the real usage never exceeded the limit.
    var currentResourceUsage = AtomicLongMap.<String>create();
    var currentResourceDemand = AtomicLongMap.<String>create();

    processInstances(config, resources, workflows, activeInstances, currentResourceUsage, currentResourceDemand);

    // TODO: stats might be inaccurate if some instances fail processing
    updateResourceStats(resources, currentResourceUsage);
    currentResourceDemand.asMap().forEach(stats::recordResourceDemanded);

    final long durationMillis = t0.until(time.get(), ChronoUnit.MILLIS);
    stats.recordTickDuration(TICK_TYPE, durationMillis);

    tracer.getCurrentSpan().addAnnotation("processed",
        Map.of("instances", AttributeValue.longAttributeValue(activeInstances.size())));
  }

  private void updateResourceStats(Map<String, Resource> resources,
                                   AtomicLongMap<String> currentResourceUsage) {
    resources.values().forEach(r -> stats.recordResourceConfigured(r.id(), r.concurrency()));
    currentResourceUsage.asMap().forEach(stats::recordResourceUsed);
    Sets.difference(resources.keySet(), currentResourceUsage.asMap().keySet())
        .forEach(r -> stats.recordResourceUsed(r, 0));
  }

  private void processInstances(StyxConfig config, Map<String, Resource> resources,
                                ConcurrentHashMap<WorkflowId, Optional<Workflow>> workflows,
                                Set<WorkflowInstance> activeInstances,
                                AtomicLongMap<String> currentResourceUsage,
                                AtomicLongMap<String> currentResourceDemand) {

    if (!config.globalEnabled()) {
      LOG.info("Scheduling has been disabled globally.");
    }

    var resourceExhaustedCache = new ConcurrentHashMap<String, Boolean>();

    // Shuffle the instances in order to process them in random order and reduce contention with other schedulers etc
    var shuffledInstances = shuffleInstances(activeInstances);

    // Process instances in parallel
    var futures = shuffledInstances.stream()
        .map(instance -> CompletableFuture.runAsync(() ->
            // Do not include all instance spans in parent tick span to avoid it growing too big
            tracer.spanBuilderWithExplicitParent("Styx.Scheduler.processInstance", null).startSpanAndRun(() -> {
              try {
                processInstance(config, resources, workflows, instance, resourceExhaustedCache,
                    currentResourceUsage, currentResourceDemand);
              } catch (StateTransitionConflictException e) {
                log.debug("State transition conflict when scheduling instance: {}", instance, e);
              } catch (CounterCapacityException e) {
                log.debug("Counter capacity exhausted when scheduling instance: {}", instance, e);
              } catch (Throwable e) {
                log.warn("Caught exception when scheduling instance: {}", instance, e);
              }
            }), executor))
        .collect(toList());

    // Wait for processing to complete
    CompletableFutures.allAsList(futures).join();
  }

  private void processInstance(StyxConfig config, Map<String, Resource> resources,
                               ConcurrentMap<WorkflowId, Optional<Workflow>> workflows, WorkflowInstance instance,
                               ConcurrentMap<String, Boolean> resourceExhaustedCache,
                               AtomicLongMap<String> currentResourceUsage,
                               AtomicLongMap<String> currentResourceDemand) {

    log.debug("Processing instance: {}", instance);

    // Get the run state or exit if it does not exist
    var runStateOpt = stateManager.getActiveState(instance);
    if (runStateOpt.isEmpty()) {
      return;
    }
    var runState = runStateOpt.orElseThrow();

    // Look up the resources that are used by this workflow
    // Account current resource usage
    if (StateUtil.isConsumingResources(runState.state())) {
      runState.data().resourceIds().ifPresent(ids -> ids.forEach(currentResourceUsage::incrementAndGet));
    }

    // Exit if this instance is not eligible for dequeue
    if (!shouldExecute(runState)) {
      return;
    }

    log.debug("Evaluating instance for dequeue: {}", instance);

    if (!config.globalEnabled()) {
      LOG.debug("Scheduling disabled, sending instance back to queue: {}", instance);
      stateManager.receiveIgnoreClosed(Event.retryAfter(instance, randomizedDelay()), runState.counter());
      return;
    }

    // Get the workflow configuration
    var workflowOpt = workflows.computeIfAbsent(instance.workflowId(), this::readWorkflow);
    var workflowConfig = workflowOpt
        .map(Workflow::configuration)
        // Dummy placeholder
        .orElse(WorkflowConfiguration
            .builder()
            .id(instance.workflowId().id())
            .schedule(Schedule.parse(""))
            .build());

    var workflowResourceRefs = workflowResources(config.globalConcurrency().isPresent(), workflowOpt);
    var instanceResourceRefs = resourceDecorator.decorateResources(
        runState, workflowConfig, workflowResourceRefs);

    var unknownResources = instanceResourceRefs.stream()
        .filter(resourceRef -> !resources.containsKey(resourceRef))
        .collect(toSet());

    if (!unknownResources.isEmpty()) {
      var error = Event.runError(instance, "Referenced resources not found: " + unknownResources);
      stateManager.receiveIgnoreClosed(error, runState.counter());
      return;
    }

    // Account resource demand by instances that are queued
    instanceResourceRefs.forEach(currentResourceDemand::incrementAndGet);

    // Check resource limits. This is racy and can give false positives but the transactional
    // checking happens later. This is just intended to avoid spinning on exhausted resources.
    final List<String> depletedResources = instanceResourceRefs.stream()
        .filter(resourceId -> limitReached(resourceId, resourceExhaustedCache))
        .sorted()
        .collect(toList());
    if (!depletedResources.isEmpty()) {
      log.debug("Resource limit reached for instance, not dequeueing: {}: exhausted resources={}",
          instance, depletedResources);
      MessageUtil.emitResourceLimitReachedMessage(stateManager, runState, depletedResources);
      return;
    }

    // Racy: some resources may have been removed (become unknown) by now; in that case the
    // counters code during dequeue will treat them as unlimited...
    sendDequeue(instance, runState, instanceResourceRefs);
  }

  /**
   * Shuffle instances. For the same workflow ID, older instances will appear early in the list.
   */
  @VisibleForTesting
  List<WorkflowInstance> shuffleInstances(Set<WorkflowInstance> activeInstances) {
    final Map<WorkflowId, SortedSet<WorkflowInstance>> groups = activeInstances
        .stream()
        // Using LinkedHashMap to have predictable iteration to simplify test
        .collect(groupingBy(WorkflowInstance::workflowId, LinkedHashMap::new,
            toCollection(() -> new TreeSet<>(comparing(WorkflowInstance::parameter)))));

    return merge(groups.values(), activeInstances.size());
  }

  /**
   * Merge a list of sorted sets into a list while respecting the original order within each individual set.
   *
   * For example [(1, 3, 4), (2, 5), (8, 10)] may become [1, 8, 2, 3, 10, 4, 5]
   *
   * This is how it works:
   *
   * 1. Generate full indices of the destination list
   * 2. Shuffle the indices
   * 3. For each set, take a sorted sublist of the shuffled indices; and insert each item into the indices
   * specified by the sublist
   */
  private List<WorkflowInstance> merge(Collection<SortedSet<WorkflowInstance>> instanceGroups, int size) {
    var indices = IntStream.range(0, size).boxed().collect(toCollection(ArrayList::new));
    shuffler.shuffle(indices);

    var merged = new WorkflowInstance[size];

    var from = 0;
    for (var group : instanceGroups) {
      var subIndices = indices.subList(from, from + group.size()).stream().sorted().collect(toList());
      var j = 0;
      for (var instance : group) {
        merged[subIndices.get(j++)] = instance;
      }
      from += group.size();
    }

    return List.of(merged);
  }

  @VisibleForTesting
  static long randomizedDelay() {
    return ThreadLocalRandom.current().nextLong(RANDOMIZED_DELAY_BASE / 2, (long) (RANDOMIZED_DELAY_BASE * 1.5));
  }

  private Optional<Workflow> readWorkflow(WorkflowId workflowId) {
    try {
      return storage.workflow(workflowId);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean limitReached(final String resourceId, ConcurrentMap<String, Boolean> resourceExhaustedCache) {
    return resourceExhaustedCache.computeIfAbsent(resourceId, k -> {
      try {
        return !shardedCounter.counterHasSpareCapacity(resourceId);
      } catch (RuntimeException | IOException e) {
        log.warn("Failed to check resource counter limit", e);
        return false;
      }
    });
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

  private void sendDequeue(WorkflowInstance workflowInstance, RunState state, Set<String> resourceIds) {
    double sleepingTimeSeconds = dequeueRateLimiter.acquire();
    if (sleepingTimeSeconds > 0.0001) {
      final double sleepingTimeMillis = sleepingTimeSeconds * 1000;
      final String message = "Dequeue rate limited and slept for " + sleepingTimeMillis + " ms";
      log.debug(message, sleepingTimeMillis);
      tracer.getCurrentSpan().addAnnotation(message);
    }

    if (state.data().tries() == 0) {
      log.info("Executing {}", workflowInstance);
    } else {
      log.info("Executing {}, retry #{}", workflowInstance, state.data().tries());
    }
    var dequeue = Event.dequeue(workflowInstance, resourceIds);
    stateManager.receiveIgnoreClosed(dequeue, state.counter());
  }

  interface Shuffler {
    void shuffle(List<?> list);

    Shuffler DEFAULT = Collections::shuffle;
    Shuffler NO_OP = list -> {};
  }
}
