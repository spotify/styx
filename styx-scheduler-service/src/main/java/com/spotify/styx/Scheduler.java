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

import static com.spotify.styx.WorkflowExecutionGate.NOOP;
import static java.util.Collections.emptySet;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

import com.google.auto.value.AutoValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
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
import com.spotify.styx.state.Message;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.TimeoutConfig;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
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

  @VisibleForTesting
  static final String GLOBAL_RESOURCE_ID = "GLOBAL_STYX_CLUSTER";

  private static final int SCHEDULING_BATCH_SIZE = 32;

  private final Time time;
  private final TimeoutConfig ttls;
  private final StateManager stateManager;
  private final WorkflowCache workflowCache;
  private final Storage storage;
  private final WorkflowResourceDecorator resourceDecorator;
  private final Stats stats;
  private final RateLimiter dequeueRateLimiter;
  private final WorkflowExecutionGate gate;

  public Scheduler(Time time, TimeoutConfig ttls, StateManager stateManager,
      WorkflowCache workflowCache, Storage storage,
      WorkflowResourceDecorator resourceDecorator,
      Stats stats, RateLimiter dequeueRateLimiter, WorkflowExecutionGate gate) {
    this.time = Objects.requireNonNull(time);
    this.ttls = Objects.requireNonNull(ttls);
    this.stateManager = Objects.requireNonNull(stateManager);
    this.workflowCache = Objects.requireNonNull(workflowCache);
    this.storage = Objects.requireNonNull(storage);
    this.resourceDecorator = Objects.requireNonNull(resourceDecorator);
    this.stats = Objects.requireNonNull(stats);
    this.dequeueRateLimiter = Objects.requireNonNull(dequeueRateLimiter, "dequeueRateLimiter");
    this.gate = Objects.requireNonNull(gate, "gate");
  }

  void tick() {
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

    final List<InstanceState> activeStates = stateManager.activeStates().entrySet().stream()
        .map(entry -> InstanceState.create(entry.getKey(), entry.getValue()))
        .collect(toList());

    final Set<WorkflowInstance> timedOutInstances =
        activeStates.parallelStream()
            .filter(entry -> hasTimedOut(entry.runState()))
            .map(InstanceState::workflowInstance)
            .collect(toSet());

    final Map<WorkflowId, Set<String>> workflowResourceReferences =
        activeStates.parallelStream()
            .map(InstanceState::workflowInstance)
            .map(WorkflowInstance::workflowId)
            .distinct()
            .collect(toMap(
                workflowId -> workflowId,
                workflowId -> workflowResources(globalConcurrency, workflowId)));

    final Map<String, Long> currentResourceUsage =
        activeStates.parallelStream()
            .filter(entry -> !timedOutInstances.contains(entry.workflowInstance()))
            .filter(entry -> isConsumingResources(entry.runState().state()))
            .flatMap(instanceState -> pairWithResources(globalConcurrency, instanceState))
            .collect(groupingByConcurrent(
                ResourceWithInstance::resource,
                ConcurrentHashMap::new,
                counting()));

    final List<InstanceState> eligibleInstances =
        activeStates.parallelStream()
            .filter(entry -> !timedOutInstances.contains(entry.workflowInstance()))
            .filter(entry -> shouldExecute(entry.runState()))
            .collect(toCollection(Lists::newArrayList));
    Collections.shuffle(eligibleInstances);

    timedOutInstances.forEach(this::sendTimeout);

    limitAndDequeue(config, resources, workflowResourceReferences, currentResourceUsage,
        eligibleInstances);

    updateStats(resources, currentResourceUsage);
  }

  private void limitAndDequeue(
      StyxConfig config,
      Map<String, Resource> resources,
      Map<WorkflowId, Set<String>> workflowResourceReferences,
      Map<String, Long> currentResourceUsage, List<InstanceState> eligibleInstances) {

    // Enable gating unless disabled in runtime config
    final WorkflowExecutionGate gate = config.executionGatingEnabled() ? this.gate : NOOP;

    // Process the eligible instances in batches in order to parallelize execution blocker lookup
    for (List<InstanceState> batch : Lists.partition(eligibleInstances, SCHEDULING_BATCH_SIZE)) {

      // Asynchronously look up execution blockers for a batch of instances
      final List<CompletionStage<Optional<ExecutionBlocker>>> blockers = batch.stream()
          .map(InstanceState::workflowInstance)
          .map(gate::executionBlocker)
          .collect(toList());

      // Evaluate each instance in the batch for dequeuing
      for (int i = 0; i < batch.size(); i++) {

        final boolean proceed = limitAndDequeue(resources, workflowResourceReferences,
            currentResourceUsage, batch.get(i), blockers.get(i));

        // Stop processing if rate limit was hit or thread was interrupted
        if (!proceed) {
          return;
        }
      }
    }
  }

  /**
   * We'll keep counting terminal states as if they consume resources. They are transient states and
   * should go away fairly quickly. If they don't, then we might be having some trouble cleaning up
   * the containers. In that case it's better to be conservative on resource usage.
   *
   * @return true if the state consumes resources, otherwise false.
   */
  private static boolean isConsumingResources(State state) {
    return state != State.NEW && state != State.QUEUED;
  }

  private void updateStats(Map<String, Resource> resources,
                           Map<String, Long> currentResourceUsage) {
    resources.values().forEach(r -> stats.recordResourceConfigured(r.id(), r.concurrency()));
    currentResourceUsage.forEach(stats::recordResourceUsed);
    Sets.difference(resources.keySet(), currentResourceUsage.keySet())
        .forEach(r -> stats.recordResourceUsed(r, 0));
  }

  private boolean limitAndDequeue(Map<String, Resource> resources,
      Map<WorkflowId, Set<String>> workflowResourceReferences,
      Map<String, Long> currentResourceUsage, InstanceState instance,
      CompletionStage<Optional<ExecutionBlocker>> executionBlockerFuture) {

    // Check for execution blocker
    Optional<ExecutionBlocker> blocker = Optional.empty();
    try {
      blocker = executionBlockerFuture.toCompletableFuture().get(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.debug("Interrupted");
      return false;
    } catch (ExecutionException | TimeoutException e) {
      LOG.warn("Failed to check execution blocker for {}, assuming there is no blocker",
          instance.workflowInstance(), e);
    }

    if (blocker.isPresent()) {
      stateManager.receiveIgnoreClosed(Event.retryAfter(
          instance.workflowInstance(),
          blocker.get().delay().toMillis()));
      LOG.debug("Dequeue rescheduled: {}: {}", instance.workflowInstance(), blocker.get());
      return true;
    }

    final Set<String> workflowResourceRefs =
        workflowResourceReferences.getOrDefault(instance.workflowInstance().workflowId(), emptySet());

    final Set<String> instanceResourceRefs = workflowCache.workflow(instance.workflowInstance().workflowId())
        .map(workflow -> resourceDecorator.decorateResources(
            instance.runState(), workflow.configuration(), workflowResourceRefs))
        .orElse(workflowResourceRefs);

    final Set<String> unknownResources = instanceResourceRefs.stream()
        .filter(resourceRef -> !resources.containsKey(resourceRef))
        .collect(toSet());

    final Set<String> depletedResources = instanceResourceRefs.stream()
        .filter(resourceId -> {
          if (!resources.containsKey(resourceId)) {
            return false;
          }

          final Resource resource = resources.get(resourceId);
          final long usage = currentResourceUsage.getOrDefault(resourceId, 0L);
          return usage >= resource.concurrency();
        })
        .collect(toSet());

    if (!unknownResources.isEmpty()) {
      stateManager.receiveIgnoreClosed(Event.runError(
          instance.workflowInstance(),
          String.format("Referenced resources not found: %s", unknownResources)));
    } else if (!depletedResources.isEmpty()) {
      final Message message = Message.info(
          String.format("Resource limit reached for: %s",
              depletedResources.stream()
                  .map(resources::get)
                  // Sort resource descriptions to get deterministic message contents
                  .map(Resource::toString)
                  .sorted()
                  .collect(toList())));
      if (!instance.runState().data().message().map(message::equals).orElse(false)) {
        stateManager.receiveIgnoreClosed(Event.info(instance.workflowInstance(), message));
      }
    } else {
      if (!dequeueRateLimiter.tryAcquire()) {
        LOG.debug("Dequeue rate limited");
        return false;
      }
      instanceResourceRefs.forEach(id -> currentResourceUsage.computeIfAbsent(id, id_ -> 0L));
      instanceResourceRefs.forEach(id -> currentResourceUsage.compute(id, (id_, l) -> l + 1));
      sendDequeue(instance);
    }

    return true;
  }

  private Stream<ResourceWithInstance> pairWithResources(Optional<Long> globalConcurrency,
                                                         InstanceState instanceState) {
    final WorkflowId workflowId = instanceState.workflowInstance().workflowId();
    final Set<String> workflowResources = workflowResources(globalConcurrency, workflowId);
    return workflowCache.workflow(workflowId)
        .map(workflow -> resourceDecorator.decorateResources(
            instanceState.runState(), workflow.configuration(), workflowResources))
        .orElse(workflowResources).stream()
        .map(resource -> ResourceWithInstance.create(resource, instanceState));
  }

  private Set<String> workflowResources(Optional<Long> globalConcurrency, WorkflowId workflowId) {
    final ImmutableSet.Builder<String> builder = ImmutableSet.builder();

    globalConcurrency.ifPresent(concurrency -> builder.add(GLOBAL_RESOURCE_ID));

    workflowCache.workflow(workflowId).ifPresent(workflow ->
        builder.addAll(workflow.configuration().resources()));

    return builder.build();
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

  private void sendDequeue(InstanceState instanceState) {
    final WorkflowInstance workflowInstance = instanceState.workflowInstance();
    final RunState state = instanceState.runState();

    if (state.data().tries() == 0) {
      LOG.info("Triggering {}", workflowInstance.toKey());
    } else {
      LOG.info("{} executing retry #{}", workflowInstance.toKey(), state.data().tries());
    }
    stateManager.receiveIgnoreClosed(Event.dequeue(workflowInstance));
  }

  private boolean hasTimedOut(RunState runState) {
    if (runState.state().isTerminal()) {
      return false;
    }

    final Instant now = time.get();
    final Instant deadline = Instant
        .ofEpochMilli(runState.timestamp())
        .plus(ttls.ttlOf(runState.state()));

    return !deadline.isAfter(now);
  }

  private void sendTimeout(WorkflowInstance workflowInstance) {
    LOG.info("Found stale state, issuing timeout for {}", workflowInstance);
    stateManager.receiveIgnoreClosed(Event.timeout(workflowInstance));
  }

  @AutoValue
  abstract static class ResourceWithInstance {
    abstract String resource();
    abstract InstanceState instanceState();

    static ResourceWithInstance create(String resource, InstanceState instanceState) {
      return new AutoValue_Scheduler_ResourceWithInstance(resource, instanceState);
    }
  }
}
