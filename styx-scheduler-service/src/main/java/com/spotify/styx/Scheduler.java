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
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Resource;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
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

  private final Time time;
  private final TimeoutConfig ttls;
  private final StateManager stateManager;
  private final WorkflowCache workflowCache;
  private final Storage storage;
  private final WorkflowResourceDecorator resourceDecorator;
  private final Stats stats;

  public Scheduler(Time time, TimeoutConfig ttls, StateManager stateManager,
                   WorkflowCache workflowCache, Storage storage,
                   WorkflowResourceDecorator resourceDecorator,
                   Stats stats) {
    this.time = Objects.requireNonNull(time);
    this.ttls = Objects.requireNonNull(ttls);
    this.stateManager = Objects.requireNonNull(stateManager);
    this.workflowCache = Objects.requireNonNull(workflowCache);
    this.storage = Objects.requireNonNull(storage);
    this.resourceDecorator = Objects.requireNonNull(resourceDecorator);
    this.stats = Objects.requireNonNull(stats);
  }

  void tick() {
    final Map<String, Resource> resources;
    final Optional<Long> globalConcurrency;
    try {
      resources = storage.resources().stream().collect(toMap(Resource::id, identity()));
      globalConcurrency = storage.globalConcurrency();
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
            .filter(entry -> entry.runState().state() != State.QUEUED)
            .flatMap(instanceState -> pairWithResources(globalConcurrency, instanceState))
            .collect(groupingByConcurrent(
                ResourceWithInstance::resource,
                ConcurrentHashMap::new,
                counting()));

    updateStats(resources, currentResourceUsage);

    final List<InstanceState> eligibleInstances =
        activeStates.parallelStream()
            .filter(entry -> !timedOutInstances.contains(entry.workflowInstance()))
            .filter(entry -> shouldExecute(entry.runState()))
            .collect(toCollection(Lists::newArrayList));
    Collections.shuffle(eligibleInstances);

    final Consumer<InstanceState> limitAndDequeue = (instance) -> {
      final Set<String> workflowResourceRefs =
          workflowResourceReferences.getOrDefault(instance.workflowInstance().workflowId(), emptySet());

      final Set<String> instanceResourceRefs = workflowCache.workflow(instance.workflowInstance().workflowId())
          .map(workflow -> resourceDecorator.decorateResources(
              instance.runState(), workflow.configuration(), workflowResourceRefs))
          .orElse(workflowResourceRefs);

      if (instanceResourceRefs.isEmpty()) {
        sendDequeue(instance, instanceResourceRefs);
      } else {
        evaluateResourcesForDequeue(resources, currentResourceUsage, instance, instanceResourceRefs);
      }
    };

    timedOutInstances.forEach(this::sendTimeout);
    eligibleInstances.forEach(limitAndDequeue);
  }

  private void updateStats(Map<String, Resource> resources,
                           Map<String, Long> currentResourceUsage) {
    resources.values().forEach(r -> stats.recordResourceConfigured(r.id(), r.concurrency()));
    currentResourceUsage.forEach(stats::recordResourceUsed);
    Sets.difference(resources.keySet(), currentResourceUsage.keySet())
        .forEach(r -> stats.recordResourceUsed(r, 0));
  }

  private void evaluateResourcesForDequeue(
      Map<String, Resource> resources,
      Map<String, Long> currentResourceUsage,
      InstanceState instance,
      Set<String> resourceRefs) {
    final Set<String> unknownResources = resourceRefs.stream()
        .filter(resourceRef -> !resources.containsKey(resourceRef))
        .collect(toSet());

    final Set<String> depletedResources = resourceRefs.stream()
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
                  .collect(toList())));
      final List<Message> messages = instance.runState().data().messages();
      if (messages.size() == 0 || !message.equals(messages.get(messages.size() - 1))) {
        stateManager.receiveIgnoreClosed(Event.info(instance.workflowInstance(), message));
      }
    } else {
      resourceRefs.forEach(id -> currentResourceUsage.computeIfAbsent(id, id_ -> 0L));
      resourceRefs.forEach(id -> currentResourceUsage.compute(id, (id_, l) -> l + 1));
      sendDequeue(instance, resourceRefs.stream()
          .filter(r -> !GLOBAL_RESOURCE_ID.equals(r))
          .collect(toSet()));
    }
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

  private void sendDequeue(InstanceState instanceState, Set<String> resources) {
    final WorkflowInstance workflowInstance = instanceState.workflowInstance();
    final RunState state = instanceState.runState();

    if (state.data().tries() == 0) {
      LOG.info("Triggering {}", workflowInstance.toKey());
    } else {
      LOG.info("{} executing retry #{}", workflowInstance.toKey(), state.data().tries());
    }
    stateManager.receiveIgnoreClosed(Event.dequeue(workflowInstance, resources));
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
