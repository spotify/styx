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
import static com.spotify.styx.WorkflowExecutionGate.NOOP;
import static com.spotify.styx.state.StateUtil.GLOBAL_RESOURCE_ID;
import static com.spotify.styx.state.StateUtil.getActiveInstanceStates;
import static com.spotify.styx.state.StateUtil.getResourceUsage;
import static com.spotify.styx.state.StateUtil.getTimedOutInstances;
import static com.spotify.styx.state.StateUtil.workflowResources;
import static java.util.Collections.emptySet;
import static java.util.Comparator.comparingLong;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

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
import com.spotify.styx.state.InstanceState;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.TimeoutConfig;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
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

  private static final int SCHEDULING_BATCH_SIZE = 16;

  private final Time time;
  private final TimeoutConfig ttls;
  private final StateManager stateManager;
  private final Storage storage;
  private final WorkflowResourceDecorator resourceDecorator;
  private final Stats stats;
  private final RateLimiter dequeueRateLimiter;
  private final WorkflowExecutionGate gate;

  public Scheduler(Time time, TimeoutConfig ttls, StateManager stateManager,
                   Storage storage,
                   WorkflowResourceDecorator resourceDecorator,
                   Stats stats, RateLimiter dequeueRateLimiter, WorkflowExecutionGate gate) {
    this.time = Objects.requireNonNull(time);
    this.ttls = Objects.requireNonNull(ttls);
    this.stateManager = Objects.requireNonNull(stateManager);
    this.storage = Objects.requireNonNull(storage);
    this.resourceDecorator = Objects.requireNonNull(resourceDecorator);
    this.stats = Objects.requireNonNull(stats);
    this.dequeueRateLimiter = Objects.requireNonNull(dequeueRateLimiter, "dequeueRateLimiter");
    this.gate = Objects.requireNonNull(gate, "gate");
  }

  void tick() {
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

    final Map<WorkflowInstance, RunState> activeStatesMap = stateManager.getActiveStates();
    final List<InstanceState> activeStates = getActiveInstanceStates(activeStatesMap);

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

    gateAndDequeueInstances(config, resources, workflowResourceReferences,
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

  private void gateAndDequeueInstances(
      StyxConfig config,
      Map<String, Resource> resources,
      Map<WorkflowId, Set<String>> workflowResourceReferences,
      Map<WorkflowId, Workflow> workflows,
      List<InstanceState> eligibleInstances) {
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
        final InstanceState instanceState = batch.get(i);
        dequeueInstance(resources, workflowResourceReferences,
            Optional.ofNullable(workflows.get(instanceState.workflowInstance().workflowId())),
            instanceState, blockers.get(i));
      }
    }
  }

  private void dequeueInstance(Map<String, Resource> resources,
                               Map<WorkflowId, Set<String>> workflowResourceReferences,
                               Optional<Workflow> workflowOpt,
                               InstanceState instanceState,
                               CompletionStage<Optional<ExecutionBlocker>> executionBlockerFuture) {

    // Check for execution blocker
    Optional<ExecutionBlocker> blocker = Optional.empty();
    try {
      blocker = executionBlockerFuture.toCompletableFuture().get(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.debug("Thread interrupted");
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
    } else {
      double sleepingTime = dequeueRateLimiter.acquire();
      if (sleepingTime > 0.0001) {
        LOG.debug("Dequeue rate limited and slept for {} ms", sleepingTime * 1000);
      }

      // Racy: some resources may have been removed (become unknown) by now; in that case the
      // counters code during dequeue will treat them as unlimited...
      sendDequeue(instanceState, instanceResourceRefs);
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
      LOG.info("Executing {}", workflowInstance.toKey());
    } else {
      LOG.info("Executing {}, retry #{}", workflowInstance.toKey(), state.data().tries());
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
