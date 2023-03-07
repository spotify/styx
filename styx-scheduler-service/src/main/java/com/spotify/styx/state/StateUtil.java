/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

package com.spotify.styx.state;

import static com.spotify.styx.storage.Storage.GLOBAL_RESOURCE_ID;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import com.spotify.styx.WorkflowResourceDecorator;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

public final class StateUtil {

  private StateUtil() {
    throw new UnsupportedOperationException();
  }

  static List<InstanceState> getActiveInstanceStates(
      Map<WorkflowInstance, RunState> activeStatesMap) {
    return activeStatesMap.entrySet().stream()
        .map(entry -> InstanceState.create(entry.getKey(), entry.getValue()))
        .collect(toList());
  }

  static Set<WorkflowInstance> getTimedOutInstances(Map<WorkflowId, Workflow> workflows,
                                                    List<InstanceState> activeStates,
                                                    Instant instant,
                                                    TimeoutConfig ttl) {
    return activeStates.parallelStream()
        .filter(entry -> {
          final Optional<Workflow> workflowOpt =
              Optional.ofNullable(workflows.get(entry.workflowInstance().workflowId()));
          return hasTimedOut(workflowOpt, entry.runState(), instant, ttl.ttlOf(entry.runState().state()), ttl.getMaxRunningTimeout());
        })
        .map(InstanceState::workflowInstance)
        .collect(toSet());
  }

  static ConcurrentMap<String, Long> getResourceUsage(boolean globalConcurrencyEnabled,
                                                      List<InstanceState> activeStates,
                                                      Set<WorkflowInstance> timedOutInstances,
                                                      WorkflowResourceDecorator resourceDecorator,
                                                      Map<WorkflowId, Workflow> workflows) {
    return activeStates.parallelStream()
        .filter(entry -> !timedOutInstances.contains(entry.workflowInstance()))
        .filter(entry -> isConsumingResources(entry.runState().state()))
        .flatMap(instanceState -> pairWithResources(globalConcurrencyEnabled, instanceState,
            workflows, resourceDecorator))
        .collect(groupingByConcurrent(
            ResourceWithInstance::resource,
            ConcurrentHashMap::new,
            counting()));
  }

  private static Stream<ResourceWithInstance> pairWithResources(boolean globalConcurrencyEnabled,
                                                                InstanceState instanceState,
                                                                Map<WorkflowId, Workflow> workflows,
                                                                WorkflowResourceDecorator resourceDecorator) {
    final Optional<Workflow> workflowOpt =
        Optional.ofNullable(workflows.get(instanceState.workflowInstance().workflowId()));
    final Set<String> workflowResources = workflowResources(globalConcurrencyEnabled, workflowOpt);
    return workflowOpt
        .map(workflow -> resourceDecorator.decorateResources(
            instanceState.runState(), workflow.configuration(), workflowResources))
        .orElse(workflowResources).stream()
        .map(resource -> ResourceWithInstance.create(resource, instanceState));
  }

  public static Set<String> workflowResources(boolean globalConcurrenyEnabled,
                                              Optional<Workflow> workflowOpt) {
    final ImmutableSet.Builder<String> builder = ImmutableSet.builder();
    if (globalConcurrenyEnabled) {
      builder.add(GLOBAL_RESOURCE_ID);
    }
    workflowOpt.ifPresent(wf -> builder.addAll(wf.configuration().resources()));
    return builder.build();
  }

  public static boolean hasTimedOut(Optional<Workflow> workflowOpt, RunState runState, Instant instant,
                                    Duration runStateTimeout, Duration maxRunningTimeout) {
    if (runState.state().isTerminal()) {
      return false;
    }

    Duration effectiveTimeout = runStateTimeout;

    if (runState.state() == RunState.State.RUNNING) {
      effectiveTimeout =  workflowOpt
              .flatMap(workflow -> workflow.configuration().runningTimeout())
              .orElse(runStateTimeout);
      effectiveTimeout = effectiveTimeout.compareTo(maxRunningTimeout) < 0 ? effectiveTimeout : maxRunningTimeout;
    }

    final Instant deadline = Instant
        .ofEpochMilli(runState.timestamp())
        .plus(effectiveTimeout);

    return !deadline.isAfter(instant);
  }

  public static boolean isConsumingResources(RunState.State state) {
    return List.of(
        RunState.State.PREPARE,
        RunState.State.SUBMITTING,
        RunState.State.SUBMITTED,
        RunState.State.RUNNING).contains(state);
  }

  @AutoValue
  abstract static class ResourceWithInstance {
    abstract String resource();
    abstract InstanceState instanceState();

    static ResourceWithInstance create(String resource, InstanceState instanceState) {
      return new AutoValue_StateUtil_ResourceWithInstance(resource, instanceState);
    }
  }
}

