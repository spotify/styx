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

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingByConcurrent;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.styx.WorkflowCache;
import com.spotify.styx.WorkflowResourceDecorator;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class StateUtil {

  public static final String GLOBAL_RESOURCE_ID = "GLOBAL_STYX_CLUSTER";

  private StateUtil() {
    throw new UnsupportedOperationException();
  }

  public static List<InstanceState> getActiveInstanceStates(
      Map<WorkflowInstance, RunState> activeStatesMap) {
    return activeStatesMap.entrySet().stream()
        .map(entry -> InstanceState.create(entry.getKey(), entry.getValue()))
        .collect(toList());
  }

  public static Set<WorkflowInstance> getTimedOutInstances(List<InstanceState> activeStates,
                                                           Instant instant,
                                                           TimeoutConfig ttl) {
    return activeStates.parallelStream()
        .filter(entry -> hasTimedOut(entry.runState(), instant, ttl.ttlOf(entry.runState().state())))
        .map(InstanceState::workflowInstance)
        .collect(toSet());
  }

  public static ConcurrentHashMap<String, Long> getResourceUsage(boolean globalConcurrencyEnabled,
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

  public static Map<String, Long> getResourcesUsageMap(Storage storage, TimeoutConfig timeoutConfig,
                                                       WorkflowCache workflowCache, Instant instant,
                                                       WorkflowResourceDecorator resourceDecorator)
      throws IOException {
    // The only inconsistency left is to miss active workflow instances. Outdated instances or
    // inactive instances will be correctly updated or removed via the strongly consistent lookups
    final Map<WorkflowInstance, RunState> strictActiveStates = storage.readActiveStates().entrySet()
        .parallelStream().filter(entry -> {
          try {
            return storage.readActiveState(entry.getKey()).isPresent();
          } catch (IOException e) {
            throw new RuntimeException("Error while fetching active states", e);
          }
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    final List<InstanceState> activeInstanceStates = getActiveInstanceStates(strictActiveStates);
    boolean globalConcurrencyEnabled = storage.config().globalConcurrency().isPresent();
    final Set<WorkflowInstance> timedOutInstances =
        getTimedOutInstances(activeInstanceStates, instant, timeoutConfig);
    return getResourceUsage(globalConcurrencyEnabled,
        activeInstanceStates, timedOutInstances, resourceDecorator, workflowCache.all());
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

  private static boolean hasTimedOut(RunState runState, Instant instant, Duration timeout) {
    if (runState.state().isTerminal()) {
      return false;
    }

    final Instant deadline = Instant
        .ofEpochMilli(runState.timestamp())
        .plus(timeout);

    return !deadline.isAfter(instant);
  }

  static boolean isConsumingResources(RunState.State state) {
    return ImmutableList.of(
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

