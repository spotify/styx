/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.storage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.ResourceNotFoundException;
import com.spotify.styx.util.TriggerInstantSpec;
import com.spotify.styx.util.TriggerUtil;
import com.spotify.styx.util.WorkflowStateUtil;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A Storage implementation with state stored in memory. For testing.
 */
public class InMemStorage implements Storage {

  private final Set<WorkflowId> enabledWorkflows = Sets.newConcurrentHashSet();
  private final ConcurrentMap<WorkflowId, Workflow> workflowStore = Maps.newConcurrentMap();
  private final ConcurrentMap<String, Resource> resourceStore = Maps.newConcurrentMap();
  private final ConcurrentMap<String, Backfill> backfillStore = Maps.newConcurrentMap();
  private final ConcurrentMap<WorkflowId, WorkflowState> workflowStatePerWorkflowId = Maps
      .newConcurrentMap();

  public final List<SequenceEvent> writtenEvents = Lists.newCopyOnWriteArrayList();
  public final Map<WorkflowInstance, RunState> activeStatesMap = Maps.newHashMap();

  public final CountDownLatch countDown;

  public InMemStorage() {
    this(0);
  }

  public InMemStorage(int expectedWorkflowExecutionInfoStored) {
    this.countDown = new CountDownLatch(expectedWorkflowExecutionInfoStored);
  }

  @Override
  public void close() {
  }

  @Override
  public StyxConfig config() {
    return StyxConfig.newBuilder()
        .globalEnabled(true)
        .globalDockerRunnerId("default")
        .build();
  }

  @Override
  public void storeWorkflow(Workflow workflow) throws IOException {
    workflowStore.put(workflow.id(), workflow);

    WorkflowState originalState = Optional.ofNullable(
        workflowStatePerWorkflowId.get(workflow.id())
    ).orElse(WorkflowState.patchEnabled(false));

    workflowStatePerWorkflowId.put(workflow.id(), originalState);
  }

  @Override
  public Optional<Workflow> workflow(WorkflowId workflowId) throws IOException {
    return Optional.ofNullable(workflowStore.get(workflowId));
  }

  @Override
  public List<Workflow> workflows(String componentId) throws IOException {
    return workflowStore.values().stream()
        .filter(w -> w.componentId().equals(componentId))
        .collect(Collectors.toList());
  }

  @Override
  public void delete(WorkflowId workflowId) throws IOException {
    workflowStore.remove(workflowId);
  }

  @Override
  public void updateNextNaturalTrigger(WorkflowId workflowId, TriggerInstantSpec spec)
      throws IOException {
    throw new UnsupportedOperationException("Unsupported Operation!");
  }

  @Override
  public Map<Workflow, TriggerInstantSpec> workflowsWithNextNaturalTrigger() throws IOException {
    throw new UnsupportedOperationException("Unsupported Operation!");
  }

  @Override
  public Map<WorkflowId, Workflow> workflows() throws IOException {
    throw new UnsupportedOperationException("Unsupported Operation!");
  }

  @Override
  public Map<WorkflowId, Workflow> workflows(Set<WorkflowId> workflowIds) {
    throw new UnsupportedOperationException("Unsupported Operation!");
  }

  @Override
  public WorkflowInstanceExecutionData executionData(WorkflowInstance workflowInstance)
      throws IOException {
    throw new UnsupportedOperationException("Unsupported Operation!");
  }

  @Override
  public List<WorkflowInstanceExecutionData> executionData(WorkflowId workflowId, String offset,
                                                           int limit) throws IOException {
    throw new UnsupportedOperationException("Unsupported Operation!");
  }

  @Override
  public List<WorkflowInstanceExecutionData> executionData(WorkflowId workflowId,
                                                           String startParameter,
                                                           String stopParameter)
      throws IOException {
    throw new UnsupportedOperationException("Unsupported Operation!");
  }


  @Override
  public boolean enabled(WorkflowId workflowId) {
    return enabledWorkflows.contains(workflowId);
  }

  @Override
  public Set<WorkflowId> enabled() throws IOException {
    return enabledWorkflows;
  }

  @Override
  public void patchState(WorkflowId workflowId, WorkflowState patchState) throws IOException {
    if (!workflowStore.containsKey(workflowId)) {
      throw new ResourceNotFoundException("Workflow not found");
    }

    patchState.enabled().ifPresent(enabled -> {
      if (enabled) {
        enabledWorkflows.add(workflowId);
      } else {
        enabledWorkflows.remove(workflowId);
      }
    });

    Optional<WorkflowState> originalState = Optional.of(
        workflowStatePerWorkflowId.getOrDefault(workflowId, patchState));
    final WorkflowState patchWorkflowState =
        WorkflowStateUtil.patchWorkflowState(originalState, patchState);
    workflowStatePerWorkflowId.put(workflowId, patchWorkflowState);
  }

  @Override
  public WorkflowState workflowState(WorkflowId workflowId) throws IOException {
    return workflowStatePerWorkflowId.get(workflowId);
  }

  @Override
  public Optional<Resource> resource(String id) throws IOException {
    return Optional.ofNullable(resourceStore.get(id));
  }

  @Override
  public void storeResource(Resource resource) throws IOException {
    resourceStore.put(resource.id(), resource);
  }

  @Override
  public List<Resource> resources() throws IOException {
    return ImmutableList.copyOf(resourceStore.values());
  }

  @Override
  public void deleteResource(String id) throws IOException {
    resourceStore.remove(id);
  }

  @Override
  public List<Backfill> backfills(boolean showAll) throws IOException {
    Stream<Backfill> backfillStream = backfillStore.values().stream();

    if (!showAll) {
      backfillStream = backfillStream
          .filter(backfill -> backfill.halted() && backfill.allTriggered());
    }

    return ImmutableList.copyOf(backfillStream.collect(Collectors.toList())
    );
  }

  @Override
  public List<Backfill> backfillsForComponent(boolean showAll, String component)
      throws IOException {
    Stream<Backfill> backfillStream = backfillStore.values().stream()
        .filter(backfill -> backfill.workflowId().componentId().equals(component));

    if (!showAll) {
      backfillStream = backfillStream
          .filter(backfill -> backfill.halted() && backfill.allTriggered());
    }

    return ImmutableList.copyOf(backfillStream.collect(Collectors.toList()));
  }

  @Override
  public List<Backfill> backfillsForWorkflow(boolean showAll, String workflow) throws IOException {
    Stream<Backfill> backfillStream = backfillStore.values().stream()
        .filter(backfill -> backfill.workflowId().id().equals(workflow));

    if (!showAll) {
      backfillStream = backfillStream
          .filter(backfill -> backfill.halted() && backfill.allTriggered());
    }

    return ImmutableList.copyOf(backfillStream.collect(Collectors.toList()));
  }

  @Override
  public List<Backfill> backfillsForWorkflowId(boolean showAll, WorkflowId workflowId)
      throws IOException {
    Stream<Backfill> backfillStream = backfillStore.values().stream()
        .filter(backfill -> backfill.workflowId().equals(workflowId));

    if (!showAll) {
      backfillStream = backfillStream
          .filter(backfill -> backfill.halted() && backfill.allTriggered());
    }

    return ImmutableList.copyOf(backfillStream.collect(Collectors.toList()));
  }

  @Override
  public Optional<Backfill> backfill(String id) {
    return Optional.ofNullable(backfillStore.get(id));
  }

  @Override
  public void storeBackfill(Backfill backfill) throws IOException {
    backfillStore.put(backfill.id(), backfill);
  }

  @Override
  public Map<Integer, Long> shardsForCounter(String counterId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLimitForCounter(String counterId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T, E extends Exception> T runInTransactionWithRetries(TransactionFunction<T, E> f)
      throws IOException, E {
    throw new UnsupportedOperationException("Unsupported Operation!");
  }

  @Override
  public SortedSet<SequenceEvent> readEvents(WorkflowInstance workflowInstance) {
    final SortedSet<SequenceEvent> events = Sets.newTreeSet(SequenceEvent.COUNTER_COMPARATOR);
    writtenEvents.stream()
        .filter(e -> e.event().workflowInstance().equals(workflowInstance))
        .forEach(events::add);

    return events;
  }

  @Override
  public void writeEvent(SequenceEvent sequenceEvent) {
    writtenEvents.add(sequenceEvent);
  }

  @Override
  public Optional<Long> getLatestStoredCounter(WorkflowInstance workflowInstance)
      throws IOException {
    final SortedSet<SequenceEvent> storedEvents = readEvents(workflowInstance);
    if (storedEvents.isEmpty()) {
      return Optional.empty();
    } else {
      final SequenceEvent lastStoredEvent = storedEvents.last();
      return Optional.of(lastStoredEvent.counter());
    }
  }

  @Override
  public void writeActiveState(WorkflowInstance workflowInstance, RunState state) {
    activeStatesMap.put(workflowInstance, state);
  }

  @Override
  public void deleteActiveState(WorkflowInstance workflowInstance) {
    activeStatesMap.remove(workflowInstance);
  }

  @Override
  public Set<WorkflowInstance> listActiveInstances() {
    return activeStatesMap.keySet();
  }

  @Override
  public Map<WorkflowInstance, RunState> readActiveStates() throws IOException {
    return activeStatesMap;
  }

  @Override
  public Map<WorkflowInstance, RunState> readActiveStates(String componentId)
      throws IOException {
    return activeStatesMap.entrySet().stream()
        .filter((entry) -> componentId.equals(entry.getKey().workflowId().componentId()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Map<WorkflowInstance, RunState> readActiveStatesByTriggerId(String triggerId)
      throws IOException {
    return activeStatesMap.entrySet().stream()
        .filter((entry) -> triggerId.equals(TriggerUtil.triggerId(entry.getValue().data().trigger().get())))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  @Override
  public Optional<RunState> readActiveState(WorkflowInstance workflowInstance) {
    return Optional.ofNullable(activeStatesMap.get(workflowInstance));
  }
}
