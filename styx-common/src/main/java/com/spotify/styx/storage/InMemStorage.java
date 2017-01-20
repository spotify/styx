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
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.util.ResourceNotFoundException;
import com.spotify.styx.util.WorkflowStateUtil;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

/**
 * A Storage implementation with state stored in memory. For testing.
 */
public class InMemStorage implements Storage {

  private boolean globalEnabled = true;
  private final Set<WorkflowId> enabledWorkflows = Sets.newConcurrentHashSet();
  private final Set<String> components = Sets.newConcurrentHashSet();
  private final ConcurrentMap<WorkflowId, Workflow> workflowStore = Maps.newConcurrentMap();
  private final ConcurrentMap<String, Resource> resourceStore = Maps.newConcurrentMap();
  private final ConcurrentMap<WorkflowId, String> dockerImagesPerWorkflowId = Maps.newConcurrentMap();
  private final ConcurrentMap<String, String> dockerImagesPerComponent = Maps.newConcurrentMap();
  private final ConcurrentMap<WorkflowId, WorkflowState> workflowStatePerWorkflowId = Maps.newConcurrentMap();

  public final List<SequenceEvent> writtenEvents = Lists.newCopyOnWriteArrayList();
  public final Map<WorkflowInstance, Long> activeStatesMap = Maps.newHashMap();

  public final CountDownLatch countDown;

  public InMemStorage() {
    this(0);
  }

  public InMemStorage(int expectedWorkflowExecutionInfoStored) {
    this.countDown = new CountDownLatch(expectedWorkflowExecutionInfoStored);
  }

  @Override
  public boolean globalEnabled() {
    return globalEnabled;
  }

  @Override
  public boolean setGlobalEnabled(boolean enabled) {
    final boolean oldValue = globalEnabled();
    this.globalEnabled = enabled;
    return oldValue;
  }

  @Override
  public String globalDockerRunnerId() throws IOException {
    return "default";
  }

  @Override
  public void store(Workflow workflow) throws IOException {
    workflowStore.put(workflow.id(), workflow);
    components.add(workflow.id().componentId());
  }

  @Override
  public Optional<Workflow> workflow(WorkflowId workflowId) throws IOException {
    return Optional.ofNullable(workflowStore.get(workflowId));
  }

  @Override
  public void delete(WorkflowId workflowId) throws IOException {
    throw new UnsupportedOperationException("Unsupported Operation!");
  }

  @Override
  public void updateNextNaturalTrigger(WorkflowId workflowId, Instant nextNaturalTrigger) throws IOException {
    throw new UnsupportedOperationException("Unsupported Operation!");
  }

  @Override
  public Map<Workflow, Optional<Instant>> workflowsWithNextNaturalTrigger()
      throws IOException {
    throw new UnsupportedOperationException("Unsupported Operation!");
  }

  @Override
  public WorkflowInstanceExecutionData executionData(WorkflowInstance workflowInstance) throws IOException {
    throw new UnsupportedOperationException("Unsupported Operation!");
  }

  @Override
  public List<WorkflowInstanceExecutionData> executionData(WorkflowId workflowId, String offset,
                                                           int limit) throws IOException {
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

    patchState.dockerImage().ifPresent(image -> dockerImagesPerWorkflowId.put(workflowId, image));
    Optional<WorkflowState> originalState = Optional.of(
        workflowStatePerWorkflowId.getOrDefault(workflowId, patchState));
    workflowStatePerWorkflowId.put(workflowId, WorkflowStateUtil.patchWorkflowState(originalState, patchState));
  }

  @Override
  public void patchState(String componentId, WorkflowState state) throws IOException {

    if (!components.contains(componentId)) {
      throw new ResourceNotFoundException("Component not found");
    }

    if (state.dockerImage().isPresent()) {
      dockerImagesPerComponent.put(componentId, state.dockerImage().get());
    }
  }

  @Override
  public Optional<String> getDockerImage(WorkflowId workflowId) throws IOException {
    if (dockerImagesPerWorkflowId.containsKey(workflowId)) {
      return Optional.of(dockerImagesPerWorkflowId.get(workflowId));
    }

    if (dockerImagesPerComponent.containsKey(workflowId.componentId())) {
      return Optional.of(dockerImagesPerComponent.get(workflowId.componentId()));
    }

    return Optional.ofNullable(workflowStore.get(workflowId))
        .flatMap(w -> w.schedule().dockerImage());
  }

  @Override
  public WorkflowState workflowState(WorkflowId workflowId) throws IOException {
    return
        workflowStatePerWorkflowId.getOrDefault(
            workflowId,
            WorkflowState.patchEnabled(false));
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
    activeStatesMap.computeIfPresent(sequenceEvent.event().workflowInstance(), (k, v) -> v + 1);
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
  public void writeActiveState(WorkflowInstance workflowInstance, long counter) {
    activeStatesMap.put(workflowInstance, counter);
  }

  @Override
  public void deleteActiveState(WorkflowInstance workflowInstance) {
    activeStatesMap.remove(workflowInstance);
  }

  @Override
  public Map<WorkflowInstance, Long> readActiveWorkflowInstances() throws IOException {
    return activeStatesMap;
  }

  @Override
  public Map<WorkflowInstance, Long> readActiveWorkflowInstances(String componentId) throws IOException {
    return activeStatesMap.entrySet().stream()
        .filter((entry) -> componentId.equals(entry.getKey().workflowId().componentId()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public Optional<Long> getCounterFromActiveStates(WorkflowInstance workflowInstance) throws IOException {
    return Optional.ofNullable(activeStatesMap.get(workflowInstance));
  }
}
