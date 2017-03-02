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

import com.google.cloud.datastore.Datastore;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import org.apache.hadoop.hbase.client.Connection;

/**
 * A {@link Storage} implementation backed by Datastore and Bigtable
 */
public class AggregateStorage implements Storage {

  private final BigtableStorage bigtableStorage;
  private final DatastoreStorage datastoreStorage;

  public AggregateStorage(Connection connection, Datastore datastore, Duration retryBaseDelay) {
    this.bigtableStorage = new BigtableStorage(connection, retryBaseDelay);
    this.datastoreStorage = new DatastoreStorage(datastore, retryBaseDelay);
  }

  @Override
  public SortedSet<SequenceEvent> readEvents(WorkflowInstance workflowInstance) throws IOException {
    return bigtableStorage.readEvents(workflowInstance);
  }

  @Override
  public void writeEvent(SequenceEvent sequenceEvent) throws IOException {
    bigtableStorage.writeEvent(sequenceEvent);
  }

  @Override
  public Optional<Long> getLatestStoredCounter(WorkflowInstance workflowInstance)
      throws IOException {
    return bigtableStorage.getLatestStoredCounter(workflowInstance);
  }

  @Override
  public Optional<Long> globalConcurrency() throws IOException {
    return datastoreStorage.globalConcurrency();
  }

  @Override
  public Map<WorkflowInstance, Long> readActiveWorkflowInstances() throws IOException {
    return datastoreStorage.allActiveStates();
  }

  @Override
  public Map<WorkflowInstance, Long> readActiveWorkflowInstances(String componentId) throws IOException {
    return datastoreStorage.activeStates(componentId);
  }

  @Override
  public void writeActiveState(WorkflowInstance workflowInstance, long counter) throws IOException {
    datastoreStorage.writeActiveState(workflowInstance, counter);
  }

  @Override
  public void deleteActiveState(WorkflowInstance workflowInstance) throws IOException {
    datastoreStorage.deleteActiveState(workflowInstance);
  }

  @Override
  public boolean globalEnabled() throws IOException {
    return datastoreStorage.globalEnabled();
  }

  @Override
  public boolean debugEnabled() throws IOException {
    return datastoreStorage.debugEnabled();
  }

  @Override
  public Optional<Double> submissionRate() throws IOException {
    return datastoreStorage.submissionRate();
  }

  @Override
  public boolean setGlobalEnabled(boolean enabled) throws IOException {
    return datastoreStorage.setGlobalEnabled(enabled);
  }

  @Override
  public String globalDockerRunnerId() throws IOException {
    return datastoreStorage.globalDockerRunnerId();
  }

  @Override
  public boolean enabled(WorkflowId workflowId) throws IOException {
    return datastoreStorage.enabled(workflowId);
  }

  @Override
  public Set<WorkflowId> enabled() throws IOException {
    return datastoreStorage.enabled();
  }

  @Override
  public WorkflowInstanceExecutionData executionData(WorkflowInstance workflowInstance) throws IOException {
    return bigtableStorage.executionData(workflowInstance);
  }

  @Override
  public List<WorkflowInstanceExecutionData> executionData(WorkflowId workflowId, String offset,
                                                           int limit) throws IOException {
    return bigtableStorage.executionData(workflowId, offset, limit);
  }

  @Override
  public void storeWorkflow(Workflow workflow) throws IOException {
    datastoreStorage.store(workflow);
  }

  @Override
  public Optional<Workflow> workflow(WorkflowId workflowId) throws IOException {
    return datastoreStorage.workflow(workflowId);
  }

  @Override
  public void delete(WorkflowId workflowId) throws IOException {
    datastoreStorage.delete(workflowId);
  }

  @Override
  public void updateNextNaturalTrigger(WorkflowId workflowId, Instant nextNaturalTrigger) throws IOException {
    datastoreStorage.updateNextNaturalTrigger(workflowId, nextNaturalTrigger);
  }

  @Override
  public Map<Workflow, Optional<Instant>> workflowsWithNextNaturalTrigger()
      throws IOException {
    return datastoreStorage.workflowsWithNextNaturalTrigger();
  }

  @Override
  public void patchState(WorkflowId workflowId, WorkflowState state) throws IOException {
    datastoreStorage.patchState(workflowId, state);
  }

  @Override
  public void patchState(String componentId, WorkflowState state) throws IOException {
    datastoreStorage.patchState(componentId, state);
  }

  @Override
  public Optional<String> getDockerImage(WorkflowId workflowId) throws IOException {
    return datastoreStorage.getDockerImage(workflowId);
  }

  @Override
  public WorkflowState workflowState(WorkflowId workflowId) throws IOException {
    return datastoreStorage.workflowState(workflowId);
  }

  @Override
  public Optional<Resource> resource(String id) throws IOException {
    return datastoreStorage.getResource(id);
  }

  @Override
  public List<Resource> resources() throws IOException {
    return datastoreStorage.getResources();
  }

  @Override
  public void deleteResource(String id) throws IOException {
    datastoreStorage.deleteResource(id);
  }

  @Override
  public void storeBackfill(Backfill backfill) throws IOException {
    datastoreStorage.storeBackfill(backfill);
  }

  @Override
  public void storeResource(Resource resource) throws IOException {
    datastoreStorage.postResource(resource);
  }

  @Override
  public List<Backfill> backfills() throws IOException {
    return datastoreStorage.getBackfills();
  }

  @Override
  public Optional<Backfill> backfill(String id) throws IOException {
    return datastoreStorage.getBackfill(id);
  }
}
