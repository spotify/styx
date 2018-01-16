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
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.util.TriggerInstantSpec;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.client.Connection;

/**
 * A {@link Storage} implementation backed by Datastore and Bigtable
 */
public class AggregateStorage implements Storage {

  private final BigtableStorage bigtableStorage;
  private final DatastoreStorage datastoreStorage;
  private final Supplier<DatastoreTransactionalStorage> transactionSupplier;

  public AggregateStorage(Connection connection, Datastore datastore, Duration retryBaseDelay) {
    this.bigtableStorage = new BigtableStorage(connection, retryBaseDelay);
    this.datastoreStorage = new DatastoreStorage(datastore, retryBaseDelay);
    this.transactionSupplier =  () -> new DatastoreTransactionalStorage(datastore.newTransaction());
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
  public StyxConfig config() throws IOException {
    return datastoreStorage.config();
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
  public List<WorkflowInstanceExecutionData> executionData(WorkflowId workflowId, String start,
                                                           String stop) throws IOException {
    return bigtableStorage.executionData(workflowId, start, stop);
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
  public List<Workflow> workflows(String componentId) throws IOException {
    return datastoreStorage.workflows(componentId);
  }

  @Override
  public void delete(WorkflowId workflowId) throws IOException {
    datastoreStorage.delete(workflowId);
  }

  @Override
  public void updateNextNaturalTrigger(WorkflowId workflowId, TriggerInstantSpec triggerSpec)
      throws IOException {
    datastoreStorage.updateNextNaturalTrigger(workflowId, triggerSpec);
  }

  @Override
  public void updateNextNaturalTriggerOld(WorkflowId workflowId, Instant instant) throws IOException {
    datastoreStorage.updateNextNaturalTrigger(workflowId, instant);
  }

  @Override
  public Map<Workflow, TriggerInstantSpec> workflowsWithNextNaturalTrigger() throws IOException {
    return datastoreStorage.workflowsWithNextNaturalTrigger();
  }

  @Override
  public Map<WorkflowId, Workflow> workflows() throws IOException {
    return datastoreStorage.workflows();
  }

  @Override
  public void patchState(WorkflowId workflowId, WorkflowState state) throws IOException {
    datastoreStorage.patchState(workflowId, state);
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
  public List<Backfill> backfills(boolean showAll) throws IOException {
    return datastoreStorage.getBackfills(showAll);
  }

  @Override
  public List<Backfill> backfillsForComponent(boolean showAll, String component) throws IOException {
    return datastoreStorage.getBackfillsForComponent(showAll, component);
  }

  @Override
  public List<Backfill> backfillsForWorkflow(boolean showAll, String workflow) throws IOException {
    return datastoreStorage.getBackfillsForWorkflow(showAll, workflow);
  }

  @Override
  public List<Backfill> backfillsForWorkflowId(boolean showAll, WorkflowId workflowId) throws IOException {
    return datastoreStorage.getBackfillsForWorkflowId(showAll, workflowId);
  }

  @Override
  public Optional<Backfill> backfill(String id) throws IOException {
    return datastoreStorage.getBackfill(id);
  }

  @Override
  public TransactionalStorage newTransactionalStorage() {
    return transactionSupplier.get();
  }
}
