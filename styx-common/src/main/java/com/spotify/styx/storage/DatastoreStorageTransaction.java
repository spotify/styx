/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.storage;

import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_ALL_TRIGGERED;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_COMPONENT;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_CONCURRENCY;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_DESCRIPTION;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_END;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_HALTED;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_NEXT_NATURAL_OFFSET_TRIGGER;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_NEXT_NATURAL_TRIGGER;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_NEXT_TRIGGER;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_REVERSE;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_SCHEDULE;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_START;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_TRIGGER_PARAMETERS;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_WORKFLOW;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_WORKFLOW_ENABLED;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_WORKFLOW_JSON;
import static com.spotify.styx.storage.DatastoreStorage.activeWorkflowInstanceIndexShardEntryKey;
import static com.spotify.styx.storage.DatastoreStorage.activeWorkflowInstanceIndexShardEntryKeyShifted;
import static com.spotify.styx.storage.DatastoreStorage.activeWorkflowInstanceKey;
import static com.spotify.styx.storage.DatastoreStorage.entityToBackfill;
import static com.spotify.styx.storage.DatastoreStorage.entityToRunState;
import static com.spotify.styx.storage.DatastoreStorage.instantToTimestamp;
import static com.spotify.styx.storage.DatastoreStorage.isBrokenShard;
import static com.spotify.styx.storage.DatastoreStorage.runStateToEntity;
import static com.spotify.styx.util.ShardedCounter.KIND_COUNTER_LIMIT;
import static com.spotify.styx.util.ShardedCounter.KIND_COUNTER_SHARD;
import static com.spotify.styx.util.ShardedCounter.PROPERTY_COUNTER_ID;
import static com.spotify.styx.util.ShardedCounter.PROPERTY_LIMIT;
import static com.spotify.styx.util.ShardedCounter.PROPERTY_SHARD_INDEX;
import static com.spotify.styx.util.ShardedCounter.PROPERTY_SHARD_VALUE;

import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Entity.Builder;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.StringValue;
import com.google.common.annotations.VisibleForTesting;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.ResourceNotFoundException;
import com.spotify.styx.util.Shard;
import com.spotify.styx.util.ShardedCounter;
import com.spotify.styx.util.TriggerInstantSpec;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatastoreStorageTransaction implements StorageTransaction {

  private static final Logger LOG = LoggerFactory.getLogger(DatastoreStorageTransaction.class);

  private final CheckedDatastoreTransaction tx;

  public DatastoreStorageTransaction(CheckedDatastoreTransaction transaction) {
    this.tx = Objects.requireNonNull(transaction);
  }

  @Override
  public void commit() throws TransactionException {
    try {
      tx.commit();
    } catch (DatastoreIOException e) {
      throw new TransactionException(e.getCause());
    }
  }

  @Override
  public void rollback() throws TransactionException {
    try {
      tx.rollback();
    } catch (DatastoreIOException e) {
      throw new TransactionException(e.getCause());
    }
  }

  @Override
  public boolean isActive() {
    return tx.isActive();
  }

  @Override
  public void updateCounter(ShardedCounter shardedCounter, String resource, int delta) throws IOException {
    shardedCounter.updateCounter(this, resource, delta);
  }

  @Override
  public Optional<Shard> shard(String counterId, int shardIndex) throws IOException {
    // TODO there's no need for this to be transactional
    final Key shardKey = tx.getDatastore().newKeyFactory().setKind(KIND_COUNTER_SHARD)
        .newKey(counterId + "-" + shardIndex);
    Entity shardEntity = tx.get(shardKey);
    if (shardEntity == null) {
      return Optional.empty();
    }
    return Optional.of(Shard.create(counterId, shardIndex,
                                    (int) tx.get(shardKey).getLong(PROPERTY_SHARD_VALUE)));
  }

  @Override
  public void store(Shard shard) throws IOException {
    tx.put(Entity.newBuilder(tx.getDatastore().newKeyFactory().setKind(KIND_COUNTER_SHARD)
                                 .newKey(shard.counterId() + "-" + shard.index()))
                        .set(PROPERTY_COUNTER_ID, shard.counterId())
                        .set(PROPERTY_SHARD_INDEX, shard.index())
                        .set(PROPERTY_SHARD_VALUE, shard.value())
                        .build());
  }

  @Override
  public void updateLimitForCounter(String counterId, long limit) throws IOException {
    final Key limitKey = tx.getDatastore().newKeyFactory().setKind(KIND_COUNTER_LIMIT).newKey(counterId);
    tx.put(Entity.newBuilder(limitKey).set(PROPERTY_LIMIT, limit).build());
  }

  @Override
  public void store(Resource resource) throws IOException {
    tx.put(resourceToEntity(tx.getDatastore(), resource));
  }

  private Entity resourceToEntity(CheckedDatastore datastore, Resource resource) {
    final Key key = datastore.newKeyFactory().setKind(KIND_COUNTER_LIMIT).newKey(resource.id());
    return Entity.newBuilder(key)
        .set(PROPERTY_LIMIT, resource.concurrency())
        .build();
  }

  @Override
  public WorkflowId store(Workflow workflow) throws IOException {
    final Key componentKey = DatastoreStorage.componentKey(tx.getDatastore().newKeyFactory(), workflow.componentId());
    if (tx.get(componentKey) == null) {
      tx.put(Entity.newBuilder(componentKey).build());
    }

    final String json = OBJECT_MAPPER.writeValueAsString(workflow);
    final Key workflowKey = DatastoreStorage.workflowKey(tx.getDatastore().newKeyFactory(), workflow.id());
    final Optional<Entity> workflowOpt = DatastoreStorage.getOpt(tx, workflowKey);
    final Entity workflowEntity = DatastoreStorage.asBuilderOrNew(workflowOpt, workflowKey)
        .set(PROPERTY_WORKFLOW_JSON,
            StringValue.newBuilder(json).setExcludeFromIndexes(true).build())
        .build();

    tx.put(workflowEntity);

    return workflow.id();
  }

  @Override
  public WorkflowId storeWorkflowWithNextNaturalTrigger(Workflow workflow, TriggerInstantSpec triggerSpec)
      throws IOException {

    final Key componentKey = DatastoreStorage.componentKey(tx.getDatastore().newKeyFactory(), workflow.componentId());
    if (tx.get(componentKey) == null) {
      tx.put(Entity.newBuilder(componentKey).build());
    }

    final String json = OBJECT_MAPPER.writeValueAsString(workflow);
    final Key workflowKey = DatastoreStorage.workflowKey(tx.getDatastore().newKeyFactory(), workflow.id());
    final Optional<Entity> workflowOpt = DatastoreStorage.getOpt(tx, workflowKey);
    final Builder entity = DatastoreStorage.asBuilderOrNew(workflowOpt, workflowKey)
        .set(PROPERTY_WORKFLOW_JSON, StringValue.newBuilder(json).setExcludeFromIndexes(true).build())
        .set(PROPERTY_NEXT_NATURAL_TRIGGER, instantToTimestamp(triggerSpec.instant()))
        .set(PROPERTY_NEXT_NATURAL_OFFSET_TRIGGER, instantToTimestamp(triggerSpec.offsetInstant()));
    tx.put(entity.build());

    return workflow.id();

  }

  @Override
  public Optional<Workflow> workflow(WorkflowId workflowId) throws IOException {
    final Optional<Entity> entityOptional =
        DatastoreStorage.getOpt(tx, DatastoreStorage.workflowKey(tx.getDatastore().newKeyFactory(), workflowId));
    if (entityOptional.isPresent()) {
      return Optional.of(DatastoreStorage.parseWorkflowJson(entityOptional.get(), workflowId));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public WorkflowId updateNextNaturalTrigger(WorkflowId workflowId, TriggerInstantSpec triggerSpec) throws IOException {
    final Key workflowKey = DatastoreStorage
        .workflowKey(tx.getDatastore().newKeyFactory(), workflowId);
    final Optional<Entity> workflowOpt = DatastoreStorage.getOpt(tx, workflowKey);
    if (!workflowOpt.isPresent()) {
      throw new ResourceNotFoundException(
          String.format("%s:%s doesn't exist.", workflowId.componentId(), workflowId.id()));
    }

    final Entity.Builder builder = Entity
        .newBuilder(workflowOpt.get())
        .set(PROPERTY_NEXT_NATURAL_TRIGGER, instantToTimestamp(triggerSpec.instant()))
        .set(PROPERTY_NEXT_NATURAL_OFFSET_TRIGGER, instantToTimestamp(triggerSpec.offsetInstant()));
    tx.put(builder.build());

    return workflowId;
  }

  @Override
  public WorkflowId patchState(WorkflowId workflowId, WorkflowState state) throws IOException {
    final Key workflowKey = DatastoreStorage
        .workflowKey(tx.getDatastore().newKeyFactory(), workflowId);
    final Optional<Entity> workflowOpt = DatastoreStorage.getOpt(tx, workflowKey);
    if (!workflowOpt.isPresent()) {
      throw new ResourceNotFoundException(
          String.format("%s:%s doesn't exist.", workflowId.componentId(), workflowId.id()));
    }

    final Entity.Builder builder = Entity.newBuilder(workflowOpt.get());
    state.enabled().ifPresent(x -> builder.set(PROPERTY_WORKFLOW_ENABLED, x));
    state.nextNaturalTrigger()
        .ifPresent(x -> builder.set(PROPERTY_NEXT_NATURAL_TRIGGER, instantToTimestamp(x)));
    state.nextNaturalOffsetTrigger()
        .ifPresent(x -> builder.set(PROPERTY_NEXT_NATURAL_OFFSET_TRIGGER, instantToTimestamp(x)));
    tx.put(builder.build());

    return workflowId;
  }

  @Override
  public Optional<RunState> readActiveState(WorkflowInstance instance) throws IOException {
    final Entity entity = tx.get(activeWorkflowInstanceKey(tx.getDatastore().newKeyFactory(), instance));
    if (entity == null) {
      return Optional.empty();
    } else {
      return Optional.of(entityToRunState(entity, instance));
    }
  }

  @Override
  public WorkflowInstance writeActiveState(WorkflowInstance instance, RunState state)
      throws IOException {
    // Note: the parent entity need not actually exist
    final Key indexEntryKey = activeWorkflowInstanceIndexShardEntryKey(tx.getDatastore().newKeyFactory(), instance);
    final Entity indexEntry = Entity.newBuilder(indexEntryKey).build();
    tx.add(indexEntry);
    tx.add(runStateToEntity(tx.getDatastore().newKeyFactory(), instance, state));
    return instance;
  }

  @VisibleForTesting
  WorkflowInstance writeActiveStateShifted(WorkflowInstance instance, RunState state)
      throws IOException {
    // Note: the parent entity need not actually exist
    final Key indexEntryKey = activeWorkflowInstanceIndexShardEntryKeyShifted(tx.getDatastore().newKeyFactory(), instance);
    final Entity indexEntry = Entity.newBuilder(indexEntryKey).build();
    tx.add(indexEntry);
    tx.add(runStateToEntity(tx.getDatastore().newKeyFactory(), instance, state));
    return instance;
  }

  @Override
  public WorkflowInstance updateActiveState(WorkflowInstance instance, RunState state)
      throws IOException {
    tx.update(runStateToEntity(tx.getDatastore().newKeyFactory(), instance, state));
    return instance;
  }

  @Override
  public WorkflowInstance deleteActiveState(WorkflowInstance instance) throws IOException {
    final Key indexShardEntryKey = activeWorkflowInstanceIndexShardEntryKey(tx.getDatastore().newKeyFactory(), instance);
    tx.delete(indexShardEntryKey);

    final String name = indexShardEntryKey.getParent().getName();
    if (isBrokenShard(name)) {
      LOG.debug("workflow instance {} is mapped to shard {}, so deleting from shifted shard as well",
          instance.toKey(), name);

      final Key indexShardEntryKeyShifted = activeWorkflowInstanceIndexShardEntryKeyShifted(
          tx.getDatastore().newKeyFactory(), instance);
      tx.delete(indexShardEntryKeyShifted);
    }

    tx.delete(activeWorkflowInstanceKey(tx.getDatastore().newKeyFactory(), instance));
    return instance;
  }

  @Override
  public Backfill store(Backfill backfill) throws IOException {
    final Key key = DatastoreStorage.backfillKey(tx.getDatastore().newKeyFactory(), backfill.id());
    Entity.Builder builder = Entity.newBuilder(key)
        .set(PROPERTY_CONCURRENCY, backfill.concurrency())
        .set(PROPERTY_START, instantToTimestamp(backfill.start()))
        .set(PROPERTY_END, instantToTimestamp(backfill.end()))
        .set(PROPERTY_COMPONENT, backfill.workflowId().componentId())
        .set(PROPERTY_WORKFLOW, backfill.workflowId().id())
        .set(PROPERTY_SCHEDULE, backfill.schedule().toString())
        .set(PROPERTY_NEXT_TRIGGER, instantToTimestamp(backfill.nextTrigger()))
        .set(PROPERTY_ALL_TRIGGERED, backfill.allTriggered())
        .set(PROPERTY_HALTED, backfill.halted())
        .set(PROPERTY_REVERSE, backfill.reverse());

    backfill.description().ifPresent(x -> builder.set(PROPERTY_DESCRIPTION, StringValue
        .newBuilder(x).setExcludeFromIndexes(true).build()));

    if (backfill.triggerParameters().isPresent()) {
      final String json = OBJECT_MAPPER.writeValueAsString(backfill.triggerParameters().get());
      builder.set(PROPERTY_TRIGGER_PARAMETERS,
          StringValue.newBuilder(json).setExcludeFromIndexes(true).build());
    }

    tx.put(builder.build());

    return backfill;
  }

  @Override
  public Optional<Backfill> backfill(String id) throws IOException {
    final Key key = DatastoreStorage.backfillKey(tx.getDatastore().newKeyFactory(), id);
    final Entity entity = tx.get(key);
    if (entity == null) {
      return Optional.empty();
    }
    return Optional.of(entityToBackfill(entity));
  }
}
