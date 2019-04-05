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

import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.Shard;
import com.spotify.styx.util.ShardedCounter;
import com.spotify.styx.util.TriggerInstantSpec;
import java.io.IOException;
import java.util.Optional;

/**
 * The interface to the persistence layer where the same transaction is used across storage
 * operations.
 *
 * <p>Use the {@link Storage#runInTransactionWithRetries(TransactionFunction)} method for automatic
 * commit/rollback handling.
 */
public interface StorageTransaction {

  /**
   * Stores a Workflow definition.
   *
   * @param workflow the workflow to store
   */
  WorkflowId store(Workflow workflow) throws IOException;

  /**
   * Stores a Workflow definition with a next natural trigger.
   *
   * @param workflow the workflow to store
   */
  WorkflowId storeWorkflowWithNextNaturalTrigger(Workflow workflow, TriggerInstantSpec triggerInstantSpec)
      throws IOException;

  /**
   * Deletes a workflow.
   *
   * @param workflowId The workflow to delete
   * @return The id of the deleted workflow.
   */
  WorkflowId deleteWorkflow(WorkflowId workflowId) throws IOException;

  /**
   * Get a {@link Workflow} definition.
   *
   * @param workflowId  The workflow to get
   * @return Optionally a workflow, if one was found for the given id
   */
  Optional<Workflow> workflow(WorkflowId workflowId) throws IOException;

  /**
   * Get a {@link Backfill}.
   * @param id Id of the backfill
   * @return Optionally a backfill, if one was found for the given id
   */
  Optional<Backfill> backfill(String id) throws IOException;

  /**
   * Updates the next natural trigger for a {@link Workflow}.
   *
   * @param workflowId  The {@link WorkflowId} to update the next natural trigger for.
   * @param triggerSpec The next natural trigger spec describing when the {@link Workflow} should
   *                    be instantiated.
   */
  WorkflowId updateNextNaturalTrigger(WorkflowId workflowId, TriggerInstantSpec triggerSpec) throws IOException;

  /**
   * Patches the workflow state used by all its {@link Workflow}s.
   *
   * <p>All the present fields in {@link WorkflowState} will be modified atomically.
   *
   * @param workflowId  The workflow to set the flag for
   * @param state       The state object with optional fields to patch
   */
  WorkflowId patchState(WorkflowId workflowId, WorkflowState state) throws IOException;

  /**
   * Read an active workflow instance state.
   */
  Optional<RunState> readActiveState(WorkflowInstance instance) throws IOException;

  /**
   * Insert a new active workflow instance state. Fails if the state already exists.
   */
  WorkflowInstance writeActiveState(WorkflowInstance instance, RunState state)
      throws IOException;

  /**
   * Update an existing active workflow instance state. Fails if the state does not exist.
   */
  WorkflowInstance updateActiveState(WorkflowInstance instance, RunState state)
      throws IOException;

  /**
   * Remove an active workflow instance state.
   */
  WorkflowInstance deleteActiveState(WorkflowInstance instance) throws IOException;

  /**
   * Stores a backfill
   *
   * @param backfill the backfill to store
   */
  Backfill store(Backfill backfill) throws IOException;

  /**
   * Update counter by delta for the specified resource.
   */
  void updateCounter(ShardedCounter shardedCounter, String resource, int delta) throws IOException;

  /**
   * Reads a counter shard
   */
  Optional<Shard> shard(String counterId, int shardIndex) throws IOException;

  /**
   * Stores a shard
   */
  void store(Shard shard) throws IOException;

  /**
   * Updates the limit for the given counter
   */
  void updateLimitForCounter(String counterId, long limit) throws IOException;

  /**
   * Stores a resource
   */
  void store(Resource resource) throws IOException;
}
