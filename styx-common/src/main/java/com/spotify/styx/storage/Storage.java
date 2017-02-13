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

import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;

/**
 * The interface to the persistence layer.
 */
public interface Storage {

  /**
   * Returns all {@link SequenceEvent} for a {@link WorkflowInstance} in time order.
   *
   * @param workflowInstance  The workflow instance to get the events for
   */
  SortedSet<SequenceEvent> readEvents(WorkflowInstance workflowInstance) throws IOException;

  /**
   * Stores an {@link com.spotify.styx.model.Event}.
   *
   * @param sequenceEvent  The event together with its sequence number to be stored
   */
  void writeEvent(SequenceEvent sequenceEvent) throws IOException;

  /**
   * Returns the latest counter from the events of a {@link WorkflowInstance}. The returned
   * Optional is empty if no event is found for the {@link WorkflowInstance} specified.
   *
   * @param workflowInstance  The workflow instance to get the latest counter for
   */
  Optional<Long> getLatestStoredCounter(WorkflowInstance workflowInstance) throws IOException;

  /**
   * Get the global enabled flag for Styx.
   */
  boolean globalEnabled() throws IOException;

  /**
   * Get the debug flag for Styx.
   */
  boolean debugEnabled() throws IOException;

  /**
   * Set the global enabled flag for Styx.
   *
   * @param enabled     The global enabled flag
   * @return the previously stored global enable flag value
   */
  boolean setGlobalEnabled(boolean enabled) throws IOException;

  /**
   * Get the id of the current docker runner id
   */
  String globalDockerRunnerId() throws IOException;

  /**
   * Stores a Workflow definition.
   *
   * @param workflow the workflow to store
   */
  void storeWorkflow(Workflow workflow) throws IOException;

  /**
   * Get a {@link Workflow} definition.
   *
   * @param workflowId  The workflow to get
   * @return Optionally a workflow, if one was found for te given id
   */
  Optional<Workflow> workflow(WorkflowId workflowId) throws IOException;

  /**
   * Removes a workflow definition.
   *
   * @param workflowId The workflow id to remove.
   */
  void delete(WorkflowId workflowId) throws IOException;

  /**
   * Updates the next natural trigger for a {@link Workflow}.
   *
   * @param workflowId The {@link WorkflowId} to update the next natural trigger for.
   * @param nextNaturalTrigger The next natural trigger instant at which the {@link Workflow}
   *                           should be instantiated.
   */
  void updateNextNaturalTrigger(WorkflowId workflowId, Instant nextNaturalTrigger) throws IOException;

  /**
   * Get all {@link Workflow}s with their respective nextNaturalTrigger,
   * which is empty if it hasn't been initialized before.
   */
  Map<Workflow, Optional<Instant>> workflowsWithNextNaturalTrigger() throws IOException;

  /**
   * Stores information about an active {@link WorkflowInstance} to be tracked.
   *
   * @param workflowInstance  The {@link WorkflowInstance} that entered an active state
   * @param counter           The last processed event count for the {@link WorkflowInstance}
   */
  void writeActiveState(WorkflowInstance workflowInstance, long counter) throws IOException;

  /**
   * Removes a reference to active {@link WorkflowInstance}, to be called when the instance enters
   * a final state in Styx and it shouldn't be tracked anymore.
   *
   * @param workflowInstance  The {@link WorkflowInstance} that entered a final state
   */
  void deleteActiveState(WorkflowInstance workflowInstance) throws IOException;

  /**
   * Return a map of all active {@link WorkflowInstance}s to their last consumed sequence count.
   *
   * <p>A {@link WorkflowInstance} is active if there has been at least one call to
   *
   * {@link #writeActiveState(WorkflowInstance, long)} and no calls to
   * {@link #deleteActiveState(WorkflowInstance)}.
   *
   * @return The map of workflow instances to sequence counts
   */
  Map<WorkflowInstance, Long> readActiveWorkflowInstances() throws IOException;

  /**
   * Return a map of all active {@link WorkflowInstance}s to their last consumed sequence count,
   * for workflows that belong to a given component id.
   *
   * <p>A {@link WorkflowInstance} is active if there has been at least one call to
   *
   * {@link #writeActiveState(WorkflowInstance, long)} and no calls to
   * {@link #deleteActiveState(WorkflowInstance)}.
   *
   * @return The map of workflow instances to sequence counts
   */
  Map<WorkflowInstance, Long> readActiveWorkflowInstances(String componentId) throws IOException;

  /**
   * Get execution information for a {@link WorkflowInstance}.
   *
   * @param workflowInstance The workflow to get execution information for
   * @return the {@link WorkflowInstanceExecutionData} with execution information
   */
  WorkflowInstanceExecutionData executionData(WorkflowInstance workflowInstance) throws IOException;

  /**
   * Get execution information for all the {@link WorkflowInstance} of the specified {@link WorkflowId}.
   *
   * <p>Results can be paginated based on a offset {@link WorkflowInstance#parameter()} and a limit.
   *
   * @param workflowId  The workflowId to get execution information for
   * @param offset      The offset parameter
   * @param limit       Maximum number of results to return
   * @return A {@link WorkflowInstanceExecutionData} of all the instances
   */
  List<WorkflowInstanceExecutionData> executionData(WorkflowId workflowId, String offset, int limit)
      throws IOException;

  /**
   * Use workflowState instead.
   * Get enabled flag for a {@link Workflow}.
   *
   * @param workflowId  The workflow to get the flag for
   * @return true if the queried workflow is enabled
   */
  @Deprecated
  boolean enabled(WorkflowId workflowId) throws IOException;

  /**
   * Get set of of all enabled {@link Workflow}.
   *
   * @return the set of enabled {@link Workflow}
   */
  Set<WorkflowId> enabled() throws IOException;

  /**
   * Patches the workflow state used by all its {@link Workflow}s.
   *
   * <p>All the present fields in {@link WorkflowState} will be modified atomically.
   *
   * @param workflowId  The workflow to set the flag for
   * @param state       The state object with optional fields to patch
   */
  void patchState(WorkflowId workflowId, WorkflowState state) throws IOException;

  /**
   * Patches the component state used by all its {@link Workflow}s.
   *
   * <p>All the present fields in {@link WorkflowState} will be modified atomically.
   *
   * <p>That this method will ignore the {@link WorkflowState#enabled()} field since enable/disable
   * is only supported on the workflow level. See {@link #patchState(WorkflowId, WorkflowState)}.
   *
   * @param componentId  The component to patch the state for
   * @param state        The state object with optional fields to patch
   */
  void patchState(String componentId, WorkflowState state) throws IOException;

  /**
   * Use workflowState instead.
   * Get a docker image name.
   *
   * @param workflowId The workflow to get the image for
   * @return the full image name
   */
  @Deprecated
  Optional<String> getDockerImage(WorkflowId workflowId) throws IOException;

  /**
   * Get the persisted workflow state for a workflow.
   *
   * @param workflowId The workflow to get the repository for
   * @return workflow state.
   */
  WorkflowState workflowState(WorkflowId workflowId) throws IOException;

  Optional<Resource> resource(String id) throws IOException;

  void storeResource(Resource resource) throws IOException;

  List<Resource> resources() throws IOException;

  void deleteResource(String id) throws IOException;

  List<Backfill> backfills() throws IOException;

  Optional<Backfill> backfill(String id) throws IOException;

  void storeBackfill(Backfill backfill) throws IOException;
}
