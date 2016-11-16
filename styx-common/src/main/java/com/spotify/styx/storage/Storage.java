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

import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowExecutionInfo;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowInstanceExecutionData;
import com.spotify.styx.model.WorkflowState;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The interface to the persistence layer.
 */
public interface Storage {

  /**
   * Get the global enabled flag for Styx.
   */
  boolean globalEnabled() throws IOException;

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
  void store(Workflow workflow) throws IOException;

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
   * @param workflowId The workflowid to remove.
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
   * Get execution information for a {@link WorkflowInstance}.
   *
   * @param workflowInstance The workflow to get execution information for
   * @return the {@link WorkflowInstanceExecutionData} with execution information
   */
  WorkflowInstanceExecutionData executionData(WorkflowInstance workflowInstance) throws IOException;

  /**
   * Get execution information for all the {@link WorkflowInstance} of the specified {@link WorkflowId}.
   *
   * @param workflowId  The workflowId to get execution information for
   * @return A {@link WorkflowInstanceExecutionData} of all the instances
   */
  List<WorkflowInstanceExecutionData> executionData(WorkflowId workflowId) throws IOException;

  /**
   * Stores a WorkflowExecutionInfo instance for later retrieval.
   *
   * @param workflowExecutionInfo the info to store
   */
  void store(WorkflowExecutionInfo workflowExecutionInfo) throws IOException;

  /**
   * Returns all statuses.
   */
  Map<WorkflowInstance, List<WorkflowExecutionInfo>> getExecutionInfo(WorkflowId workflowId)
      throws IOException;

  /**
   * Returns all statuses in time order from earliest to latest.
   */
  List<WorkflowExecutionInfo> getExecutionInfo(WorkflowInstance workflowInstance)
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
  Optional<WorkflowState> workflowState(WorkflowId workflowId) throws IOException;
}
