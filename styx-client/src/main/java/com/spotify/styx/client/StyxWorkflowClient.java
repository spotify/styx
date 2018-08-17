/*-
 * -\-\-
 * styx-client
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

package com.spotify.styx.client;

import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Interface for Styx client, workflow resources.
 */
public interface StyxWorkflowClient {

  /**
   * Get a {@link Workflow}
   *
   * @param componentId component id
   * @param workflowId  workflow id
   * @return the {@link Workflow}
   */
  CompletionStage<Workflow> workflow(final String componentId, final String workflowId);

  /**
   * Get all {@link Workflow}s
   *
   * @return all {@link Workflow}s
   */
  CompletionStage<List<Workflow>> workflows();

  /**
   * Create or update a workflow
   *
   * @param componentId    component id
   * @param workflowConfig workflow configuration
   * @return the created {@link Workflow}
   */
  CompletionStage<Workflow> createOrUpdateWorkflow(String componentId,
                                                   WorkflowConfiguration workflowConfig);

  /**
   * Delete a {@link Workflow}
   *
   * @param componentId component id
   * @param workflowId  workflow id
   */
  CompletionStage<Void> deleteWorkflow(String componentId, String workflowId);

  /**
   * Get a {@link WorkflowState}
   *
   * @param componentId component id
   * @param workflowId  workflow id
   * @return the {@link WorkflowState}
   */
  CompletionStage<WorkflowState> workflowState(final String componentId, final String workflowId);

  /**
   * Get execution data of an instance of a {@link Workflow}
   *
   * @param componentId component id
   * @param workflowId  workflow id
   * @param parameter   parameter
   * @return the {@link WorkflowInstanceExecutionData}
   */
  CompletionStage<WorkflowInstanceExecutionData> workflowInstanceExecutions(
      final String componentId,
      final String workflowId,
      final String parameter
  );

  /**
   * Update {@link WorkflowState}
   *
   * @param componentId   component id
   * @param workflowId    workflow id
   * @param workflowState workflow state
   * @return the updated {@link WorkflowState}
   */
  CompletionStage<WorkflowState> updateWorkflowState(final String componentId,
                                                     final String workflowId,
                                                     final WorkflowState workflowState);
}
