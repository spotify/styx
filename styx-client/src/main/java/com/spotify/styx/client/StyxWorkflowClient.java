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
import com.spotify.styx.model.WorkflowState;
import java.util.concurrent.CompletionStage;

/**
 * Interface for Styx client, workflow resources.
 */
public interface StyxWorkflowClient {

  /**
   * Get a {@link Workflow}
   *
   * @param componentId componentId id
   * @param workflowId  workflowId id
   */
  CompletionStage<Workflow> workflow(final String componentId, final String workflowId);

  /**
   * Get a {@link WorkflowState}
   *
   * @param componentId componentId id
   * @param workflowId  workflowId id
   */
  CompletionStage<WorkflowState> workflowState(final String componentId, final String workflowId);
}
