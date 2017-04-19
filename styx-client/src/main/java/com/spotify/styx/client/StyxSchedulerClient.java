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

import com.spotify.styx.model.WorkflowInstance;
import java.util.concurrent.CompletionStage;

/**
 * Interface for Styx client, scheduler resources.
 */
public interface StyxSchedulerClient {

  /**
   * Trigger a {@link WorkflowInstance}
   *
   * @param componentId component id
   * @param workflowId  workflow id
   * @param parameter   parameter
   */
  CompletionStage<Void> triggerWorkflowInstance(final String componentId,
                                                final String workflowId,
                                                final String parameter);

  /**
   * Halt a {@link WorkflowInstance}
   *
   * @param componentId component id
   * @param workflowId  workflow id
   * @param parameter   parameter
   */
  CompletionStage<Void> haltWorkflowInstance(final String componentId,
                                             final String workflowId,
                                             final String parameter);

  /**
   * Retry a {@link WorkflowInstance}
   *
   * @param componentId component id
   * @param workflowId  workflow id
   * @param parameter   parameter
   */
  CompletionStage<Void> retryWorkflowInstance(final String componentId,
                                              final String workflowId,
                                              final String parameter);
}
