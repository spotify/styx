/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx;

import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import java.util.Map;
import java.util.Optional;

/**
 * Simple caching interface for {@link Workflow}s
 */
public interface WorkflowCache {

  /**
   * Get a stored {@link Workflow}.
   *
   * @param workflowId  Id of the workflow to get
   * @return Optionally a Workflow, if found
   */
  Optional<Workflow> workflow(WorkflowId workflowId);

  /**
   * Get all stored {@link Workflow}s.
   *
   * @return a map of all Workflows
   */
  Map<WorkflowId, Workflow> all();
}
