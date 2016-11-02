/*
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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Simple in memory implementation of {@link WorkflowCache}.
 */
public class InMemWorkflowCache implements WorkflowCache {

  private static final Logger LOG = LoggerFactory.getLogger(InMemWorkflowCache.class);

  private final ConcurrentMap<WorkflowId, Workflow> workflowStore = Maps.newConcurrentMap();

  @Override
  public void store(Workflow workflow) {
    LOG.info("Storing {}", workflow);
    workflowStore.put(workflow.id(), workflow);
  }

  @Override
  public Optional<Workflow> workflow(WorkflowId workflowId) {
    return Optional.ofNullable(workflowStore.get(workflowId));
  }

  @Override
  public ImmutableSet<Workflow> all() {
    return ImmutableSet.copyOf(workflowStore.values());
  }
}
