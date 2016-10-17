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
package com.spotify.styx.monitoring;

import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowExecutionInfo;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowInstanceExecutionData;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.Time;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public final class MeteredStorage extends MeteredBase implements Storage {

  private final Storage delegate;

  public MeteredStorage(Storage delegate, Stats stats, Time time) {
    super(stats, time);
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public boolean globalEnabled() throws IOException {
    return timedStorage("globalEnabled", delegate::globalEnabled);
  }

  @Override
  public boolean setGlobalEnabled(boolean enabled) throws IOException {
    return timedStorage("setGlobalEnabled", () -> delegate.setGlobalEnabled(enabled));
  }

  @Override
  public String globalDockerRunnerId() throws IOException {
    return timedStorage("globalDockerRunnerId", delegate::globalDockerRunnerId);
  }

  @Override
  public void store(Workflow workflow) throws IOException {
    timedStorage("storeWorkflow", () -> delegate.store(workflow));
  }

  @Override
  public Optional<Workflow> workflow(WorkflowId workflowId) throws IOException {
    return timedStorage("workflow", () -> delegate.workflow(workflowId));
  }

  @Override
  public WorkflowInstanceExecutionData executionData(WorkflowInstance workflowInstance) throws IOException {
    return timedStorage("executionData", () -> delegate.executionData(workflowInstance));
  }

  @Override
  public List<WorkflowInstanceExecutionData> executionData(WorkflowId workflowId)
      throws IOException {
    return timedStorage("executionData", () -> delegate.executionData(workflowId));
  }

  @Override
  public void store(WorkflowExecutionInfo workflowExecutionInfo) throws IOException {
    timedStorage("store", () -> delegate.store(workflowExecutionInfo));
  }

  @Override
  public Map<WorkflowInstance, List<WorkflowExecutionInfo>> getExecutionInfo(
      WorkflowId workflowId) throws IOException {
    return timedStorage("getExecutionInfo", () -> delegate.getExecutionInfo(workflowId));
  }

  @Override
  public List<WorkflowExecutionInfo> getExecutionInfo(WorkflowInstance workflowInstance)
      throws IOException {
    return timedStorage("getExecutionInfo", () -> delegate.getExecutionInfo(workflowInstance));
  }

  @Override
  public boolean enabled(WorkflowId workflowId) throws IOException {
    return timedStorage("enabled", () -> delegate.enabled(workflowId));
  }

  @Override
  public Set<WorkflowId> enabled() throws IOException {
    return timedStorage("enabled", () -> delegate.enabled());
  }

  @Override
  public void patchState(WorkflowId workflowId, WorkflowState state) throws IOException {
    timedStorage("patchState", () -> delegate.patchState(workflowId, state));
  }

  @Override
  public void patchState(String componentId, WorkflowState state) throws IOException {
    timedStorage("patchState", () -> delegate.patchState(componentId, state));
  }

  @Override
  public Optional<String> getDockerImage(WorkflowId workflowId) throws IOException {
    return timedStorage("getDockerImage", () -> delegate.getDockerImage(workflowId));
  }

  @Override
  public Optional<WorkflowState> workflowState(WorkflowId workflowId) throws IOException {
    return timedStorage("workflowState", () -> delegate.workflowState(workflowId));
  }
}
