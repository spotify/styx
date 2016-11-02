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

package com.spotify.styx.monitoring;

import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.storage.EventStorage;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.SortedSet;

public final class MeteredEventStorage extends MeteredBase implements EventStorage {

  private final EventStorage delegate;

  public MeteredEventStorage(EventStorage delegate, Stats stats, Time time) {
    super(stats, time);
    this.delegate = Objects.requireNonNull(delegate);
  }

  @Override
  public SortedSet<SequenceEvent> readEvents(WorkflowInstance workflowInstance) throws IOException {
    return timedStorage("readEvents", () -> delegate.readEvents(workflowInstance));
  }

  @Override
  public void writeEvent(SequenceEvent sequenceEvent) throws IOException {
    timedStorage("writeEvent", () -> delegate.writeEvent(sequenceEvent));
  }

  @Override
  public Optional<Long> getLatestStoredCounter(WorkflowInstance workflowInstance)
      throws IOException {
    return timedStorage("getLatestStoredCounter", () -> delegate.getLatestStoredCounter(workflowInstance));
  }

  @Override
  public void writeActiveState(WorkflowInstance workflowInstance, long counter) throws IOException {
    timedStorage("writeActiveState", () -> delegate.writeActiveState(workflowInstance, counter));
  }

  @Override
  public void deleteActiveState(WorkflowInstance workflowInstance) throws IOException {
    timedStorage("deleteActiveState", () -> delegate.deleteActiveState(workflowInstance));
  }

  @Override
  public Map<WorkflowInstance, Long> readActiveWorkflowInstances() throws IOException {
    return timedStorage("readActiveWorkflowInstances", () -> delegate.readActiveWorkflowInstances());
  }

  @Override
  public Map<WorkflowInstance, Long> readActiveWorkflowInstances(String componentId) throws IOException {
    return timedStorage("readActiveWorkflowInstances", () -> delegate.readActiveWorkflowInstances(componentId));
  }
}
