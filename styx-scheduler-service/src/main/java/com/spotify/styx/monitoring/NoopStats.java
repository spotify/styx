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

import com.codahale.metrics.Gauge;
import com.spotify.styx.model.WorkflowId;

final class NoopStats implements Stats {

  @Override
  public void storageOperation(String operation, long durationMillis) {
  }

  @Override
  public void dockerOperation(String operation, long durationMillis) {
  }

  @Override
  public void submitToRunningTime(long durationSeconds) {
  }

  @Override
  public void registerQueuedEvents(Gauge<Long> queuedEventsCount) {
  }

  @Override
  public void registerActiveStates(Gauge<Long> activeStatesCount) {
  }

  @Override
  public void registerActiveStates(WorkflowId workflowId, Gauge<Long> activeStatesCount) {
  }

  @Override
  public void registerWorkflowCount(String status, Gauge<Long> workflowCount) {
  }

  @Override
  public void pullImageError() {
  }
}
