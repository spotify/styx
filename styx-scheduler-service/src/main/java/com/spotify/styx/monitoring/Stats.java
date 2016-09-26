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

/**
 * Interface for collecting statistics from throughout the Styx service
 */
public interface Stats {

  void storageOperation(String operation, long durationMillis);

  void dockerOperation(String operation, long durationMillis);

  void submitToRunningTime(long durationSeconds);

  void registerActiveStates(Gauge<Long> activeStatesCount);

  void registerActiveStates(WorkflowId workflowId, Gauge<Long> activeStatesCount);

  void registerWorkflowCount(String status, Gauge<Long> workflowCount);

  void pullImageError();

  Stats NOOP = new NoopStats();
}
