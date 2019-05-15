/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2018 Spotify AB
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
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.state.RunState;

/**
 * Interface for collecting statistics from throughout the Styx service
 */
public interface Stats {

  Stats NOOP = new NoopStats();

  void registerActiveStatesMetric(RunState.State state, String triggerName, Gauge<Long> activeStatesCount);

  void registerWorkflowCountMetric(String status, Gauge<Long> workflowCount);

  void registerSubmissionRateLimitMetric(Gauge<Double> submissionRateLimit);

  void recordStorageOperation(String operation, long durationMillis, String status);

  void recordDockerOperation(String operation, long durationMillis, String status);

  void recordDockerOperationError(String operation, String type);

  void recordSubmission(String executionId);

  void recordRunning(String executionId);

  void recordExitCode(int exitCode);

  void recordPullImageError();

  void recordNaturalTrigger();

  void recordTerminationLogMissing();

  void recordTerminationLogInvalid();

  void recordExitCodeMismatch();

  void recordResourceConfigured(String resource, long configured);

  void recordResourceUsed(String resource, long used);

  void recordResourceDemanded(String resource, long demanded);

  void recordEventConsumer(SequenceEvent event);

  void recordEventConsumerError(SequenceEvent event);

  void recordWorkflowConsumer(String action);

  void recordWorkflowConsumerError();

  void recordPublishing(String type, String state);

  void recordPublishingError(String type, String state);

  void recordTickDuration(String type, long duration);

  void recordDatastoreEntityReads(String kind, int n);

  void recordDatastoreEntityWrites(String kind, int n);

  void recordDatastoreEntityDeletes(String kind, int n);

  void recordDatastoreQueries(String kind, int n);

  void recordCounterCacheHit();

  void recordCounterCacheMiss();

  void recordKubernetesOperation(String operation, long durationMillis, String status);

  void recordKubernetesOperationError(String operation, String type, int code);
}
