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

import com.codahale.metrics.Gauge;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.state.RunState;

final class NoopStats implements Stats {

  @Override
  public void registerQueuedEventsMetric(Gauge<Long> queuedEventsCount) {
  }

  @Override
  public void registerActiveStatesMetric(RunState.State state, String triggerName,
                                         Gauge<Long> activeStatesCount) {
  }

  @Override
  public void registerActiveStatesMetric(WorkflowId workflowId, Gauge<Long> activeStatesCount) {
  }

  @Override
  public void registerWorkflowCountMetric(String status, Gauge<Long> workflowCount) {
  }

  @Override
  public void registerSubmissionRateLimitMetric(Gauge<Double> submissionRateLimit) {
  }

  @Override
  public void recordStorageOperation(String operation, long durationMillis, String status) {
  }

  @Override
  public void recordDockerOperation(String operation, long durationMillis, String status) {
  }

  @Override
  public void recordDockerOperationError(String operation, String type, int code, long durationMillis) {
  }

  @Override
  public void recordSubmitToRunningTime(long durationSeconds) {
  }

  @Override
  public void recordExitCode(WorkflowId workflowId, int exitCode) {
  }

  @Override
  public void recordPullImageError() {
  }

  @Override
  public void recordNaturalTrigger() {
  }

  @Override
  public void recordTerminationLogMissing() {
  }

  @Override
  public void recordTerminationLogInvalid() {
  }

  @Override
  public void recordExitCodeMismatch() {
  }

  @Override
  public void recordResourceConfigured(String resource, long configured) {
  }

  @Override
  public void recordResourceUsed(String resource, long used) {
  }

  @Override
  public void recordEventConsumer(SequenceEvent event) {
  }

  @Override
  public void recordEventConsumerError(SequenceEvent event) {
  }

  @Override
  public void recordNewWorkflow() {
  }

  @Override
  public void recordUpdatedWorkflow() {
  }

  @Override
  public void recordRemovedWorkflow() {
  }

  @Override
  public void recordWorkflowConsumerError() {
  }
}
