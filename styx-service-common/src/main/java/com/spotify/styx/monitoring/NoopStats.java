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

final class NoopStats implements Stats {

  @Override
  public void registerActiveStatesMetric(RunState.State state, String triggerName,
                                         Gauge<Long> activeStatesCount) {
    // nop
  }

  @Override
  public void registerWorkflowCountMetric(String status, Gauge<Long> workflowCount) {
    // nop
  }

  @Override
  public void registerSubmissionRateLimitMetric(Gauge<Double> submissionRateLimit) {
    // nop
  }

  @Override
  public void recordStorageOperation(String operation, long durationMillis, String status) {
    // nop
  }

  @Override
  public void recordDockerOperation(String operation, long durationMillis, String status) {
    // nop
  }

  @Override
  public void recordDockerOperationError(String operation, String type) {
    // nop
  }

  @Override
  public void recordSubmission(String executionId) {
    // nop
  }

  @Override
  public void recordRunning(String executionId) {
    // nop
  }

  @Override
  public void recordExitCode(int exitCode) {
    // nop
  }

  @Override
  public void recordPullImageError() {
    // nop
  }

  @Override
  public void recordNaturalTrigger() {
    // nop
  }

  @Override
  public void recordTerminationLogMissing() {
    // nop
  }

  @Override
  public void recordTerminationLogInvalid() {
    // nop
  }

  @Override
  public void recordExitCodeMismatch() {
    // nop
  }

  @Override
  public void recordResourceConfigured(String resource, long configured) {
    // nop
  }

  @Override
  public void recordResourceUsed(String resource, long used) {
    // nop
  }

  @Override
  public void recordResourceDemanded(String resource, long demanded) {
    // nop
  }

  @Override
  public void recordEventConsumer(SequenceEvent event) {
    // nop
  }

  @Override
  public void recordEventConsumerError(SequenceEvent event) {
    // nop
  }

  @Override
  public void recordWorkflowConsumer(String action) {
    // nop
  }

  @Override
  public void recordWorkflowConsumerError() {
    // nop
  }

  @Override
  public void recordPublishing(final String type, final String state) {
    // nop
  }

  @Override
  public void recordPublishingError(final String type, final String state) {
    // nop
  }

  @Override
  public void recordTickDuration(String type, long duration) {
    // nop
  }

  @Override
  public void recordDatastoreEntityReads(String kind, int n) {
    // nop
  }

  @Override
  public void recordDatastoreQueries(String kind, int n) {
    // nop
  }

  @Override
  public void recordDatastoreEntityWrites(String kind, int n) {
    // nop
  }

  @Override
  public void recordDatastoreEntityDeletes(String kind, int n) {
    // nop
  }

  @Override
  public void recordCounterCacheHit() {
    // nop
  }

  @Override
  public void recordCounterCacheMiss() {
    // nop
  }

  @Override
  public void recordKubernetesOperation(String operation, long durationMillis, String status) {
    // nop
  }

  @Override
  public void recordKubernetesOperationError(String operation, String type, int code) {
    // nop
  }
}
