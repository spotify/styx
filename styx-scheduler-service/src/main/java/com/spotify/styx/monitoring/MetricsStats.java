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
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.EventUtil;
import com.spotify.styx.util.Time;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javaslang.Tuple;
import javaslang.Tuple2;
import javaslang.Tuple3;

public final class MetricsStats implements Stats {

  private static final String UNIT_SECOND = "s";
  private static final String UNIT_MILLISECOND = "ms";
  private static final MetricId BASE = MetricId.build("styx");

  static final MetricId QUEUED_EVENTS = BASE
      .tagged("what", "queued-events-count")
      .tagged("unit", "events");

  static final MetricId ACTIVE_STATES_PER_RUNSTATE_PER_TRIGGER = BASE
      .tagged("what", "active-states-per-runstate-per-trigger-count")
      .tagged("unit", "state");

  static final MetricId WORKFLOW_COUNT = BASE
      .tagged("what", "workflow-count")
      .tagged("unit", "workflow");

  static final MetricId RESOURCE_CONFIGURED = BASE
      .tagged("what", "resource-configured");

  static final MetricId RESOURCE_USED = BASE
      .tagged("what", "resource-used");

  static final MetricId EXIT_CODE_RATE = BASE
      .tagged("what", "exit-code-rate");

  static final MetricId STORAGE_DURATION = BASE
      .tagged("what", "storage-operation-duration")
      .tagged("unit", UNIT_MILLISECOND);

  static final String OPERATION = "operation";

  static final MetricId STORAGE_RATE = BASE
      .tagged("what", "storage-operation-rate")
      .tagged("unit", OPERATION);

  static final MetricId DOCKER_DURATION = BASE
      .tagged("what", "docker-operation-duration")
      .tagged("unit", UNIT_MILLISECOND);

  static final MetricId DOCKER_RATE = BASE
      .tagged("what", "docker-operation-rate")
      .tagged("unit", OPERATION);

  static final MetricId DOCKER_ERROR_RATE = BASE
      .tagged("what", "docker-operation-error-rate")
      .tagged("unit", OPERATION);

  static final MetricId TRANSITIONING_DURATION = BASE
      .tagged("what", "time-transitioning-between-submitted-running")
      .tagged("unit", UNIT_SECOND);

  static final MetricId PULL_IMAGE_ERROR_RATE = BASE
      .tagged("what", "pull-image-error-rate")
      .tagged("unit", "error");

  static final MetricId NATURAL_TRIGGER_RATE = BASE
      .tagged("what", "natural-trigger-rate")
      .tagged("unit", "trigger");

  static final MetricId TERMINATION_LOG_MISSING = BASE
      .tagged("what", "termination-log-missing");

  static final MetricId TERMINATION_LOG_INVALID = BASE
      .tagged("what", "termination-log-invalid");

  static final MetricId EXIT_CODE_MISMATCH = BASE
      .tagged("what", "exit-code-mismatch");

  static final MetricId SUBMISSION_RATE_LIMIT = BASE
      .tagged("what", "submission-rate-limit")
      .tagged("unit", "submission/s");

  static final MetricId EVENT_CONSUMER_RATE = BASE
      .tagged("what", "event-consumer-rate");

  static final MetricId EVENT_CONSUMER_ERROR_RATE = BASE
      .tagged("what", "event-consumer-error-rate")
      .tagged("unit", "error");

  static final MetricId WORKFLOW_CONSUMER_RATE = BASE
      .tagged("what", "workflow-consumer-rate");

  static final MetricId WORKFLOW_CONSUMER_ERROR_RATE = BASE
      .tagged("what", "workflow-consumer-error-rate")
      .tagged("unit", "error");

  private static final String STATUS = "status";

  private final SemanticMetricRegistry registry;
  private final Time time;

  private final Histogram submitToRunning;
  private final Meter pullImageErrorMeter;
  private final Meter naturalTrigger;
  private final Meter terminationLogMissing;
  private final Meter terminationLogInvalid;
  private final Meter exitCodeMismatch;
  private final Meter workflowConsumerErrorMeter;
  private final ConcurrentMap<String, Histogram> storageOperationHistograms;
  private final ConcurrentMap<String, Meter> storageOperationMeters;
  private final ConcurrentMap<String, Histogram> dockerOperationHistograms;
  private final ConcurrentMap<String, Meter> dockerOperationMeters;
  private final ConcurrentMap<WorkflowId, Gauge> activeStatesPerWorkflowGauges;
  private final ConcurrentMap<Tuple2<WorkflowId, Integer>, Meter> exitCodePerWorkflowMeters;
  private final ConcurrentMap<Tuple3<String, String, Integer>, Meter> dockerOperationErrorMeters;
  private final ConcurrentMap<String, Histogram> resourceConfiguredHistograms;
  private final ConcurrentMap<String, Histogram> resourceUsedHistograms;
  private final ConcurrentMap<String, Meter> eventConsumerErrorMeters;
  private final ConcurrentMap<String, Meter> eventConsumerMeters;
  private final ConcurrentMap<String, Meter> workflowConsumerMeters;

  /**
   * Submission timestamps (nanotime) keyed on execution id.
   */
  private final Cache<String, Long> submissionTimestamps = CacheBuilder.newBuilder()
      .maximumSize(100_000)
      .build();

  public MetricsStats(SemanticMetricRegistry registry, Time time) {
    this.registry = Objects.requireNonNull(registry);
    this.time = Objects.requireNonNull(time, "time");

    this.submitToRunning = registry.histogram(TRANSITIONING_DURATION);
    this.pullImageErrorMeter = registry.meter(PULL_IMAGE_ERROR_RATE);
    this.naturalTrigger = registry.meter(NATURAL_TRIGGER_RATE);
    this.terminationLogMissing = registry.meter(TERMINATION_LOG_MISSING);
    this.terminationLogInvalid = registry.meter(TERMINATION_LOG_INVALID);
    this.exitCodeMismatch = registry.meter(EXIT_CODE_MISMATCH);
    this.workflowConsumerErrorMeter = registry.meter(WORKFLOW_CONSUMER_ERROR_RATE);
    this.storageOperationHistograms = new ConcurrentHashMap<>();
    this.storageOperationMeters = new ConcurrentHashMap<>();
    this.dockerOperationHistograms = new ConcurrentHashMap<>();
    this.dockerOperationMeters = new ConcurrentHashMap<>();
    this.activeStatesPerWorkflowGauges = new ConcurrentHashMap<>();
    this.exitCodePerWorkflowMeters = new ConcurrentHashMap<>();
    this.dockerOperationErrorMeters = new ConcurrentHashMap<>();
    this.resourceConfiguredHistograms = new ConcurrentHashMap<>();
    this.resourceUsedHistograms = new ConcurrentHashMap<>();
    this.eventConsumerErrorMeters = new ConcurrentHashMap<>();
    this.eventConsumerMeters = new ConcurrentHashMap<>();
    this.workflowConsumerMeters = new ConcurrentHashMap<>();
  }

  @Override
  public void registerQueuedEventsMetric(Gauge<Long> queuedEventsCount) {
    registry.register(QUEUED_EVENTS, queuedEventsCount);
  }

  @Override
  public void registerActiveStatesMetric(RunState.State state, String triggerName,
                                         Gauge<Long> activeStatesCount) {
    registry.register(ACTIVE_STATES_PER_RUNSTATE_PER_TRIGGER.tagged(
        "state", state.name(), "trigger", triggerName), activeStatesCount);
  }

  @Override
  public void registerWorkflowCountMetric(String status, Gauge<Long> workflowCount) {
    registry.register(WORKFLOW_COUNT.tagged(STATUS, status), workflowCount);
  }

  @Override
  public void registerSubmissionRateLimitMetric(Gauge<Double> submissionRateLimit) {
    registry.register(SUBMISSION_RATE_LIMIT, submissionRateLimit);
  }

  @Override
  public void recordStorageOperation(String operation, long durationMillis, String status) {
    storageOpHistogram(operation, status).update(durationMillis);
    storageOpMeter(operation, status).mark();
  }

  @Override
  public void recordDockerOperation(String operation, long durationMillis, String status) {
    dockerOpHistogram(operation, status).update(durationMillis);
    dockerOpMeter(operation, status).mark();
  }

  @Override
  public void recordDockerOperationError(String operation, String type, int code, long durationMillis) {
    dockerOpErrorMeter(operation, type, code).mark();
  }

  @Override
  public void recordSubmission(String executionId) {
    submissionTimestamps.put(executionId, time.nanoTime());
  }

  @Override
  public void recordRunning(String executionId) {
    final Long submissionNanos = submissionTimestamps.getIfPresent(executionId);
    if (submissionNanos != null) {
      final long runningNanos = time.nanoTime();
      submissionTimestamps.invalidate(executionId);
      submitToRunning.update(TimeUnit.NANOSECONDS.toSeconds(runningNanos - submissionNanos));
    }
  }

  @Override
  public void recordExitCode(WorkflowId workflowId, int exitCode) {
    exitCodeMeter(workflowId, exitCode).mark();
  }

  @Override
  public void recordPullImageError() {
    pullImageErrorMeter.mark();
  }

  @Override
  public void recordNaturalTrigger() {
    naturalTrigger.mark();
  }

  @Override
  public void recordTerminationLogMissing() {
    terminationLogMissing.mark();
  }

  @Override
  public void recordTerminationLogInvalid() {
    terminationLogInvalid.mark();
  }

  @Override
  public void recordExitCodeMismatch() {
    exitCodeMismatch.mark();
  }

  @Override
  public void recordResourceConfigured(String resource, long configured) {
    resourceConfiguredHistogram(resource).update(configured);
  }

  @Override
  public void recordResourceUsed(String resource, long used) {
    resourceUsedHistogram(resource).update(used);
  }

  @Override
  public void recordEventConsumer(SequenceEvent event) {
    eventConsumerMeter(event).mark();
  }

  @Override
  public void recordEventConsumerError(SequenceEvent event) {
    eventConsumerErrorMeter(event).mark();
  }

  @Override
  public void recordWorkflowConsumer(String action) {
    workflowConsumerMeter(action).mark();
  }

  @Override
  public void recordWorkflowConsumerError() {
    workflowConsumerErrorMeter.mark();
  }

  private Meter exitCodeMeter(WorkflowId workflowId, int exitCode) {
    return exitCodePerWorkflowMeters
        .computeIfAbsent(Tuple.of(workflowId, exitCode), (tuple) ->
            registry.meter(EXIT_CODE_RATE.tagged(
                "component-id", tuple._1.componentId(),
                "workflow-id", tuple._1.id(),
                "exit-code", String.valueOf(tuple._2))));
  }

  private Meter dockerOpErrorMeter(String operation, String type, int code) {
    return dockerOperationErrorMeters
        .computeIfAbsent(Tuple.of(operation, type, code), (tuple) ->
            registry.meter(DOCKER_ERROR_RATE.tagged(
                "operation", tuple._1,
                "type", tuple._2,
                "code", String.valueOf(tuple._3))));
  }

  private Histogram storageOpHistogram(String operation, String status) {
    return storageOperationHistograms.computeIfAbsent(
        operation, (op) -> registry.histogram(STORAGE_DURATION.tagged(OPERATION, op, STATUS, status)));
  }

  private Meter storageOpMeter(String operation, String status) {
    return storageOperationMeters.computeIfAbsent(
        operation, (op) -> registry.meter(STORAGE_RATE.tagged(OPERATION, op, STATUS, status)));
  }

  private Histogram dockerOpHistogram(String operation, String status) {
    return dockerOperationHistograms.computeIfAbsent(
        operation, (op) -> registry.histogram(DOCKER_DURATION.tagged(OPERATION, op, STATUS, status)));
  }

  private Meter dockerOpMeter(String operation, String status) {
    return dockerOperationMeters.computeIfAbsent(
        operation, (op) -> registry.meter(DOCKER_RATE.tagged(OPERATION, op, STATUS, status)));
  }

  private Histogram resourceConfiguredHistogram(String resource) {
    return resourceConfiguredHistograms.computeIfAbsent(
        resource, (op) -> registry.histogram(RESOURCE_CONFIGURED.tagged("resource", resource)));
  }

  private Histogram resourceUsedHistogram(String resource) {
    return resourceUsedHistograms.computeIfAbsent(
        resource, (op) -> registry.histogram(RESOURCE_USED.tagged("resource", resource)));
  }

  private Meter eventConsumerMeter(SequenceEvent sequenceEvent) {
    final String eventType = EventUtil.name(sequenceEvent.event());
    return eventConsumerMeters.computeIfAbsent(
        eventType, (op) -> registry.meter(EVENT_CONSUMER_RATE.tagged("event-type", eventType)));
  }

  private Meter eventConsumerErrorMeter(SequenceEvent sequenceEvent) {
    final String eventType = EventUtil.name(sequenceEvent.event());
    return eventConsumerErrorMeters.computeIfAbsent(
        eventType, (op) -> registry.meter(EVENT_CONSUMER_ERROR_RATE.tagged("event-type", eventType)));
  }

  private Meter workflowConsumerMeter(String action) {
    return workflowConsumerMeters.computeIfAbsent(
        action, (op) -> registry.meter(WORKFLOW_CONSUMER_RATE.tagged("action", action)));
  }
}
