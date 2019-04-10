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
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricBuilder;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.styx.model.SequenceEvent;
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

  /**
   * A variant of {@link SemanticMetricBuilder#HISTOGRAMS} that uses {@link SlidingTimeWindowArrayReservoir}
   * instead of {@link com.codahale.metrics.ExponentiallyDecayingReservoir}.
   */
  static final SemanticMetricBuilder<Histogram> HISTOGRAM = new SemanticMetricBuilder<Histogram>() {
    @Override
    public Histogram newMetric() {
      // TODO: What time window do we want?
      return new Histogram(new SlidingTimeWindowArrayReservoir(30, TimeUnit.SECONDS));
    }

    @Override
    public boolean isInstance(Metric metric) {
      return metric instanceof Histogram;
    }
  };

  private static final String UNIT_SECOND = "s";
  private static final String UNIT_MILLISECOND = "ms";
  private static final MetricId BASE = MetricId.build("styx");

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

  static final MetricId RESOURCE_DEMANDED = BASE
      .tagged("what", "resource-demanded");

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

  static final MetricId KUBERNETES_DURATION = BASE
      .tagged("what", "kubernetes-operation-duration")
      .tagged("unit", UNIT_MILLISECOND);

  static final MetricId KUBERNETES_RATE = BASE
      .tagged("what", "kubernetes-operation-rate")
      .tagged("unit", OPERATION);

  static final MetricId KUBERNETES_ERROR_RATE = BASE
      .tagged("what", "kubernetes-operation-error-rate")
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

  static final MetricId PUBLISHING_RATE = BASE
      .tagged("what", "publishing-rate");

  static final MetricId PUBLISHING_ERROR_RATE = BASE
      .tagged("what", "publishing-error-rate")
      .tagged("unit", "error");

  static final MetricId TICK_DURATION = BASE
      .tagged("what", "tick-duration")
      .tagged("unit", UNIT_MILLISECOND);

  static final MetricId DATASTORE_OPERATION_RATE = BASE
      .tagged("what", "datastore-operation-rate");

  static final MetricId COUNTER_CACHE_RATE = BASE
      .tagged("what", "counter-cache-rate");

  private static final String STATUS = "status";
  private static final String COUNTER_CACHE_RESULT = "result";
  private static final String COUNTER_CACHE_HIT = "hit";
  private static final String COUNTER_CACHE_MISS = "miss";

  private final SemanticMetricRegistry registry;
  private final Time time;

  private final Histogram submitToRunning;
  private final Meter pullImageErrorMeter;
  private final Meter naturalTrigger;
  private final Meter terminationLogMissing;
  private final Meter terminationLogInvalid;
  private final Meter exitCodeMismatch;
  private final Meter workflowConsumerErrorMeter;
  private final Meter counterCacheHitMeter;
  private final Meter counterCacheMissMeter;
  private final ConcurrentMap<String, Histogram> storageOperationHistograms;
  private final ConcurrentMap<String, Meter> storageOperationMeters;
  private final ConcurrentMap<String, Histogram> dockerOperationHistograms;
  private final ConcurrentMap<String, Meter> dockerOperationMeters;
  private final ConcurrentMap<Tuple2<String, String>, Meter> dockerOperationErrorMeters;
  private final ConcurrentMap<String, Histogram> kubernetesOperationHistograms;
  private final ConcurrentMap<String, Meter> kubernetesOperationMeters;
  private final ConcurrentMap<Tuple3<String, String, Integer>, Meter> kubernetesOperationErrorMeters;
  private final ConcurrentMap<Integer, Meter> exitCodeMeters;
  private final ConcurrentMap<String, Histogram> resourceConfiguredHistograms;
  private final ConcurrentMap<String, Histogram> resourceUsedHistograms;
  private final ConcurrentMap<String, Histogram> resourceDemandedHistograms;
  private final ConcurrentMap<String, Meter> eventConsumerErrorMeters;
  private final ConcurrentMap<String, Meter> eventConsumerMeters;
  private final ConcurrentMap<String, Meter> publishingMeters;
  private final ConcurrentMap<String, Meter> publishingErrorMeters;
  private final ConcurrentMap<String, Meter> workflowConsumerMeters;
  private final ConcurrentMap<String, Histogram> tickHistograms;
  private final ConcurrentMap<Tuple2<String, String>, Meter> datastoreOperationMeters;

  /**
   * Submission timestamps (nanotime) keyed on execution id.
   */
  private final Cache<String, Long> submissionTimestamps = CacheBuilder.newBuilder()
      .maximumSize(100_000)
      .build();

  public MetricsStats(SemanticMetricRegistry registry, Time time) {
    this.registry = Objects.requireNonNull(registry);
    this.time = Objects.requireNonNull(time, "time");

    this.submitToRunning = registry.getOrAdd(TRANSITIONING_DURATION, HISTOGRAM);
    this.pullImageErrorMeter = registry.meter(PULL_IMAGE_ERROR_RATE);
    this.naturalTrigger = registry.meter(NATURAL_TRIGGER_RATE);
    this.terminationLogMissing = registry.meter(TERMINATION_LOG_MISSING);
    this.terminationLogInvalid = registry.meter(TERMINATION_LOG_INVALID);
    this.exitCodeMismatch = registry.meter(EXIT_CODE_MISMATCH);
    this.workflowConsumerErrorMeter = registry.meter(WORKFLOW_CONSUMER_ERROR_RATE);
    this.counterCacheHitMeter = registry.meter(COUNTER_CACHE_RATE.tagged(COUNTER_CACHE_RESULT, COUNTER_CACHE_HIT));
    this.counterCacheMissMeter = registry.meter(COUNTER_CACHE_RATE.tagged(COUNTER_CACHE_RESULT, COUNTER_CACHE_MISS));
    this.storageOperationHistograms = new ConcurrentHashMap<>();
    this.storageOperationMeters = new ConcurrentHashMap<>();
    this.dockerOperationHistograms = new ConcurrentHashMap<>();
    this.dockerOperationMeters = new ConcurrentHashMap<>();
    this.dockerOperationErrorMeters = new ConcurrentHashMap<>();
    this.kubernetesOperationHistograms = new ConcurrentHashMap<>();
    this.kubernetesOperationMeters = new ConcurrentHashMap<>();
    this.kubernetesOperationErrorMeters = new ConcurrentHashMap<>();
    this.exitCodeMeters = new ConcurrentHashMap<>();
    this.resourceConfiguredHistograms = new ConcurrentHashMap<>();
    this.resourceUsedHistograms = new ConcurrentHashMap<>();
    this.resourceDemandedHistograms = new ConcurrentHashMap<>();
    this.eventConsumerErrorMeters = new ConcurrentHashMap<>();
    this.eventConsumerMeters = new ConcurrentHashMap<>();
    this.publishingMeters = new ConcurrentHashMap<>();
    this.publishingErrorMeters = new ConcurrentHashMap<>();
    this.workflowConsumerMeters = new ConcurrentHashMap<>();
    this.tickHistograms = new ConcurrentHashMap<>();
    this.datastoreOperationMeters = new ConcurrentHashMap<>();
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
  public void recordDockerOperationError(String operation, String type) {
    dockerOpErrorMeter(operation, type).mark();
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
  public void recordExitCode(int exitCode) {
    exitCodeMeter(exitCode).mark();
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
  public void recordResourceDemanded(String resource, long demanded) {
    resourceDemandedHistogram(resource).update(demanded);
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

  @Override
  public void recordPublishing(String type, String state) {
    publishingMeter(type, state).mark();
  }

  @Override
  public void recordPublishingError(String type, String state) {
    publishingErrorMeter(type, state).mark();
  }

  @Override
  public void recordTickDuration(String type, long duration) {
    tickHistogram(type).update(duration);
  }

  @Override
  public void recordDatastoreEntityReads(String kind, int n) {
    recordDatastoreOperations("read", kind, n);
  }

  @Override
  public void recordDatastoreEntityWrites(String kind, int n) {
    recordDatastoreOperations("write", kind, n);
  }

  @Override
  public void recordDatastoreEntityDeletes(String kind, int n) {
    recordDatastoreOperations("delete", kind, n);
  }

  @Override
  public void recordDatastoreQueries(String kind, int n) {
    recordDatastoreOperations("query", kind, n);
  }

  @Override
  public void recordCounterCacheHit() {
    counterCacheHitMeter.mark();
  }

  @Override
  public void recordCounterCacheMiss() {
    counterCacheMissMeter.mark();
  }

  @Override
  public void recordKubernetesOperation(String operation, long durationMillis, String status) {
    kubernetesOpHistogram(operation, status).update(durationMillis);
    kubernetesOpMeter(operation, status).mark();
  }

  @Override
  public void recordKubernetesOperationError(String operation, String type, int code) {
    kubernetesOpErrorMeter(operation, type, code).mark();
  }

  private void recordDatastoreOperations(String operation, String kind, int n) {
    datastoreOperationMeter(operation, kind).mark(n);
  }

  private Meter exitCodeMeter(int exitCode) {
    return exitCodeMeters
        .computeIfAbsent(exitCode, ignore -> registry.meter(EXIT_CODE_RATE.tagged(
            "exit-code", Integer.toString(exitCode))));
  }

  private Meter dockerOpErrorMeter(String operation, String type) {
    return dockerOperationErrorMeters
        .computeIfAbsent(Tuple.of(operation, type), (tuple) ->
            registry.meter(DOCKER_ERROR_RATE.tagged(
                "operation", tuple._1,
                "type", tuple._2)));
  }

  private Meter kubernetesOpErrorMeter(String operation, String type, int code) {
    return kubernetesOperationErrorMeters
        .computeIfAbsent(Tuple.of(operation, type, code), (tuple) ->
            registry.meter(KUBERNETES_ERROR_RATE.tagged(
                "operation", tuple._1,
                "type", tuple._2,
                "code", String.valueOf(tuple._3))));
  }

  private Histogram storageOpHistogram(String operation, String status) {
    return storageOperationHistograms.computeIfAbsent(
        operation, (op) -> registry.getOrAdd(STORAGE_DURATION.tagged(OPERATION, op, STATUS, status), HISTOGRAM));
  }

  private Meter storageOpMeter(String operation, String status) {
    return storageOperationMeters.computeIfAbsent(
        operation, (op) -> registry.meter(STORAGE_RATE.tagged(OPERATION, op, STATUS, status)));
  }

  private Histogram dockerOpHistogram(String operation, String status) {
    return dockerOperationHistograms.computeIfAbsent(
        operation, (op) -> registry.getOrAdd(DOCKER_DURATION.tagged(OPERATION, op, STATUS, status), HISTOGRAM));
  }

  private Meter dockerOpMeter(String operation, String status) {
    return dockerOperationMeters.computeIfAbsent(
        operation, (op) -> registry.meter(DOCKER_RATE.tagged(OPERATION, op, STATUS, status)));
  }

  private Histogram kubernetesOpHistogram(String operation, String status) {
    return kubernetesOperationHistograms.computeIfAbsent(
        operation, (op) -> registry.getOrAdd(KUBERNETES_DURATION.tagged(OPERATION, op, STATUS, status), HISTOGRAM));
  }

  private Meter kubernetesOpMeter(String operation, String status) {
    return kubernetesOperationMeters.computeIfAbsent(
        operation, (op) -> registry.meter(KUBERNETES_RATE.tagged(OPERATION, op, STATUS, status)));
  }

  private Histogram resourceConfiguredHistogram(String resource) {
    return resourceConfiguredHistograms.computeIfAbsent(
        resource, (op) -> registry.getOrAdd(RESOURCE_CONFIGURED.tagged("resource", resource), HISTOGRAM));
  }

  private Histogram resourceUsedHistogram(String resource) {
    return resourceUsedHistograms.computeIfAbsent(
        resource, (op) -> registry.getOrAdd(RESOURCE_USED.tagged("resource", resource), HISTOGRAM));
  }

  private Histogram resourceDemandedHistogram(String resource) {
    return resourceDemandedHistograms.computeIfAbsent(
        resource, (op) -> registry.getOrAdd(RESOURCE_DEMANDED.tagged("resource", resource), HISTOGRAM));
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

  private Meter publishingMeter(String type, String state) {
    return publishingMeters.computeIfAbsent(
        type, (op) -> registry.meter(PUBLISHING_RATE.tagged("type", type, "state", state)));
  }

  private Meter publishingErrorMeter(String type, String state) {
    return publishingErrorMeters.computeIfAbsent(
        type, (op) -> registry.meter(PUBLISHING_ERROR_RATE.tagged("type", type, "state", state)));
  }

  private Histogram tickHistogram(String type) {
    return tickHistograms.computeIfAbsent(
        type, (op) -> registry.getOrAdd(TICK_DURATION.tagged("type", type), HISTOGRAM));
  }

  private Meter datastoreOperationMeter(String operation, String kind) {
    return datastoreOperationMeters.computeIfAbsent(Tuple.of(operation, kind),
        t -> registry.meter(DATASTORE_OPERATION_RATE.tagged("operation", operation, "kind", kind)));
  }
}
