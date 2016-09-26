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
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.styx.model.WorkflowId;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class MetricsStats implements Stats {

  private static final String UNIT_SECOND = "s";
  private static final String UNIT_MILLISECOND = "ms";
  private static final String UNIT_FREQUENCY = "Hz";
  private static final MetricId BASE = MetricId.build("styx");

  private static final MetricId ACTIVE_STATES = BASE
      .tagged("what", "active-states-count")
      .tagged("unit", "state");

  private static final MetricId ACTIVE_STATES_PER_WORKFLOW = BASE
      .tagged("what", "active-states-per-workflow-count")
      .tagged("unit", "state");

  private static final MetricId WORKFLOW_COUNT = BASE
      .tagged("what", "workflow-count")
      .tagged("unit", "workflow");

  private static final MetricId STORAGE_DURATION = BASE
      .tagged("what", "storage-operation-duration")
      .tagged("unit", UNIT_MILLISECOND);

  private static final MetricId STORAGE_RATE = BASE
      .tagged("what", "storage-operation-rate")
      .tagged("unit", UNIT_FREQUENCY);

  private static final MetricId DOCKER_DURATION = BASE
      .tagged("what", "docker-operation-duration")
      .tagged("unit", UNIT_MILLISECOND);

  private static final MetricId DOCKER_RATE = BASE
      .tagged("what", "docker-operation-rate")
      .tagged("unit", UNIT_FREQUENCY);

  private static final MetricId TRANSITIONING_DURATION = BASE
      .tagged("what", "time-transitioning-between-submitted-running")
      .tagged("unit", UNIT_SECOND);

  private static final MetricId PULL_IMAGE_ERROR_RATE = BASE
      .tagged("what", "pull-image-error-rate")
      .tagged("unit", UNIT_FREQUENCY);

  private final SemanticMetricRegistry registry;

  private final Histogram submitToRunning;
  private final Meter pullImageErrorMeter;
  private final ConcurrentMap<String, Histogram> storageOperationHistograms;
  private final ConcurrentMap<String, Meter> storageOperationMeters;
  private final ConcurrentMap<String, Histogram> dockerOperationHistograms;
  private final ConcurrentMap<String, Meter> dockerOperationMeters;
  private final ConcurrentHashMap<WorkflowId, Gauge> activeStatesPerWorkflowGauges;

  public MetricsStats(SemanticMetricRegistry registry) {
    this.registry = Objects.requireNonNull(registry);

    this.submitToRunning = registry.histogram(TRANSITIONING_DURATION);
    this.pullImageErrorMeter = registry.meter(PULL_IMAGE_ERROR_RATE);
    this.storageOperationHistograms = new ConcurrentHashMap<>();
    this.storageOperationMeters = new ConcurrentHashMap<>();
    this.dockerOperationHistograms = new ConcurrentHashMap<>();
    this.dockerOperationMeters = new ConcurrentHashMap<>();
    this.activeStatesPerWorkflowGauges = new ConcurrentHashMap<>();
  }

  @Override
  public void storageOperation(String operation, long durationMillis) {
    storageOpHistogram(operation).update(durationMillis);
    storageOpMeter(operation).mark();
  }

  @Override
  public void dockerOperation(String operation, long durationMillis) {
    dockerOpHistogram(operation).update(durationMillis);
    dockerOpMeter(operation).mark();
  }

  @Override
  public void submitToRunningTime(long durationSeconds) {
    submitToRunning.update(durationSeconds);
  }

  @Override
  public void registerActiveStates(Gauge<Long> activeStatesCount) {
    registry.register(ACTIVE_STATES, activeStatesCount);
  }

  @Override
  public void registerActiveStates(WorkflowId workflowId, Gauge<Long> activeStatesCount) {
    activeStatesPerWorkflowGauges.computeIfAbsent(
        workflowId, (ignoreKey) -> registry.register(
            ACTIVE_STATES_PER_WORKFLOW.tagged(
                "component-id", workflowId.componentId(), "endpoint-id", workflowId.endpointId()),
            activeStatesCount));
  }

  @Override
  public void registerWorkflowCount(String status, Gauge<Long> workflowCount) {
    registry.register(WORKFLOW_COUNT.tagged("status", status), workflowCount);
  }

  @Override
  public void pullImageError() {
    pullImageErrorMeter.mark();
  }

  private Histogram storageOpHistogram(String operation) {
    return storageOperationHistograms.computeIfAbsent(
        operation, (op) -> registry.histogram(STORAGE_DURATION.tagged("operation", op)));
  }

  private Meter storageOpMeter(String operation) {
    return storageOperationMeters.computeIfAbsent(
        operation, (op) -> registry.meter(STORAGE_RATE.tagged("operation", op)));
  }

  private Histogram dockerOpHistogram(String operation) {
    return dockerOperationHistograms.computeIfAbsent(
        operation, (op) -> registry.histogram(DOCKER_DURATION.tagged("operation", op)));
  }

  private Meter dockerOpMeter(String operation) {
    return dockerOperationMeters.computeIfAbsent(
        operation, (op) -> registry.meter(DOCKER_RATE.tagged("operation", op)));
  }
}
