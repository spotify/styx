/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2017 Spotify AB
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.state.RunState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MetricsStatsTest {

  private static final String UNIT_SECOND = "s";
  private static final String UNIT_MILLISECOND = "ms";
  private static final MetricId BASE = MetricId.build("styx");

  private static final MetricId QUEUED_EVENTS = BASE
      .tagged("what", "queued-events-count")
      .tagged("unit", "events");

  private static final MetricId ACTIVE_STATES_PER_RUNSTATE_PER_TRIGGER = BASE
      .tagged("what", "active-states-per-runstate-per-trigger-count")
      .tagged("unit", "state");

  private static final MetricId ACTIVE_STATES_PER_WORKFLOW = BASE
      .tagged("what", "active-states-per-workflow-count")
      .tagged("unit", "state");

  private static final MetricId WORKFLOW_COUNT = BASE
      .tagged("what", "workflow-count")
      .tagged("unit", "workflow");

  private static final MetricId RESOURCE_CONFIGURED = BASE
      .tagged("what", "resource-configured");

  private static final MetricId RESOURCE_USED = BASE
      .tagged("what", "resource-used");

  private static final MetricId EXIT_CODE_RATE = BASE
      .tagged("what", "exit-code-rate");

  private static final MetricId STORAGE_DURATION = BASE
      .tagged("what", "storage-operation-duration")
      .tagged("unit", UNIT_MILLISECOND);

  private static final MetricId STORAGE_RATE = BASE
      .tagged("what", "storage-operation-rate")
      .tagged("unit", "operation");

  private static final MetricId DOCKER_DURATION = BASE
      .tagged("what", "docker-operation-duration")
      .tagged("unit", UNIT_MILLISECOND);

  private static final MetricId DOCKER_RATE = BASE
      .tagged("what", "docker-operation-rate")
      .tagged("unit", "operation");

  private static final MetricId TRANSITIONING_DURATION = BASE
      .tagged("what", "time-transitioning-between-submitted-running")
      .tagged("unit", UNIT_SECOND);

  private static final MetricId PULL_IMAGE_ERROR_RATE = BASE
      .tagged("what", "pull-image-error-rate")
      .tagged("unit", "error");

  private static final MetricId NATURAL_TRIGGER_RATE = BASE
      .tagged("what", "natural-trigger-rate")
      .tagged("unit", "trigger");

  private static final MetricId TERMINATION_LOG_MISSING = BASE
      .tagged("what", "termination-log-missing");

  private static final MetricId TERMINATION_LOG_INVALID = BASE
      .tagged("what", "termination-log-invalid");

  private static final MetricId EXIT_CODE_MISMATCH = BASE
      .tagged("what", "exit-code-mismatch");

  private static final MetricId SUBMISSION_RATE_LIMIT = BASE
      .tagged("what", "submission-rate-limit")
      .tagged("unit", "submission/s");

  private Stats stats;
  
  @Mock
  private SemanticMetricRegistry registry;

  @Mock
  private Histogram histogram;

  @Mock
  private Meter meter;

  @Mock
  private Gauge<Long> gauge;

  @Before
  public void setUp() throws Exception {
    when(registry.histogram(TRANSITIONING_DURATION)).thenReturn(histogram);
    when(registry.meter(PULL_IMAGE_ERROR_RATE)).thenReturn(meter);
    when(registry.meter(NATURAL_TRIGGER_RATE)).thenReturn(meter);
    when(registry.meter(TERMINATION_LOG_MISSING)).thenReturn(meter);
    when(registry.meter(TERMINATION_LOG_INVALID)).thenReturn(meter);
    when(registry.meter(EXIT_CODE_MISMATCH)).thenReturn(meter);
    stats = new MetricsStats(registry);
  }

  @Test
  public void shouldRecordStorageOperation() throws Exception {
    String operation = "operation";
    when(registry.histogram(STORAGE_DURATION.tagged("operation", operation))).thenReturn(histogram);
    when(registry.meter(STORAGE_RATE.tagged("operation", operation))).thenReturn(meter);
    stats.recordStorageOperation(operation, 1000L);
    verify(histogram).update(1000L);
    verify(meter).mark();
  }

  @Test
  public void shouldRecordDockerOperation() throws Exception {
    String operation = "operation";
    when(registry.histogram(DOCKER_DURATION.tagged("operation", operation))).thenReturn(histogram);
    when(registry.meter(DOCKER_RATE.tagged("operation", operation))).thenReturn(meter);
    stats.recordDockerOperation("operation", 1000L);
    verify(histogram).update(1000L);
    verify(meter).mark();
  }

  @Test
  public void shouldRecordSubmitToRunningTime() throws Exception {
    stats.recordSubmitToRunningTime(1000L);
    verify(histogram).update(1000L);
  }

  @Test
  public void shouldRegisterQueuedEventsMetric() throws Exception {
    stats.registerQueuedEventsMetric(gauge);
    verify(registry).register(QUEUED_EVENTS, gauge);
  }

  @Test
  public void shouldRegisterActiveStatesPerTriggerMetric() throws Exception {
    RunState.State state = RunState.State.NEW;
    stats.registerActiveStatesMetric(state, "triggerName", gauge);
    verify(registry).register(ACTIVE_STATES_PER_RUNSTATE_PER_TRIGGER.tagged(
        "state", state.name(), "trigger", "triggerName"), gauge);
  }

  @Test
  public void shouldRegisterActiveStatesMetric() throws Exception {
    WorkflowId workflowId = WorkflowId.create("component", "workflow");
    stats.registerActiveStatesMetric(workflowId, gauge);
    verify(registry).register(ACTIVE_STATES_PER_WORKFLOW.tagged(
        "component-id", workflowId.componentId(), "workflow-id", workflowId.id()), gauge);
  }

  @Test
  public void shouldRegisterWorkflowCountMetric() throws Exception {
    String status = "status";
    stats.registerWorkflowCountMetric(status, gauge);
    verify(registry).register(WORKFLOW_COUNT.tagged("status", status), gauge);
  }

  @Test
  public void shouldRecordExitCode() throws Exception {
    WorkflowId workflowId = WorkflowId.create("component", "workflow");
    when(registry.meter(EXIT_CODE_RATE.tagged(
        "component-id", workflowId.componentId(),
        "workflow-id", workflowId.id(),
        "exit-code", "0"))).thenReturn(meter);
    stats.recordExitCode(workflowId, 0);
    verify(meter).mark();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRegisterSubmissionRateLimitMetric() throws Exception {
    Gauge<Double> gauge = mock(Gauge.class);
    stats.registerSubmissionRateLimitMetric(gauge);
    verify(registry).register(SUBMISSION_RATE_LIMIT, gauge);
  }

  @Test
  public void shouldRecordTerminationLogMissing() throws Exception {
    stats.recordTerminationLogMissing();
    verify(meter).mark();
  }

  @Test
  public void shouldRecordTerminationLogInvalid() throws Exception {
    stats.recordTerminationLogInvalid();
    verify(meter).mark();
  }

  @Test
  public void shouldRecordExitCodeMismatch() throws Exception {
    stats.recordExitCodeMismatch();
    verify(meter).mark();
  }

  @Test
  public void shouldRecordNaturalTrigger() throws Exception {
    stats.recordNaturalTrigger();
    verify(meter).mark();
  }

  @Test
  public void shouldRecordPullImageError() throws Exception {
    stats.recordPullImageError();
    verify(meter).mark();
  }

  @Test
  public void shouldRecordResourceConfigured() throws Exception {
    String resource = "resource";
    when(registry.histogram(RESOURCE_CONFIGURED.tagged("resource", resource))).thenReturn(histogram);
    stats.recordResourceConfigured(resource, 100L);
    verify(histogram).update(100L);
  }

  @Test
  public void shouldRecordResourceUsed() throws Exception {
    String resource = "resource";
    when(registry.histogram(RESOURCE_USED.tagged("resource", resource))).thenReturn(histogram);
    stats.recordResourceUsed(resource, 100L);
    verify(histogram).update(100L);
  }
}
