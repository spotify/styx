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

import static com.spotify.styx.monitoring.MetricsStats.ACTIVE_STATES_PER_RUNSTATE_PER_TRIGGER;
import static com.spotify.styx.monitoring.MetricsStats.ACTIVE_STATES_PER_WORKFLOW;
import static com.spotify.styx.monitoring.MetricsStats.DOCKER_DURATION;
import static com.spotify.styx.monitoring.MetricsStats.DOCKER_ERROR_RATE;
import static com.spotify.styx.monitoring.MetricsStats.DOCKER_RATE;
import static com.spotify.styx.monitoring.MetricsStats.EVENT_CONSUMER_ERROR_RATE;
import static com.spotify.styx.monitoring.MetricsStats.EVENT_CONSUMER_RATE;
import static com.spotify.styx.monitoring.MetricsStats.EXIT_CODE_MISMATCH;
import static com.spotify.styx.monitoring.MetricsStats.EXIT_CODE_RATE;
import static com.spotify.styx.monitoring.MetricsStats.NATURAL_TRIGGER_RATE;
import static com.spotify.styx.monitoring.MetricsStats.PULL_IMAGE_ERROR_RATE;
import static com.spotify.styx.monitoring.MetricsStats.QUEUED_EVENTS;
import static com.spotify.styx.monitoring.MetricsStats.RESOURCE_CONFIGURED;
import static com.spotify.styx.monitoring.MetricsStats.RESOURCE_USED;
import static com.spotify.styx.monitoring.MetricsStats.STORAGE_DURATION;
import static com.spotify.styx.monitoring.MetricsStats.STORAGE_RATE;
import static com.spotify.styx.monitoring.MetricsStats.SUBMISSION_RATE_LIMIT;
import static com.spotify.styx.monitoring.MetricsStats.TERMINATION_LOG_INVALID;
import static com.spotify.styx.monitoring.MetricsStats.TERMINATION_LOG_MISSING;
import static com.spotify.styx.monitoring.MetricsStats.TRANSITIONING_DURATION;
import static com.spotify.styx.monitoring.MetricsStats.WORKFLOW_CONSUMER_ERROR_RATE;
import static com.spotify.styx.monitoring.MetricsStats.WORKFLOW_CONSUMER_RATE;
import static com.spotify.styx.monitoring.MetricsStats.WORKFLOW_COUNT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.Time;
import java.time.Instant;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MetricsStatsTest {

  private Stats stats;

  @Mock
  private SemanticMetricRegistry registry;

  @Mock
  private Histogram histogram;

  @Mock
  private Meter meter;

  @Mock
  private Gauge<Long> gauge;

  @Mock Time time;

  @Before
  public void setUp() throws Exception {
    when(time.nanoTime()).then(a -> System.nanoTime());
    when(time.get()).then(a -> Instant.now());
    when(registry.histogram(TRANSITIONING_DURATION)).thenReturn(histogram);
    when(registry.meter(PULL_IMAGE_ERROR_RATE)).thenReturn(meter);
    when(registry.meter(NATURAL_TRIGGER_RATE)).thenReturn(meter);
    when(registry.meter(TERMINATION_LOG_MISSING)).thenReturn(meter);
    when(registry.meter(TERMINATION_LOG_INVALID)).thenReturn(meter);
    when(registry.meter(EXIT_CODE_MISMATCH)).thenReturn(meter);
    when(registry.meter(WORKFLOW_CONSUMER_ERROR_RATE)).thenReturn(meter);
    stats = new MetricsStats(registry, time);
  }

  @Test
  public void shouldRecordStorageOperation() throws Exception {
    String operation = "write";
    String status = "success";
    when(registry.histogram(STORAGE_DURATION.tagged("operation", operation, "status", status))).thenReturn(histogram);
    when(registry.meter(STORAGE_RATE.tagged("operation", operation, "status", status))).thenReturn(meter);
    stats.recordStorageOperation(operation, 1000L, status);
    verify(histogram).update(1000L);
    verify(meter).mark();
  }

  @Test
  public void shouldRecordDockerOperation() throws Exception {
    String operation = "start";
    String status = "success";
    when(registry.histogram(DOCKER_DURATION.tagged("operation", operation, "status", status))).thenReturn(histogram);
    when(registry.meter(DOCKER_RATE.tagged("operation", operation, "status", status))).thenReturn(meter);
    stats.recordDockerOperation(operation, 1000L, status);
    verify(histogram).update(1000L);
    verify(meter).mark();
  }

  @Test
  public void shouldRecordDockerOperationError() throws Exception {
    String operation = "start";
    String type = "kubernetes-client";
    int code = 429;
    when(registry.meter(DOCKER_ERROR_RATE.tagged("operation", operation, "type", type, "code", String.valueOf(code)))).thenReturn(meter);
    stats.recordDockerOperationError(operation, type, code, 1000L);
    verify(meter).mark();
  }

  @Test
  public void shouldRecordSubmitToRunningTime() throws Exception {
    when(time.nanoTime()).thenReturn(SECONDS.toNanos(17L));
    stats.recordSubmission("foo");
    when(time.nanoTime()).thenReturn(SECONDS.toNanos(4711L));
    stats.recordRunning("foo");
    verify(histogram).update(4711L - 17L);
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

  @Test
  public void shouldRecordEventConsumer() throws Exception {
    final SequenceEvent event = SequenceEvent
        .create(Event.triggerExecution(TestData.WORKFLOW_INSTANCE, Trigger.natural()), 0L, 0L);
    when(registry.meter(EVENT_CONSUMER_RATE.tagged("event-type", "triggerExecution"))).thenReturn(meter);
    stats.recordEventConsumer(event);
    verify(meter).mark();
  }

  @Test
  public void shouldRecordEventConsumerError() throws Exception {
    final SequenceEvent event = SequenceEvent
        .create(Event.triggerExecution(TestData.WORKFLOW_INSTANCE, Trigger.natural()), 0L, 0L);
    when(registry.meter(EVENT_CONSUMER_ERROR_RATE.tagged("event-type", "triggerExecution"))).thenReturn(meter);
    stats.recordEventConsumerError(event);
    verify(meter).mark();
  }

  @Test
  public void shouldRecordWorkflowConsumer() throws Exception {
    when(registry.meter(WORKFLOW_CONSUMER_RATE.tagged("action", "updated"))).thenReturn(meter);
    stats.recordWorkflowConsumer("updated");
    verify(meter).mark();
  }

  @Test
  public void shouldRecordWorkflowConsumerError() throws Exception {
    stats.recordWorkflowConsumerError();
    verify(meter).mark();
  }
}
