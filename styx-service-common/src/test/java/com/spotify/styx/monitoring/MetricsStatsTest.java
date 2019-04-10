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

import static com.spotify.styx.monitoring.MetricsStats.ACTIVE_STATES_PER_RUNSTATE_PER_TRIGGER;
import static com.spotify.styx.monitoring.MetricsStats.COUNTER_CACHE_RATE;
import static com.spotify.styx.monitoring.MetricsStats.DATASTORE_OPERATION_RATE;
import static com.spotify.styx.monitoring.MetricsStats.DOCKER_DURATION;
import static com.spotify.styx.monitoring.MetricsStats.DOCKER_ERROR_RATE;
import static com.spotify.styx.monitoring.MetricsStats.DOCKER_RATE;
import static com.spotify.styx.monitoring.MetricsStats.EVENT_CONSUMER_ERROR_RATE;
import static com.spotify.styx.monitoring.MetricsStats.EVENT_CONSUMER_RATE;
import static com.spotify.styx.monitoring.MetricsStats.EXIT_CODE_MISMATCH;
import static com.spotify.styx.monitoring.MetricsStats.EXIT_CODE_RATE;
import static com.spotify.styx.monitoring.MetricsStats.HISTOGRAM;
import static com.spotify.styx.monitoring.MetricsStats.KUBERNETES_DURATION;
import static com.spotify.styx.monitoring.MetricsStats.KUBERNETES_ERROR_RATE;
import static com.spotify.styx.monitoring.MetricsStats.KUBERNETES_RATE;
import static com.spotify.styx.monitoring.MetricsStats.NATURAL_TRIGGER_RATE;
import static com.spotify.styx.monitoring.MetricsStats.PUBLISHING_ERROR_RATE;
import static com.spotify.styx.monitoring.MetricsStats.PUBLISHING_RATE;
import static com.spotify.styx.monitoring.MetricsStats.PULL_IMAGE_ERROR_RATE;
import static com.spotify.styx.monitoring.MetricsStats.RESOURCE_CONFIGURED;
import static com.spotify.styx.monitoring.MetricsStats.RESOURCE_DEMANDED;
import static com.spotify.styx.monitoring.MetricsStats.RESOURCE_USED;
import static com.spotify.styx.monitoring.MetricsStats.STORAGE_DURATION;
import static com.spotify.styx.monitoring.MetricsStats.STORAGE_RATE;
import static com.spotify.styx.monitoring.MetricsStats.SUBMISSION_RATE_LIMIT;
import static com.spotify.styx.monitoring.MetricsStats.TERMINATION_LOG_INVALID;
import static com.spotify.styx.monitoring.MetricsStats.TERMINATION_LOG_MISSING;
import static com.spotify.styx.monitoring.MetricsStats.TICK_DURATION;
import static com.spotify.styx.monitoring.MetricsStats.TRANSITIONING_DURATION;
import static com.spotify.styx.monitoring.MetricsStats.WORKFLOW_CONSUMER_ERROR_RATE;
import static com.spotify.styx.monitoring.MetricsStats.WORKFLOW_CONSUMER_RATE;
import static com.spotify.styx.monitoring.MetricsStats.WORKFLOW_COUNT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.Time;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

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
  public void setUp() {
    when(time.nanoTime()).then(a -> System.nanoTime());
    when(registry.getOrAdd(TRANSITIONING_DURATION, HISTOGRAM)).thenReturn(histogram);
    when(registry.meter(PULL_IMAGE_ERROR_RATE)).thenReturn(meter);
    when(registry.meter(NATURAL_TRIGGER_RATE)).thenReturn(meter);
    when(registry.meter(TERMINATION_LOG_MISSING)).thenReturn(meter);
    when(registry.meter(TERMINATION_LOG_INVALID)).thenReturn(meter);
    when(registry.meter(EXIT_CODE_MISMATCH)).thenReturn(meter);
    when(registry.meter(WORKFLOW_CONSUMER_ERROR_RATE)).thenReturn(meter);
    when(registry.meter(COUNTER_CACHE_RATE.tagged("result", "miss"))).thenReturn(meter);
    when(registry.meter(COUNTER_CACHE_RATE.tagged("result", "hit"))).thenReturn(meter);
    stats = new MetricsStats(registry, time);
  }

  @Test
  public void shouldRecordStorageOperation() {
    String operation = "write";
    String status = "success";
    when(registry.getOrAdd(STORAGE_DURATION.tagged("operation", operation, "status", status), HISTOGRAM)).thenReturn(histogram);
    when(registry.meter(STORAGE_RATE.tagged("operation", operation, "status", status))).thenReturn(meter);
    stats.recordStorageOperation(operation, 1000L, status);
    verify(histogram).update(1000L);
    verify(meter).mark();
  }

  @Test
  public void shouldRecordDockerOperation() {
    String operation = "start";
    String status = "success";
    when(registry.getOrAdd(DOCKER_DURATION.tagged("operation", operation, "status", status), HISTOGRAM)).thenReturn(histogram);
    when(registry.meter(DOCKER_RATE.tagged("operation", operation, "status", status))).thenReturn(meter);
    stats.recordDockerOperation(operation, 1000L, status);
    verify(histogram).update(1000L);
    verify(meter).mark();
  }

  @Test
  public void shouldRecordDockerOperationError() {
    String operation = "start";
    String type = "kubernetes-client";
    when(registry.meter(DOCKER_ERROR_RATE.tagged("operation", operation, "type", type))).thenReturn(meter);
    stats.recordDockerOperationError(operation, type);
    verify(meter).mark();
  }


  @Test
  public void shouldRecordKubernetesOperation() {
    var operation = "start";
    var status = "success";
    when(registry.getOrAdd(KUBERNETES_DURATION.tagged("operation", operation, "status", status), HISTOGRAM)).thenReturn(histogram);
    when(registry.meter(KUBERNETES_RATE.tagged("operation", operation, "status", status))).thenReturn(meter);
    stats.recordKubernetesOperation(operation, 1000L, status);
    verify(histogram).update(1000L);
    verify(meter).mark();
  }

  @Test
  public void shouldRecordKubernetesOperationError() {
    var operation = "start";
    var type = "kubernetes-client";
    var code = 409;
    when(registry.meter(KUBERNETES_ERROR_RATE.tagged("operation", operation, "type", type, "code",
        String.valueOf(code)))).thenReturn(meter);
    stats.recordKubernetesOperationError(operation, type, code);
    verify(meter).mark();
  }

  @Test
  public void shouldRecordSubmitToRunningTime() {
    when(time.nanoTime()).thenReturn(SECONDS.toNanos(17L));
    stats.recordSubmission("foo");
    when(time.nanoTime()).thenReturn(SECONDS.toNanos(4711L));
    stats.recordRunning("foo");
    verify(histogram).update(4711L - 17L);
  }

  @Test
  public void shouldRegisterActiveStatesPerTriggerMetric() {
    RunState.State state = RunState.State.NEW;
    stats.registerActiveStatesMetric(state, "triggerName", gauge);
    verify(registry).register(ACTIVE_STATES_PER_RUNSTATE_PER_TRIGGER.tagged(
        "state", state.name(), "trigger", "triggerName"), gauge);
  }

  @Test
  public void shouldRegisterWorkflowCountMetric() {
    String status = "status";
    stats.registerWorkflowCountMetric(status, gauge);
    verify(registry).register(WORKFLOW_COUNT.tagged("status", status), gauge);
  }

  @Test
  public void shouldRecordExitCode() {
    WorkflowId workflowId = WorkflowId.create("component", "workflow");
    when(registry.meter(EXIT_CODE_RATE.tagged("exit-code", "0"))).thenReturn(meter);
    stats.recordExitCode(0);
    verify(meter).mark();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldRegisterSubmissionRateLimitMetric() {
    Gauge<Double> gauge = mock(Gauge.class);
    stats.registerSubmissionRateLimitMetric(gauge);
    verify(registry).register(SUBMISSION_RATE_LIMIT, gauge);
  }

  @Test
  public void shouldRecordTerminationLogMissing() {
    stats.recordTerminationLogMissing();
    verify(meter).mark();
  }

  @Test
  public void shouldRecordTerminationLogInvalid() {
    stats.recordTerminationLogInvalid();
    verify(meter).mark();
  }

  @Test
  public void shouldRecordExitCodeMismatch() {
    stats.recordExitCodeMismatch();
    verify(meter).mark();
  }

  @Test
  public void shouldRecordNaturalTrigger() {
    stats.recordNaturalTrigger();
    verify(meter).mark();
  }

  @Test
  public void shouldRecordPullImageError() {
    stats.recordPullImageError();
    verify(meter).mark();
  }

  @Test
  public void shouldRecordResourceConfigured() {
    String resource = "resource";
    when(registry.getOrAdd(RESOURCE_CONFIGURED.tagged("resource", resource), HISTOGRAM)).thenReturn(histogram);
    stats.recordResourceConfigured(resource, 100L);
    verify(histogram).update(100L);
  }

  @Test
  public void shouldRecordResourceUsed() {
    String resource = "resource";
    when(registry.getOrAdd(RESOURCE_USED.tagged("resource", resource), HISTOGRAM)).thenReturn(histogram);
    stats.recordResourceUsed(resource, 100L);
    verify(histogram).update(100L);
  }

  @Test
  public void shouldRecordResourceDemanded() {
    String resource = "resource";
    when(registry.getOrAdd(RESOURCE_DEMANDED.tagged("resource", resource), HISTOGRAM)).thenReturn(histogram);
    stats.recordResourceDemanded(resource, 17);
    verify(histogram).update(17L);
  }

  @Test
  public void shouldRecordEventConsumer() {
    final SequenceEvent event = SequenceEvent.create(
        Event.triggerExecution(TestData.WORKFLOW_INSTANCE, Trigger.natural(), TriggerParameters.zero()), 0L, 0L);
    when(registry.meter(EVENT_CONSUMER_RATE.tagged("event-type", "triggerExecution"))).thenReturn(meter);
    stats.recordEventConsumer(event);
    verify(meter).mark();
  }

  @Test
  public void shouldRecordEventConsumerError() {
    final SequenceEvent event = SequenceEvent.create(
        Event.triggerExecution(TestData.WORKFLOW_INSTANCE, Trigger.natural(), TriggerParameters.zero()), 0L, 0L);
    when(registry.meter(EVENT_CONSUMER_ERROR_RATE.tagged("event-type", "triggerExecution"))).thenReturn(meter);
    stats.recordEventConsumerError(event);
    verify(meter).mark();
  }

  @Test
  public void shouldRecordWorkflowConsumer() {
    when(registry.meter(WORKFLOW_CONSUMER_RATE.tagged("action", "updated"))).thenReturn(meter);
    stats.recordWorkflowConsumer("updated");
    verify(meter).mark();
  }

  @Test
  public void shouldRecordWorkflowConsumerError() {
    stats.recordWorkflowConsumerError();
    verify(meter).mark();
  }

  @Test
  public void shouldRecordPublishing() {
    when(registry.meter(PUBLISHING_RATE.tagged("type", "deploying", "state", "SUBMITTED"))).thenReturn(meter);
    stats.recordPublishing("deploying", "SUBMITTED");
    verify(meter).mark();
  }

  @Test
  public void shouldRecordPublishingError() {
    when(registry.meter(PUBLISHING_ERROR_RATE.tagged("type", "deploying", "state", "SUBMITTED"))).thenReturn(meter);
    stats.recordPublishingError("deploying", "SUBMITTED");
    verify(meter).mark();
  }

  @Test
  public void shouldRecordTickDuration() {
    String type = "dummy-tick";
    when(registry.getOrAdd(TICK_DURATION.tagged("type", type), HISTOGRAM)).thenReturn(histogram);
    stats.recordTickDuration(type, 100L);
    verify(histogram).update(100L);
  }

  @Test
  public void shouldRecordDatastoreEntityReads() {
    when(registry.meter(DATASTORE_OPERATION_RATE.tagged("operation", "read", "kind", "foobar"))).thenReturn(meter);
    stats.recordDatastoreEntityReads("foobar", 17);
    verify(meter).mark(17);
  }

  @Test
  public void shouldRecordDatastoreEntityWrites() {
    when(registry.meter(DATASTORE_OPERATION_RATE.tagged("operation", "write", "kind", "foobar"))).thenReturn(meter);
    stats.recordDatastoreEntityWrites("foobar", 17);
    verify(meter).mark(17);
  }

  @Test
  public void shouldRecordDatastoreEntityDeletes() {
    when(registry.meter(DATASTORE_OPERATION_RATE.tagged("operation", "delete", "kind", "foobar"))).thenReturn(meter);
    stats.recordDatastoreEntityDeletes("foobar", 17);
    verify(meter).mark(17);
  }

  @Test
  public void shouldRecordDatastoreEntityQueries() {
    when(registry.meter(DATASTORE_OPERATION_RATE.tagged("operation", "query", "kind", "foobar"))).thenReturn(meter);
    stats.recordDatastoreQueries("foobar", 17);
    verify(meter).mark(17);
  }

  @Test
  public void shouldRecordCounterCacheHit() {
    stats.recordCounterCacheHit();
    verify(meter).mark();
  }

  @Test
  public void shouldRecordCounterCacheMiss() {
    stats.recordCounterCacheMiss();
    verify(meter).mark();
  }

  @Test
  public void shouldCreateHistogram() {
    final Histogram histogram = HISTOGRAM.newMetric();
    assertThat(histogram, is(not(nullValue())));
    assertThat(HISTOGRAM.isInstance(histogram), is(true));
  }
}
