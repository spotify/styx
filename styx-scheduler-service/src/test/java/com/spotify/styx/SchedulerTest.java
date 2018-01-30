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

package com.spotify.styx;

import static com.spotify.styx.state.TimeoutConfig.createWithDefaultTtl;
import static java.time.Duration.ofSeconds;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.longThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
import com.spotify.futures.CompletableFutures;
import com.spotify.styx.WorkflowExecutionGate.ExecutionBlocker;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.TimeoutConfig;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.EventUtil;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SchedulerTest {

  private static final WorkflowId WORKFLOW_ID1 =
      WorkflowId.create("styx1", "example1");
  private static final WorkflowId WORKFLOW_ID2 =
      WorkflowId.create("styx2", "example2");
  private static final WorkflowInstance INSTANCE_1 =
      WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T01");

  private WorkflowCache workflowCache;
  private Scheduler scheduler;

  private Instant now = Instant.parse("2016-12-02T22:00:00Z");
  private Time time = () -> now;

  private List<Resource> resourceLimits = Lists.newArrayList();

  private ExecutorService executor = Executors.newCachedThreadPool();
  private ConcurrentMap<WorkflowInstance, RunState> activeStates = Maps.newConcurrentMap();


  @Mock WorkflowResourceDecorator resourceDecorator;
  @Mock RateLimiter rateLimiter;
  @Mock Stats stats;
  @Mock StyxConfig config;
  @Mock WorkflowExecutionGate gate;
  @Mock StateManager stateManager;

  @Before
  public void setUp() throws Exception {
    when(rateLimiter.tryAcquire()).thenReturn(true);

    when(gate.executionBlocker(any()))
        .thenReturn(WorkflowExecutionGate.NO_BLOCKER);
  }

  @After
  public void tearDown() throws Exception {
    executor.shutdownNow();
  }

  private void setUp(long timeoutSeconds) throws IsClosedException, IOException {
    workflowCache = new InMemWorkflowCache();
    TimeoutConfig timeoutConfig = createWithDefaultTtl(ofSeconds(timeoutSeconds));

    final Storage storage = mock(Storage.class);
    when(storage.resources()).thenReturn(resourceLimits);
    when(config.globalConcurrency()).thenReturn(Optional.empty());
    when(storage.config()).thenReturn(config);

    when(resourceDecorator.decorateResources(
        any(RunState.class), any(WorkflowConfiguration.class), anySetOf(String.class)))
        .thenAnswer(a -> a.getArgumentAt(2, Set.class));

    scheduler = new Scheduler(time, timeoutConfig, stateManager, workflowCache, storage, resourceDecorator,
                              stats, rateLimiter, gate);
  }

  private void setResourceLimit(String resourceId, long limit) {
    resourceLimits.removeIf(r -> r.id().equals(resourceId));
    resourceLimits.add(Resource.create(resourceId, limit));
  }

  private void initWorkflow(Workflow workflow) {
    workflowCache.store(workflow);
  }

  private void populateActiveStates(RunState... runStates) throws IsClosedException {
    for (RunState runState : runStates) {
      activeStates.put(runState.workflowInstance(), runState);
    }
    when(stateManager.activeStates()).thenReturn(activeStates);
  }

  private void removeActiveStates(Set<WorkflowInstance> workflowInstances) throws IsClosedException {
    for (WorkflowInstance workflowInstance : workflowInstances) {
      activeStates.remove(workflowInstance);
    }
    when(stateManager.activeStates()).thenReturn(activeStates);
  }

  private Workflow workflowUsingResources(WorkflowId id, String... resources) {
    return Workflow.create(
        id.componentId(),
        WorkflowConfiguration.builder()
            .id(id.id())
            .schedule(Schedule.HOURS)
            .resources(resources)
            .build());
  }

  @Test
  public void shouldBeRateLimiting() throws Exception {

    when(rateLimiter.tryAcquire()).thenReturn(false);

    setUp(20);
    setResourceLimit("r1", 2);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, time));

    scheduler.tick();

    verify(rateLimiter).tryAcquire();
    verify(stats).recordResourceUsed("r1", 0L);
    verify(stateManager, never()).receiveIgnoreClosed(any());

    when(rateLimiter.tryAcquire()).thenReturn(true);

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_1));
    verify(rateLimiter, times(2)).tryAcquire();
    verify(stats).recordResourceUsed("r1", 1L);
  }

  @Test
  public void shouldTimeoutActiveState() throws Exception {
    setUp(5);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, time));

    now = now.plus(5, ChronoUnit.SECONDS);
    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(Event.timeout(INSTANCE_1));
  }

  @Test
  public void shouldNotTimeoutTerminalState() throws Exception {
    setUp(0);

    initWorkflow(workflowUsingResources(WORKFLOW_ID1));
    populateActiveStates(RunState.create(INSTANCE_1, State.DONE, time));

    scheduler.tick();
    verify(stateManager, never()).receiveIgnoreClosed(any());
  }

  @Test
  public void shouldNotTransitionIfNotTimedOut() throws Exception {
    setUp(20);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));
    populateActiveStates(RunState.create(INSTANCE_1, State.NEW, time));

    scheduler.tick();

    verify(stateManager, never()).receiveIgnoreClosed(any());
  }

  @Test
  public void shouldExecuteRetryIfDelayHasPassed() throws Exception {
    setUp(20);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));

    StateData stateData = StateData.newBuilder().retryDelayMillis(15_000L).tries(10).build();

    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, stateData, time));

    now = now.plus(15, ChronoUnit.SECONDS);
    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_1));
  }

  @Test
  public void shouldExecuteRetryIfDelayIsReset() throws Exception {
    setUp(20);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));

    StateData stateData = StateData.newBuilder().retryDelayMillis(15_000L).tries(10).build();
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, stateData, time));

    now = now.plus(10, ChronoUnit.SECONDS);
    scheduler.tick();

    verify(stateManager, never()).receiveIgnoreClosed(any());

    stateData = StateData.newBuilder().retryDelayMillis(0L).tries(10).build();
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, stateData, time));

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_1));
  }

  @Test
  public void shouldExecuteNewTriggers() throws Exception {
    setUp(20);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));

    StateData stateData = StateData.newBuilder().tries(0).build();
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, stateData, time));

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_1));
  }

  @Test
  public void shouldFailWhenUnknownResourceReference() throws Exception {
    setUp(20);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "unknown"));
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, time));

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(Event.runError(INSTANCE_1,
        "Referenced resources not found: [unknown]"));
  }

  @Test
  public void shouldIssueInfoIfGlobalResourceDepleted() throws Exception {
    setUp(20);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));
    when(config.globalConcurrency()).thenReturn(Optional.of(0L));
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, time));

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(Event.info(INSTANCE_1,
        Message.info("Resource limit reached for: [Resource{id=GLOBAL_STYX_CLUSTER, concurrency=0}]")));
  }

  @Test
  public void shouldIssueInfoIfResourceDepleted() throws Exception {
    setUp(20);
    setResourceLimit("r1", 0);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, time));

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(Event.info(INSTANCE_1,
        Message.info("Resource limit reached for: [Resource{id=r1, concurrency=0}]")));
  }

  @Test
  public void shouldFailWhenUnknownAndDepletedResources() throws Exception {
    setUp(20);
    setResourceLimit("r1", 0);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1", "r2", "r3"));

    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, time));

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(Event.runError(INSTANCE_1,
        "Referenced resources not found: [r2, r3]"));
  }

  @Test
  public void shouldIssueInfoOnceIfRepeated() throws Exception {
    setUp(20);
    setResourceLimit("r1", 0);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    StateData stateData = StateData.newBuilder()
        .addMessage(Message.info("Resource limit reached for: [Resource{id=r1, concurrency=0}]"))
        .build();
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, stateData, time));

    scheduler.tick();

    verify(stateManager, times(0)).receiveIgnoreClosed(
        Event.info(INSTANCE_1,
            Message.info("Resource limit reached for: [Resource{id=r1, concurrency=0}]")));
  }

  @Test
  public void shouldDequeueIfResourceValueIsIncreased() throws Exception {
    setUp(20);
    setResourceLimit("r1", 0);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, time));

    scheduler.tick();

    verify(stateManager, never()).receiveIgnoreClosed(Event.dequeue(INSTANCE_1));

    setResourceLimit("r1", 1);
    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_1));
  }

  @Test
  public void shouldLimitConcurrencyForResource() throws Exception {
    setUp(20);
    setResourceLimit("r1", 3);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));

    for (int i = 0; i < 4; i++) {
      populateActiveStates(RunState.create(instance(WORKFLOW_ID1, "i" + i), State.QUEUED, time));
    }

    scheduler.tick();

    ArgumentCaptor<Event> capturedEvents = ArgumentCaptor.forClass(Event.class);
    verify(stateManager, times(4)).receiveIgnoreClosed(capturedEvents.capture());
    issuedEvents(capturedEvents, "dequeue", 3);
    issuedEvents(capturedEvents, "info", 1);
    verify(stats).recordResourceUsed("r1", 3L);
  }

  @Test
  public void shouldCountResourcesOnStatesConsumingResources() throws Exception {
    setUp(20);
    setResourceLimit("r1", 3);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));

    // do not consume resources
    populateActiveStates(RunState.create(instance(WORKFLOW_ID1, "i0"), State.NEW, time));
    populateActiveStates(RunState.create(instance(WORKFLOW_ID1, "i1"), State.QUEUED, time));

    // consume resources
    populateActiveStates(RunState.create(instance(WORKFLOW_ID1, "i2"), State.SUBMITTING, time));
    populateActiveStates(RunState.create(instance(WORKFLOW_ID1, "i3"), State.PREPARE, time));
    populateActiveStates(RunState.create(instance(WORKFLOW_ID1, "i4"), State.TERMINATED, time));

    scheduler.tick();

    verify(stateManager, times(1)).receiveIgnoreClosed(any());
    verify(stateManager, times(1)).receiveIgnoreClosed(
        Event.info(instance(WORKFLOW_ID1, "i1"),
            Message.info("Resource limit reached for: [Resource{id=r1, concurrency=3}]")));
    verify(stats).recordResourceUsed("r1", 3L);
  }

  @Test
  public void shouldNotExceedResourceLimitsIfAlreadyAtLimit() throws Exception {
    setUp(20);
    setResourceLimit("r1", 3);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));

    // do not consume resources
    populateActiveStates(RunState.create(instance(WORKFLOW_ID1, "i0"), State.NEW, time));
    populateActiveStates(RunState.create(instance(WORKFLOW_ID1, "i1"), State.QUEUED, time));

    // consume resources
    populateActiveStates(RunState.create(instance(WORKFLOW_ID1, "i2"), State.PREPARE, time));
    populateActiveStates(RunState.create(instance(WORKFLOW_ID1, "i3"), State.PREPARE, time));
    populateActiveStates(RunState.create(instance(WORKFLOW_ID1, "i4"), State.PREPARE, time));

    scheduler.tick();

    verify(stateManager, never()).receiveIgnoreClosed(Event.dequeue(INSTANCE_1));
    verify(stats).recordResourceUsed("r1", 3L);

    scheduler.tick();

    verify(stateManager, never()).receiveIgnoreClosed(Event.dequeue(INSTANCE_1));
    verify(stats, times(2)).recordResourceUsed("r1", 3L);
  }

  @Test
  public void shouldLimitConcurrencyAcrossWorkflows() throws Exception {
    setUp(20);
    setResourceLimit("r1", 3);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    initWorkflow(workflowUsingResources(WORKFLOW_ID2, "r1"));

    for (int i = 0; i < 4; i++) {
      populateActiveStates(RunState.create(instance(WORKFLOW_ID1, "i" + i), State.NEW, time));
      populateActiveStates(RunState.create(instance(WORKFLOW_ID2, "i" + i), State.QUEUED, time));
    }

    ArgumentCaptor<Event> capturedEvents = ArgumentCaptor.forClass(Event.class);
    scheduler.tick();
    verify(stateManager, times(4)).receiveIgnoreClosed(capturedEvents.capture());

    issuedEvents(capturedEvents, "dequeue", 3, WORKFLOW_ID2);
    verify(stats).recordResourceUsed("r1", 3L);
  }

  @Test
  public void shouldLimitConcurrencyUsingMultipleResources() throws Exception {
    setUp(20);
    setResourceLimit("r1", 3);
    setResourceLimit("r2", 2);
    setResourceLimit("r3", 2);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1", "r2"));

    for (int i = 0; i < 4; i++) {
      populateActiveStates(RunState.create(instance(WORKFLOW_ID1, "i" + i), State.QUEUED, time));
    }

    ArgumentCaptor<Event> capturedEvents = ArgumentCaptor.forClass(Event.class);
    scheduler.tick();
    verify(stateManager, times(4)).receiveIgnoreClosed(capturedEvents.capture());

    issuedEvents(capturedEvents, "dequeue", 2, WORKFLOW_ID1);
    verify(stats).recordResourceUsed("r1", 2L);
    verify(stats).recordResourceUsed("r2", 2L);
  }

  @Test
  public void shouldLimitConcurrencyUsingMultipleResourcesAcrossWorkflows() throws Exception {
    setUp(20);
    setResourceLimit("r1", 3);
    setResourceLimit("r2", 2);
    setResourceLimit("common", 4);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1", "common"));
    initWorkflow(workflowUsingResources(WORKFLOW_ID2, "r2", "common"));

    for (int i = 0; i < 4; i++) {
      populateActiveStates(RunState.create(instance(WORKFLOW_ID1, "i" + i), State.QUEUED, time));
      populateActiveStates(RunState.create(instance(WORKFLOW_ID2, "i" + i), State.QUEUED, time));
    }

    ArgumentCaptor<Event> capturedEvents = ArgumentCaptor.forClass(Event.class);
    scheduler.tick();
    verify(stateManager, times(8)).receiveIgnoreClosed(capturedEvents.capture());

    issuedEvents(capturedEvents, "dequeue", 4);

    verify(stats).recordResourceUsed(eq("r1"), longThat(is(greaterThanOrEqualTo(1L))));
    verify(stats).recordResourceUsed(eq("r2"), longThat(is(greaterThanOrEqualTo(1L))));
    verify(stats).recordResourceUsed("common", 4L);
  }

  @Test
  public void shouldFreeResourcesWhenStatesComplete() throws Exception {
    shouldLimitConcurrencyUsingMultipleResourcesAcrossWorkflows();

    // Get events issued from previous test
    ArgumentCaptor<Event> capturedEvents = ArgumentCaptor.forClass(Event.class);
    verify(stateManager, times(8)).receiveIgnoreClosed(capturedEvents.capture());

    Set<WorkflowInstance> dequeuedInstances = capturedEvents.getAllValues().stream()
        .filter(event -> "dequeue".equals(EventUtil.name(event))).map(
            Event::workflowInstance).collect(toSet());

    long completed1 = dequeuedInstances.stream().filter(wfi -> wfi.workflowId().equals(WORKFLOW_ID1)).count();
    long completed2 = dequeuedInstances.stream().filter(wfi -> wfi.workflowId().equals(WORKFLOW_ID2)).count();
    assertThat(completed1 + completed2, is(4L));

    removeActiveStates(dequeuedInstances);

    // Capture new events from this test
    ArgumentCaptor<Event> newCapturedEvents = ArgumentCaptor.forClass(Event.class);
    scheduler.tick();
    verify(stateManager, times(12)).receiveIgnoreClosed(newCapturedEvents.capture());

    long expectedRuns1 = Math.min(4 - completed1, 3); // limit r1
    long expectedRuns2 = Math.min(4 - completed2, 2); // limit r2
    issuedEvents(newCapturedEvents, "dequeue", dequeuedInstances.size() + (expectedRuns1 + expectedRuns2));
  }


  @Test
  public void shouldDecorateWorkflowInstanceResources() throws Exception {
    setUp(20);

    Workflow workflow = workflowUsingResources(WORKFLOW_ID1, "foo", "bar");
    when(resourceDecorator.decorateResources(
        any(RunState.class), any(WorkflowConfiguration.class), anySetOf(String.class)))
        .thenReturn(ImmutableSet.of("baz", "quux", "GLOBAL_STYX_CLUSTER"));

    when(config.globalConcurrency()).thenReturn(Optional.of(17L));

    setResourceLimit("baz", 4);
    setResourceLimit("quux", 4);
    initWorkflow(workflow);
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, time));

    scheduler.tick();

    verify(resourceDecorator).decorateResources(any(RunState.class), eq(workflow.configuration()),
        eq(ImmutableSet.of("foo", "bar", "GLOBAL_STYX_CLUSTER")));

    verify(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_1));
  }

  @Test
  public void shouldLimitOnDecoratedWorkflowInstanceResourcesIfNotAvailable() throws Exception {
    setUp(20);

    Workflow workflow = workflowUsingResources(WORKFLOW_ID1, "foo", "bar");
    when(resourceDecorator.decorateResources(
        any(RunState.class), any(WorkflowConfiguration.class), anySetOf(String.class)))
        .thenReturn(ImmutableSet.of("baz", "GLOBAL_STYX_CLUSTER"));

    when(config.globalConcurrency()).thenReturn(Optional.of(17L));

    setResourceLimit("baz", 0);
    initWorkflow(workflow);
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, time));

    scheduler.tick();

    verify(resourceDecorator).decorateResources(any(RunState.class), eq(workflow.configuration()),
        eq(ImmutableSet.of("foo", "bar", "GLOBAL_STYX_CLUSTER")));

    verify(stateManager).receiveIgnoreClosed(Event.info(INSTANCE_1,
        Message.info("Resource limit reached for: [Resource{id=baz, concurrency=0}]")));

    verify(stateManager, never()).receiveIgnoreClosed(Event.dequeue(INSTANCE_1));
  }

  @Test
  public void shouldCountDecoratedResourcesOnNonQueuedStates() throws Exception {
    setUp(20);

    Workflow workflow = workflowUsingResources(WORKFLOW_ID1, "foo", "bar");
    when(resourceDecorator.decorateResources(
        any(RunState.class), any(WorkflowConfiguration.class), anySetOf(String.class)))
        .thenReturn(ImmutableSet.of("baz", "GLOBAL_STYX_CLUSTER"));

    when(config.globalConcurrency()).thenReturn(Optional.of(17L));

    setResourceLimit("baz", 4);
    initWorkflow(workflow);

    WorkflowInstance i0 = instance(WORKFLOW_ID1, "i0");
    WorkflowInstance i1 = instance(WORKFLOW_ID1, "i1");
    WorkflowInstance i2 = instance(WORKFLOW_ID1, "i2");
    WorkflowInstance i3 = instance(WORKFLOW_ID1, "i3");
    WorkflowInstance i4 = instance(WORKFLOW_ID1, "i4");

    populateActiveStates(
        RunState.create(i0, State.QUEUED, time),
        RunState.create(i1, State.SUBMITTING, time),
        RunState.create(i2, State.PREPARE, time),
        RunState.create(i3, State.TERMINATED, time),
        RunState.create(i4, State.QUEUED, time));

    scheduler.tick();

    // 3 invocations to count current resource usage + 2 invocations to calculate future usage for queued states
    verify(resourceDecorator, times(3 + 2)).decorateResources(any(RunState.class), eq(workflow.configuration()),
        eq(ImmutableSet.of("foo", "bar", "GLOBAL_STYX_CLUSTER")));

    verify(stateManager).receiveIgnoreClosed(Matchers.argThat(
        either(is(Event.dequeue(i0)))
            .or(is(Event.dequeue(i4)))));
  }

  @Test
  public void shouldRetryLaterOnExecutionBlockers() throws Exception {
    when(config.executionGatingEnabled()).thenReturn(true);

    final ExecutionBlocker blocker = ExecutionBlocker.of("missing dep", Duration.ofMinutes(17));
    when(gate.executionBlocker(any())).thenReturn(
        CompletableFuture.completedFuture(Optional.of(blocker)));

    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);

    setUp(TimeUnit.DAYS.toSeconds(2));
    initWorkflow(workflow);

    final StateData stateData = StateData.newBuilder().tries(0).build();
    final RunState runState = RunState.create(INSTANCE_1, State.QUEUED, stateData, time);

    populateActiveStates(runState);

    scheduler.tick();

    verify(gate).executionBlocker(INSTANCE_1);
    verify(stateManager).receiveIgnoreClosed(Event.retryAfter(INSTANCE_1, blocker.delay().toMillis()));
    verify(stateManager, never()).receiveIgnoreClosed(Event.dequeue(INSTANCE_1));

    now = now.plus(blocker.delay());
    when(gate.executionBlocker(any())).thenReturn(WorkflowExecutionGate.NO_BLOCKER);

    scheduler.tick();

    verify(gate, times(2)).executionBlocker(INSTANCE_1);

    verify(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_1));
  }

  @Test
  public void shouldNotGateExecutionIfDisabled() throws Exception {
    when(config.executionGatingEnabled()).thenReturn(false);

    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);

    setUp(20);
    initWorkflow(workflow);

    final StateData stateData = StateData.newBuilder().tries(0).build();
    final RunState runState = RunState.create(INSTANCE_1, State.QUEUED, stateData, time);

    populateActiveStates(runState);

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_1));
    verifyZeroInteractions(gate);
  }

  @Test
  public void shouldIgnoreGatingFailure() throws Exception {
    when(config.executionGatingEnabled()).thenReturn(true);

    when(gate.executionBlocker(any())).thenReturn(
        CompletableFutures.exceptionallyCompletedFuture(new Exception()));

    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);

    setUp(20);
    initWorkflow(workflow);

    final StateData stateData = StateData.newBuilder().tries(0).build();
    final RunState runState = RunState.create(INSTANCE_1, State.QUEUED, stateData, time);

    populateActiveStates(runState);

    scheduler.tick();

    verify(gate).executionBlocker(INSTANCE_1);

    verify(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_1));
    verifyZeroInteractions(gate);
  }

  private WorkflowInstance instance(WorkflowId id, String instanceId) {
    return WorkflowInstance.create(id, instanceId);
  }

  private void issuedEvents(ArgumentCaptor<Event> events, String eventType, long times) {
    assertThat(events.getAllValues()
            .stream()
            .filter(event -> eventType.equals(EventUtil.name(event)))
            .count(),
        is(times));
  }

  private void issuedEvents(ArgumentCaptor<Event> events, String eventType, long times, WorkflowId workflowId) {
    assertThat(events.getAllValues()
            .stream()
            .filter(event -> eventType.equals(EventUtil.name(event)))
            .filter(event -> event.workflowInstance().workflowId().equals(workflowId))
            .count(),
        is(times));
  }
}
