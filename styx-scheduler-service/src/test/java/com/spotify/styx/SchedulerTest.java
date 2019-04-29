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

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anySet;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anySetOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.RateLimiter;
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
import com.spotify.styx.state.StateTransitionConflictException;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.EventUtil;
import com.spotify.styx.util.ShardedCounter;
import com.spotify.styx.util.Time;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class SchedulerTest {

  private static final WorkflowId WORKFLOW_ID1 =
      WorkflowId.create("styx1", "example1");
  private static final WorkflowId WORKFLOW_ID2 =
      WorkflowId.create("styx2", "example2");
  private static final WorkflowInstance INSTANCE_1 =
      WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T01");
  private static final WorkflowInstance INSTANCE_2 =
      WorkflowInstance.create(WORKFLOW_ID2, "2016-12-02T01");

  private Scheduler scheduler;

  private Instant now = Instant.parse("2016-12-02T22:00:00Z");
  private Time time = () -> now;

  private List<Resource> resourceLimits = Lists.newArrayList();

  private ExecutorService executor = Executors.newCachedThreadPool();
  private ConcurrentMap<WorkflowInstance, RunState> activeStates = Maps.newConcurrentMap();

  private Map<WorkflowId, Workflow> workflows;

  @Mock WorkflowResourceDecorator resourceDecorator;
  @Mock RateLimiter rateLimiter;
  @Mock Stats stats;
  @Mock StyxConfig config;
  @Mock StateManager stateManager;
  @Mock Storage storage;
  @Mock ShardedCounter shardedCounter;
  @Mock Logger log;

  @Captor ArgumentCaptor<Event> eventCaptor = ArgumentCaptor.forClass(Event.class);

  @Before
  public void setUp() throws Exception {
    workflows = new HashMap<>();
    when(shardedCounter.counterHasSpareCapacity(anyString())).thenReturn(true);
    doNothing().when(stateManager).receiveIgnoreClosed(eventCaptor.capture(), anyLong());

    when(storage.resources()).thenReturn(resourceLimits);
    when(config.globalConcurrency()).thenReturn(Optional.empty());
    when(storage.config()).thenReturn(config);

    when(resourceDecorator.decorateResources(
        any(RunState.class), any(WorkflowConfiguration.class), anySet()))
        .thenAnswer(a -> a.getArgument(2));

    when(storage.workflow(any())).then(a -> Optional.ofNullable(workflows.get(a.<WorkflowId>getArgument(0))));

    when(stateManager.listActiveInstances()).thenReturn(activeStates.keySet());
    when(stateManager.getActiveState(any())).then(a ->
        Optional.ofNullable(activeStates.get(a.<WorkflowInstance>getArgument(0))));

    scheduler = new Scheduler(time, stateManager, storage, resourceDecorator,
        stats, rateLimiter, shardedCounter, executor, log);
  }

  @After
  public void tearDown() {
    executor.shutdownNow();
  }

  private void setResourceLimit(String resourceId, long limit) {
    resourceLimits.removeIf(r -> r.id().equals(resourceId));
    resourceLimits.add(Resource.create(resourceId, limit));
  }

  private void initWorkflow(Workflow workflow) {
    workflows.put(workflow.id(), workflow);
  }

  private void populateActiveStates(RunState... runStates) {
    for (RunState runState : runStates) {
      activeStates.put(runState.workflowInstance(), runState);
    }
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
    when(rateLimiter.acquire()).thenReturn(1.0);

    setResourceLimit("r1", 2);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, time.get()));

    scheduler.tick();

    verify(stats).recordResourceUsed("r1", 0L);
    verify(stateManager).receiveIgnoreClosed(
        eq(Event.dequeue(INSTANCE_1, ImmutableSet.of("r1"))), anyLong());
    verify(rateLimiter).acquire();
  }

  @Test
  public void shouldExecuteRetryIfDelayHasPassed() throws Exception {
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));

    StateData stateData = StateData.newBuilder().retryDelayMillis(15_000L).tries(10).build();

    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, stateData, time.get()));

    now = now.plus(15, ChronoUnit.SECONDS);
    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(eq(Event.dequeue(INSTANCE_1, ImmutableSet.of())), anyLong());
  }

  @Test
  public void shouldReadRunStateCounterForEachTick() throws Exception {
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));

    populateActiveStates(RunState.create(
        INSTANCE_1, State.QUEUED, StateData.zero(), time.get(), 15L));

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(any(Event.class), eq(15L));

    populateActiveStates(RunState.create(
        INSTANCE_1, State.QUEUED, StateData.zero(), time.get(), 23L));

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(any(Event.class), eq(23L));
  }

  @Test
  public void shouldExecuteRetryIfDelayIsReset() throws Exception {
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));

    StateData stateData = StateData.newBuilder().retryDelayMillis(15_000L).tries(10).build();
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, stateData, time.get()));

    now = now.plus(10, ChronoUnit.SECONDS);
    scheduler.tick();

    verify(stateManager, never()).receiveIgnoreClosed(any());

    stateData = StateData.newBuilder().retryDelayMillis(0L).tries(10).build();
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, stateData, time.get()));

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(eq(Event.dequeue(INSTANCE_1, ImmutableSet.of())), anyLong());
  }

  @Test
  public void shouldExecuteNewTriggers() throws Exception {
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));

    StateData stateData = StateData.newBuilder().tries(0).build();

    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, stateData, time.get()));

    List<WorkflowInstance> workflowInstances = new ArrayList<>();

    for (int i = 1; i <= 10; i++) {
      WorkflowId workflowId = WorkflowId.create("styx2", "example" + i);
      initWorkflow(workflowUsingResources(workflowId));
      WorkflowInstance instance = WorkflowInstance.create(workflowId, "2016-12-02T01");
      populateActiveStates(RunState.create(instance, State.QUEUED, stateData,
          time.get().minus(i, ChronoUnit.SECONDS)));
      workflowInstances.add(instance);
    }

    scheduler.tick();

    Lists.reverse(workflowInstances)
        .forEach(x -> verify(stateManager)
            .receiveIgnoreClosed(eq(Event.dequeue(x, ImmutableSet.of())), anyLong()));
    verify(stateManager).receiveIgnoreClosed(eq(
        Event.dequeue(INSTANCE_1, ImmutableSet.of())), anyLong());
  }

  @Test
  public void shouldDequeueEvenWhenMissingWorkflows() throws Exception {

    workflows.clear();

    StateData stateData = StateData.newBuilder().tries(0).build();

    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, stateData, time.get()));

    List<WorkflowInstance> workflowInstances = new ArrayList<>();

    for (int i = 1; i <= 10; i++) {
      WorkflowId workflowId = WorkflowId.create("styx2", "example" + i);
      WorkflowInstance instance = WorkflowInstance.create(workflowId, "2016-12-02T01");
      populateActiveStates(RunState.create(instance, State.QUEUED, stateData,
          time.get().minus(i, ChronoUnit.SECONDS)));
      workflowInstances.add(instance);
    }

    scheduler.tick();

    Lists.reverse(workflowInstances)
        .forEach(x -> verify(stateManager, timeout(30_000))
            .receiveIgnoreClosed(eq(Event.dequeue(x, ImmutableSet.of())), anyLong()));
    verify(stateManager, timeout(30_000)).receiveIgnoreClosed(eq(
        Event.dequeue(INSTANCE_1, ImmutableSet.of())), anyLong());
  }

  @Test
  public void shouldFailWhenUnknownResourceReference() throws Exception {
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "unknown"));
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, time.get()));

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(eq(Event.runError(INSTANCE_1,
        "Referenced resources not found: [unknown]")), anyLong());
  }

  @Test
  public void shouldFailWhenUnknownAndDepletedResources() throws Exception {
    setResourceLimit("r1", 0);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1", "r2", "r3"));

    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, time.get()));

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(
        eq(Event.runError(INSTANCE_1,
            "Referenced resources not found: [r2, r3]")),
        anyLong());
  }

  @Test
  public void shouldIssueInfoOnceIfRepeated() throws Exception {
    setResourceLimit("r1", 0);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    when(shardedCounter.counterHasSpareCapacity("r1")).thenReturn(false);

    InOrder inOrder = Mockito.inOrder(stateManager);

    StateData stateDataWithoutInfo = StateData.newBuilder()
        .build();
    RunState rsWithoutInfo = RunState.create(INSTANCE_1, State.QUEUED, stateDataWithoutInfo, time.get(), 17);
    activeStates.put(INSTANCE_1, rsWithoutInfo);

    scheduler.tick();

    inOrder.verify(stateManager).listActiveInstances();
    inOrder.verify(stateManager).getActiveState(INSTANCE_1);
    inOrder.verify(stateManager).receiveIgnoreClosed(
        Event.info(INSTANCE_1, Message.info("Resource limit reached for: [r1]")),
        rsWithoutInfo.counter());

    StateData stateDataWithInfo = StateData.newBuilder()
        .addMessage(Message.info("Resource limit reached for: [r1]"))
        .build();
    activeStates.put(INSTANCE_1, RunState.create(INSTANCE_1, State.QUEUED, stateDataWithInfo, time.get()));

    scheduler.tick();

    inOrder.verify(stateManager).listActiveInstances();
    inOrder.verify(stateManager).getActiveState(INSTANCE_1);
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  public void shouldRecordAggregateResourceUsageAndDemand() throws Exception {
    setResourceLimit("r1", 2);
    setResourceLimit("r2", 3);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    initWorkflow(workflowUsingResources(WORKFLOW_ID2, "r2"));

    // do not consume resources
    populateActiveStates(runStateWithResources(instance(WORKFLOW_ID1, "i0"), State.NEW, "r1"));
    populateActiveStates(runStateWithResources(instance(WORKFLOW_ID1, "i1"), State.QUEUED, "r1"));
    populateActiveStates(runStateWithResources(instance(WORKFLOW_ID1, "i5"), State.QUEUED, "r1"));
    populateActiveStates(runStateWithResources(instance(WORKFLOW_ID2, "i6"), State.QUEUED, "r2"));
    populateActiveStates(runStateWithResources(instance(WORKFLOW_ID1, "i4"), State.TERMINATED, "r1"));

    // consume resources
    populateActiveStates(runStateWithResources(instance(WORKFLOW_ID1, "i2"), State.SUBMITTING, "r1"));
    populateActiveStates(runStateWithResources(instance(WORKFLOW_ID1, "i3"), State.PREPARE, "r1"));

    scheduler.tick();

    verify(stats).recordResourceDemanded("r1", 2L);
    verify(stats).recordResourceDemanded("r2", 1L);
    verify(stats).recordResourceUsed("r1", 2L);
  }

  private RunState runStateWithResources(WorkflowInstance wfi, State state, String... resources) {
    var stateData = StateData.newBuilder().resourceIds(Set.of(resources)).build();
    return RunState.create(wfi, state, stateData, time.get());
  }

  @Test
  public void shouldNotExceedResourceLimitsIfAlreadyAtLimit() throws Exception {
    setResourceLimit("r1", 3);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    when(shardedCounter.counterHasSpareCapacity("r1")).thenReturn(false);

    // do not consume resources
    final WorkflowInstance i0 = instance(WORKFLOW_ID1, "i0");
    final WorkflowInstance i1 = instance(WORKFLOW_ID1, "i1");
    final RunState rs0 = RunState.create(i0, State.NEW, time.get(), 17);
    final RunState rs1 = RunState.create(i1, State.QUEUED, time.get(), 4711);
    populateActiveStates(rs0);
    populateActiveStates(rs1);

    // consume resources
    populateActiveStates(runStateWithResources(instance(WORKFLOW_ID1, "i2"), State.PREPARE, "r1"));
    populateActiveStates(runStateWithResources(instance(WORKFLOW_ID1, "i3"), State.PREPARE, "r1"));
    populateActiveStates(runStateWithResources(instance(WORKFLOW_ID1, "i4"), State.PREPARE, "r1"));

    scheduler.tick();

    verify(shardedCounter, times(1)).counterHasSpareCapacity("r1");
    assertThat(eventCaptor.getAllValues().stream()
        .anyMatch(e -> EventUtil.name(e).equals("dequeue")), is(false));
    verify(stats).recordResourceDemanded("r1", 1L);
    verify(stats).recordResourceUsed("r1", 3L);

    scheduler.tick();

    verify(stateManager, times(2)).listActiveInstances();
    verify(shardedCounter, times(2)).counterHasSpareCapacity("r1");
    assertThat(eventCaptor.getAllValues().stream()
        .anyMatch(e -> EventUtil.name(e).equals("dequeue")), is(false));
    verify(stats, times(2)).recordResourceDemanded("r1", 1L);
    verify(stats, times(2)).recordResourceUsed("r1", 3L);
  }

  @Test
  public void shouldCacheResourceUsageExceededLookup() throws Exception {
    setResourceLimit("r1", 2);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    when(shardedCounter.counterHasSpareCapacity("r1")).thenReturn(false);

    final WorkflowInstance i0 = instance(WORKFLOW_ID1, "i0");
    final WorkflowInstance i1 = instance(WORKFLOW_ID1, "i1");
    final RunState rs0 = RunState.create(i0, State.QUEUED, time.get(), 17);
    final RunState rs1 = RunState.create(i1, State.QUEUED, time.get(), 4711);
    populateActiveStates(rs0);
    populateActiveStates(rs1);

    scheduler.tick();

    verify(shardedCounter, times(1)).counterHasSpareCapacity("r1");
    verifyNoMoreInteractions(shardedCounter);
  }

  @Test
  public void shouldHandleResourceUsageExceededLookupFailure() throws Exception {
    setResourceLimit("r1", 2);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    when(shardedCounter.counterHasSpareCapacity("r1")).thenThrow(new RuntimeException("error!"));

    final WorkflowInstance i0 = instance(WORKFLOW_ID1, "i0");
    final RunState rs0 = RunState.create(i0, State.QUEUED, time.get(), 17);
    populateActiveStates(rs0);

    scheduler.tick();

    verify(shardedCounter, times(1)).counterHasSpareCapacity("r1");
    verify(stateManager).receiveIgnoreClosed(Event.dequeue(i0, ImmutableSet.of("r1")), 17);
  }

  @Test
  public void shouldDecorateWorkflowInstanceResources() throws Exception {

    Workflow workflow = workflowUsingResources(WORKFLOW_ID1, "foo", "bar");
    when(resourceDecorator.decorateResources(
        any(RunState.class), any(WorkflowConfiguration.class), anySetOf(String.class)))
        .thenReturn(ImmutableSet.of("baz", "quux", "GLOBAL_STYX_CLUSTER"));

    when(config.globalConcurrency()).thenReturn(Optional.of(17L));

    setResourceLimit("baz", 4);
    setResourceLimit("quux", 4);
    initWorkflow(workflow);
    populateActiveStates(RunState.create(INSTANCE_1, State.QUEUED, time.get()));

    scheduler.tick();

    verify(resourceDecorator).decorateResources(any(RunState.class), eq(workflow.configuration()),
        eq(ImmutableSet.of("foo", "bar", "GLOBAL_STYX_CLUSTER")));

    verify(stateManager).receiveIgnoreClosed(eq(Event.dequeue(INSTANCE_1,
        ImmutableSet.of("baz", "quux", "GLOBAL_STYX_CLUSTER"))), anyLong());
  }

  @Test
  public void shouldCountDecoratedResourcesOnNonQueuedStates() throws Exception {

    Workflow workflow = workflowUsingResources(WORKFLOW_ID1, "foo", "bar");
    when(resourceDecorator.decorateResources(
        any(RunState.class), any(WorkflowConfiguration.class), anySetOf(String.class)))
        .thenReturn(ImmutableSet.of("baz", "GLOBAL_STYX_CLUSTER"));

    when(config.globalConcurrency()).thenReturn(Optional.of(17L));

    setResourceLimit("baz", 3);
    initWorkflow(workflow);

    WorkflowInstance i0 = instance(WORKFLOW_ID1, "i0");
    WorkflowInstance i1 = instance(WORKFLOW_ID1, "i1");
    WorkflowInstance i2 = instance(WORKFLOW_ID1, "i2");
    WorkflowInstance i3 = instance(WORKFLOW_ID1, "i3");
    WorkflowInstance i4 = instance(WORKFLOW_ID1, "i4");

    populateActiveStates(
        RunState.create(i0, State.QUEUED, time.get()),
        RunState.create(i1, State.SUBMITTING, time.get()),
        RunState.create(i2, State.PREPARE, time.get()),
        RunState.create(i3, State.TERMINATED, time.get()),
        RunState.create(i4, State.QUEUED, time.get()));

    scheduler.tick();

    // TODO: verify that decorateResources was called for the expected instances
    // Resources are decorated on dequeue
    verify(resourceDecorator, times(2)).decorateResources(any(RunState.class), eq(workflow.configuration()),
        eq(ImmutableSet.of("foo", "bar", "GLOBAL_STYX_CLUSTER")));

    verify(stateManager, times(2)).receiveIgnoreClosed(argThat(
        either(is(Event.dequeue(i0, ImmutableSet.of("baz", "GLOBAL_STYX_CLUSTER"))))
            .or(is(Event.dequeue(i4, ImmutableSet.of("baz", "GLOBAL_STYX_CLUSTER"))))),
        anyLong());
  }

  @Test
  public void shouldHandleDequeueFailures() throws Exception {
    var workflow1 = workflowUsingResources(WORKFLOW_ID1);
    var workflow2 = workflowUsingResources(WORKFLOW_ID2);

    initWorkflow(workflow1);
    initWorkflow(workflow2);

    var counter1 = 17;
    var stateData1 = StateData.newBuilder().tries(0).build();
    var runState1 = RunState.create(INSTANCE_1, State.QUEUED, stateData1, time.get(), counter1);

    var counter2 = 4711;
    var stateData2 = StateData.newBuilder().tries(0).build();
    var runState2 = RunState.create(INSTANCE_2, State.QUEUED, stateData2, time.get(), counter2);

    populateActiveStates(runState1, runState2);

    doThrow(new RuntimeException("fail!"))
        .when(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_1, ImmutableSet.of()), counter1);

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_1, ImmutableSet.of()), counter1);
    verify(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_2, ImmutableSet.of()), counter2);

    verify(stats).recordTickDuration(any(), anyLong());
  }


  @Test
  public void shouldHandleStateTransitionConflicts() throws Exception {
    var workflow1 = workflowUsingResources(WORKFLOW_ID1);
    var workflow2 = workflowUsingResources(WORKFLOW_ID2);

    initWorkflow(workflow1);
    initWorkflow(workflow2);

    var counter1 = 17;
    var stateData1 = StateData.newBuilder().tries(0).build();
    var runState1 = RunState.create(INSTANCE_1, State.QUEUED, stateData1, time.get(), counter1);

    var counter2 = 4711;
    var stateData2 = StateData.newBuilder().tries(0).build();
    var runState2 = RunState.create(INSTANCE_2, State.QUEUED, stateData2, time.get(), counter2);

    populateActiveStates(runState1, runState2);
    var cause = new StateTransitionConflictException("conflict!");
    doThrow(cause).when(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_1, ImmutableSet.of()), counter1);

    scheduler.tick();

    verify(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_1, ImmutableSet.of()), counter1);
    verify(stateManager).receiveIgnoreClosed(Event.dequeue(INSTANCE_2, ImmutableSet.of()), counter2);

    verify(stats).recordTickDuration(any(), anyLong());

    verify(log).debug("State transition conflict when scheduling instance: {}", INSTANCE_1, cause);
  }

  private WorkflowInstance instance(WorkflowId id, String instanceId) {
    return WorkflowInstance.create(id, instanceId);
  }
}
