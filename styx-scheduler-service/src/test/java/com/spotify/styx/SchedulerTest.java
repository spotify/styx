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
import static java.util.Optional.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.DataEndpoint;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.state.TimeoutConfig;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.ParameterUtil;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class SchedulerTest {

  private static final WorkflowId WORKFLOW_ID1 =
      WorkflowId.create("styx1", "example1");
  private static final WorkflowId WORKFLOW_ID2 =
      WorkflowId.create("styx2", "example2");
  private static final WorkflowInstance INSTANCE =
      WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02");

  private static final Backfill BACKFILL_1 = Backfill.newBuilder()
      .id("backfill-1")
      .start(Instant.parse("2016-12-02T22:00:00Z"))
      .end(Instant.parse("2016-12-05T22:00:00Z"))
      .workflowId(WORKFLOW_ID1)
      .concurrency(2)
      .partitioning(Partitioning.HOURS)
      .nextTrigger(Instant.parse("2016-12-02T22:00:00Z"))
      .build();

  private static final Backfill BACKFILL_2 = Backfill.newBuilder()
      .id("backfill-2")
      .start(Instant.parse("2016-12-02T00:00:00Z"))
      .end(Instant.parse("2016-12-02T03:00:00Z"))
      .workflowId(WORKFLOW_ID1)
      .concurrency(3)
      .partitioning(Partitioning.HOURS)
      .nextTrigger(Instant.parse("2016-12-02T00:00:00Z"))
      .build();

  WorkflowCache workflowCache;
  Storage storage;
  StateManager stateManager;
  Scheduler scheduler;
  TriggerListener triggerListener;

  Instant now = Instant.parse("2016-12-02T22:00:00Z");
  Time time = () -> now;

  List<Resource> resourceLimits = Lists.newArrayList();

  private void setUp(int timeoutSeconds) throws StateManager.IsClosed, IOException {
    workflowCache = new InMemWorkflowCache();
    TimeoutConfig timeoutConfig = createWithDefaultTtl(ofSeconds(timeoutSeconds));

    storage = mock(Storage.class);
    triggerListener = mock(TriggerListener.class);
    when(storage.resources()).thenReturn(resourceLimits);

    stateManager = new SyncStateManager();
    scheduler = new Scheduler(time, timeoutConfig, stateManager, workflowCache, storage,
                              triggerListener);
  }

  private void setResourceLimit(String resourceId, long limit) {
    resourceLimits.removeIf(r -> r.id().equals(resourceId));
    resourceLimits.add(Resource.create(resourceId, limit));
  }

  private void initWorkflow(Workflow workflow) {
    workflowCache.store(workflow);
  }

  private void init(RunState runState) throws StateManager.IsClosed {
    stateManager.initialize(runState);
  }

  Workflow workflowUsingResources(WorkflowId id, String... resources) {
    return Workflow.create(
        id.componentId(),
        URI.create("http://example.com"),
        DataEndpoint.create(
            id.endpointId(), Partitioning.HOURS, empty(), empty(), empty(),
            Arrays.asList(resources)));
  }

  @Test
  public void shouldTriggerBackfillsNew() throws Exception {
    setUp(5);
    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);
    initWorkflow(workflow);
    final int concurrency = BACKFILL_1.concurrency();
    when(storage.backfills()).thenReturn(Collections.singletonList(BACKFILL_1));

    scheduler.tick();

    final List<Instant> instants =
        ParameterUtil.rangeOfInstants(BACKFILL_1.start(), BACKFILL_1.end(),
                                      workflow.schedule().partitioning());

    instants.stream().limit(concurrency).forEach(
        instant ->
            verify(triggerListener).event(workflow, Trigger.backfill(BACKFILL_1.id()), instant));

    verify(storage)
        .storeBackfill(BACKFILL_1.builder().nextTrigger(instants.get(concurrency)).build());
  }

  @Test
  public void shouldTriggerBackfillsInProgress() throws Exception {
    setUp(5);
    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);
    initWorkflow(workflow);
    when(storage.backfills()).thenReturn(
        Collections.singletonList(BACKFILL_1.builder()
            .nextTrigger(Instant.parse("2016-12-03T00:00:00Z"))
            .build()));

    stateManager.initialize
        (RunState.fresh(
            WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T23")));
    stateManager.receive(
        Event.triggerExecution(
            WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T23"),
            Trigger.backfill("backfill-1")));

    scheduler.tick();

    verify(triggerListener, only()).event(
        workflow,
        Trigger.backfill(BACKFILL_1.id()), Instant.parse("2016-12-03T00:00:00Z"));
  }

  @Test
  public void shouldNotTriggerCompletedBackfillsAndUpdateCompleted() throws Exception {
    setUp(5);
    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);
    initWorkflow(workflow);
    Backfill backfillWithNoPartitionsLeft = BACKFILL_2.builder().nextTrigger(BACKFILL_2.end()).build();
    when(storage.backfills()).thenReturn(Collections.singletonList(backfillWithNoPartitionsLeft));

    scheduler.tick();

    verifyZeroInteractions(triggerListener);
    final Backfill completedBackfill = backfillWithNoPartitionsLeft.builder().allTriggered(true).build();
    verify(storage).storeBackfill(completedBackfill);
  }

  @Test
  public void shouldNotTriggerBackfillsIfResourceLimit() throws Exception {
    setUp(5);
    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);
    initWorkflow(workflow);
    when(storage.backfills()).thenReturn(Collections.singletonList(BACKFILL_1));

    stateManager.initialize(
        RunState.fresh(
            WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T22")));
    stateManager.receive(
        Event.triggerExecution(
            WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T22"),
            Trigger.backfill("backfill-1")));
    stateManager.initialize
        (RunState.fresh(
            WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T23")));
    stateManager.receive(
        Event.triggerExecution(
            WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T23"),
            Trigger.backfill("backfill-1")));

    scheduler.tick();

    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void shouldNotTriggerHaltedBackfills() throws Exception {
    setUp(5);
    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);
    initWorkflow(workflow);
    when(storage.backfills()).thenReturn(Collections.singletonList(
        BACKFILL_1.builder().halted(true).build()));

    scheduler.tick();

    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void shouldNotTriggerCompletedBackfills() throws Exception {
    setUp(5);
    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);
    initWorkflow(workflow);
    when(storage.backfills()).thenReturn(Collections.singletonList(
        BACKFILL_1.builder().allTriggered(true).build()));

    scheduler.tick();

    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void shouldNotTriggerBackfillsWithMissingWorkflows() throws Exception {
    setUp(5);
    when(storage.backfills()).thenReturn(Collections.singletonList(BACKFILL_1));

    scheduler.tick();

    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void shouldTimeoutActiveState() throws Exception {
    setUp(5);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));
    init(RunState.fresh(INSTANCE, time));

    now = now.plus(5, ChronoUnit.SECONDS);
    scheduler.tick();

    assertThat(stateManager.get(INSTANCE).state(), is(State.FAILED));
  }

  @Test
  public void shouldNotTimeoutTerminalState() throws Exception {
    setUp(0);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));
    init(RunState.create(INSTANCE, State.DONE, time));

    scheduler.tick();

    assertThat(stateManager.get(INSTANCE).state(), is(State.DONE));
  }

  @Test
  public void shouldNotTransitionIfNotTimedOut() throws Exception {
    setUp(20);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));
    init(RunState.fresh(INSTANCE, time));

    scheduler.tick();

    assertThat(stateManager.get(INSTANCE).state(), is(State.NEW));
  }

  @Test
  public void shouldExecuteRetryIfDelayHasPassed() throws Exception {
    setUp(20);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));

    StateData stateData = StateData.newBuilder().retryDelayMillis(15_000L).tries(10).build();
    init(RunState.create(INSTANCE, State.QUEUED, stateData, time));

    now = now.plus(15, ChronoUnit.SECONDS);
    scheduler.tick();

    assertThat(stateManager.get(INSTANCE).state(), is(State.PREPARE));
  }

  @Test
  public void shouldExecuteNewTriggers() throws Exception {
    setUp(20);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1));

    StateData stateData = StateData.newBuilder().tries(0).build();
    init(RunState.create(INSTANCE, State.QUEUED, stateData, time));

    scheduler.tick();

    assertThat(stateManager.get(INSTANCE).state(), is(State.PREPARE));
  }

  @Test
  public void shouldFailWhenUnknownResourceReference() throws Exception {
    setUp(20);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "unknown"));
    init(RunState.create(INSTANCE, State.QUEUED, time));

    scheduler.tick();

    assertThat(
    stateManager.get(INSTANCE).data().messages().get(0).line(),
        is("Referenced resources not found: [unknown]"));
    assertThat(stateManager.get(INSTANCE).state(), is(State.FAILED));
  }

  @Test
  public void shouldIssueInfoIfResourceDepleted() throws Exception {
    setUp(20);
    setResourceLimit("r1", 0);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    init(RunState.create(INSTANCE, State.QUEUED, time));

    scheduler.tick();

    assertThat(
        stateManager.get(INSTANCE).data().messages().get(0).line(),
        is("Resource limit reached for: [Resource{id=r1, concurrency=0}]"));
    assertThat(stateManager.get(INSTANCE).state(), is(State.QUEUED));
  }

  @Test
  public void shouldFailWhenUnknownAndDepletedResources() throws Exception {
    setUp(20);
    setResourceLimit("r1", 0);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1", "r2", "r3"));

    init(RunState.create(INSTANCE, State.QUEUED, time));

    scheduler.tick();

    assertThat(
        stateManager.get(INSTANCE).data().messages().get(0).line(),
        is("Referenced resources not found: [r2, r3]"));
    assertThat(stateManager.get(INSTANCE).state(), is(State.FAILED));
  }

  @Test
  public void shouldIssueInfoOnceIfRepeated() throws Exception {
    setUp(20);
    setResourceLimit("r1", 0);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    init(RunState.create(INSTANCE, State.QUEUED, time));

    scheduler.tick();
    scheduler.tick();

    assertThat(stateManager.get(INSTANCE).data().messages().size(), is(1));
    assertThat(stateManager.get(INSTANCE).state(), is(State.QUEUED));
  }

  @Test
  public void shouldDequeueIfResourceValueIsIncreased() throws Exception {
    setUp(20);
    setResourceLimit("r1", 0);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    init(RunState.create(INSTANCE, State.QUEUED, time));

    scheduler.tick();

    assertThat(stateManager.get(INSTANCE).state(), is(State.QUEUED));

    setResourceLimit("r1", 1);
    scheduler.tick();

    assertThat(stateManager.get(INSTANCE).state(), is(State.PREPARE));
  }

  @Test
  public void shouldLimitConcurrencyForResource() throws Exception {
    setUp(20);
    setResourceLimit("r1", 3);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));

    for (int i = 0; i < 4; i++) {
      init(RunState.create(instance(WORKFLOW_ID1, "i" + i), State.QUEUED, time));
    }

    scheduler.tick();

    assertThat(countInState(State.QUEUED), is(1));
    assertThat(countInState(State.PREPARE), is(3));
  }

  @Test
  public void shouldCountResourcesOnNonQueuedStates() throws Exception {
    setUp(20);
    setResourceLimit("r1", 3);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));

    init(RunState.create(instance(WORKFLOW_ID1, "i0"), State.QUEUED, time));
    init(RunState.create(instance(WORKFLOW_ID1, "i1"), State.SUBMITTING, time));
    init(RunState.create(instance(WORKFLOW_ID1, "i2"), State.PREPARE, time));
    init(RunState.create(instance(WORKFLOW_ID1, "i3"), State.TERMINATED, time));

    scheduler.tick();

    assertThat(countInState(State.QUEUED), is(1));
  }

  @Test
  public void shouldNotExceedResourceLimitsIfAlreadyAtLimit() throws Exception {
    setUp(20);
    setResourceLimit("r1", 3);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));

    init(RunState.create(instance(WORKFLOW_ID1, "i0"), State.QUEUED, time));
    init(RunState.create(instance(WORKFLOW_ID1, "i1"), State.PREPARE, time));
    init(RunState.create(instance(WORKFLOW_ID1, "i2"), State.PREPARE, time));
    init(RunState.create(instance(WORKFLOW_ID1, "i3"), State.PREPARE, time));

    scheduler.tick();

    assertThat(countInState(State.QUEUED), is(1));
  }

  @Test
  public void shouldLimitConcurrencyAcrossWorkflows() throws Exception {
    setUp(20);
    setResourceLimit("r1", 3);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1"));
    initWorkflow(workflowUsingResources(WORKFLOW_ID2, "r1"));

    for (int i = 0; i < 4; i++) {
      init(RunState.create(instance(WORKFLOW_ID1, "i" + i), State.QUEUED, time));
      init(RunState.create(instance(WORKFLOW_ID2, "i" + i), State.QUEUED, time));
    }

    scheduler.tick();

    assertThat(countInState(State.QUEUED), is(5));
    assertThat(countInState(State.PREPARE), is(3));
  }

  @Test
  public void shouldLimitConcurrencyUsingMultipleResources() throws Exception {
    setUp(20);
    setResourceLimit("r1", 3);
    setResourceLimit("r2", 2);
    initWorkflow(workflowUsingResources(WORKFLOW_ID1, "r1", "r2"));

    for (int i = 0; i < 4; i++) {
      init(RunState.create(instance(WORKFLOW_ID1, "i" + i), State.QUEUED, time));
    }

    scheduler.tick();

    assertThat(countInState(State.QUEUED), is(2));
    assertThat(countInState(State.PREPARE), is(2));
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
      init(RunState.create(instance(WORKFLOW_ID1, "i" + i), State.QUEUED, time));
      init(RunState.create(instance(WORKFLOW_ID2, "i" + i), State.QUEUED, time));
    }

    scheduler.tick();

    assertThat(countInState(State.QUEUED), is(4));
    assertThat(countInState(State.PREPARE), is(4));
    assertThat(countInState(WORKFLOW_ID1, State.PREPARE), is(greaterThanOrEqualTo(2)));
    assertThat(countInState(WORKFLOW_ID2, State.PREPARE), is(greaterThanOrEqualTo(1)));
  }

  @Test
  public void shouldFreeResourcesWhenStatesComplete() throws Exception {
    shouldLimitConcurrencyUsingMultipleResourcesAcrossWorkflows();

    int completed1 = 0;
    int completed2 = 0;
    for (int i = 0; i < 4; i++) {
      RunState runState = stateManager.get(instance(WORKFLOW_ID1, "i" + i));
      if (runState.state() == State.PREPARE) {
        stateManager.receiveIgnoreClosed(Event.halt(runState.workflowInstance()));
        completed1++;
      }
      runState = stateManager.get(instance(WORKFLOW_ID2, "i" + i));
      if (runState.state() == State.PREPARE) {
        stateManager.receiveIgnoreClosed(Event.halt(runState.workflowInstance()));
        completed2++;
      }
    }

    assertThat(completed1 + completed2, is(4));

    scheduler.tick();

    int expectedRuns1 = Math.min(4 - completed1, 3); // limit r1
    int expectedRuns2 = Math.min(4 - completed2, 2); // limit r2

    assertThat(countInState(State.QUEUED), is(4 - expectedRuns1 - expectedRuns2));
    assertThat(countInState(State.PREPARE), is(expectedRuns1 + expectedRuns2));
  }

  private WorkflowInstance instance(WorkflowId id, String instanceId) {
    return WorkflowInstance.create(id, instanceId);
  }

  private int countInState(State state) {
    return (int) stateManager.activeStates().values().stream()
        .filter(runState -> runState.state() == state)
        .count();
  }

  private int countInState(WorkflowId workflowId, State state) {
    return (int) stateManager.activeStates().values().stream()
        .filter(runState -> runState.workflowInstance().workflowId().equals(workflowId))
        .filter(runState -> runState.state() == state)
        .count();
  }
}
