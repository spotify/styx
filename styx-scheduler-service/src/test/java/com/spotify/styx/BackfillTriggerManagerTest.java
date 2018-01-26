/*-
 * -\-\-
 * Spotify styx
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
package com.spotify.styx;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.ParameterUtil;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BackfillTriggerManagerTest {

  private static final WorkflowId WORKFLOW_ID1 =
      WorkflowId.create("styx1", "example1");

  private static final Backfill BACKFILL_1 = Backfill.newBuilder()
      .id("backfill-1")
      .start(Instant.parse("2016-12-02T22:00:00Z"))
      .end(Instant.parse("2016-12-05T22:00:00Z"))
      .workflowId(WORKFLOW_ID1)
      .concurrency(2)
      .schedule(Schedule.HOURS)
      .nextTrigger(Instant.parse("2016-12-02T22:00:00Z"))
      .build();

  private static final Backfill BACKFILL_2 = Backfill.newBuilder()
      .id("backfill-2")
      .start(Instant.parse("2016-12-02T00:00:00Z"))
      .end(Instant.parse("2016-12-02T03:00:00Z"))
      .workflowId(WORKFLOW_ID1)
      .concurrency(3)
      .schedule(Schedule.HOURS)
      .nextTrigger(Instant.parse("2016-12-02T00:00:00Z"))
      .build();

  private List<Resource> resourceLimits = Lists.newArrayList();

  @Mock
  private TriggerListener triggerListener;

  @Mock
  private Storage storage;

  @Mock StyxConfig config;

  private WorkflowCache workflowCache;

  private BackfillTriggerManager backfillTriggerManager;

  private SyncStateManager stateManager;

  private ExecutorService executor = Executors.newCachedThreadPool();

  @Before
  public void setUp() throws Exception {
    when(triggerListener.event(any(Workflow.class), any(Trigger.class), any(Instant.class)))
        .then(a -> CompletableFuture.completedFuture(null));
    when(storage.resources()).thenReturn(resourceLimits);
    when(config.globalConcurrency()).thenReturn(Optional.empty());
    when(storage.config()).thenReturn(config);

    stateManager = new SyncStateManager();
    workflowCache = new InMemWorkflowCache();
    backfillTriggerManager = new BackfillTriggerManager(stateManager, workflowCache, storage,
                                                        triggerListener);
  }

  @After
  public void tearDown() throws Exception {
    executor.shutdownNow();
  }

  @Test
  public void shouldTriggerBackfillsNew() throws Exception {
    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);
    initWorkflow(workflow);
    final int concurrency = BACKFILL_1.concurrency();
    when(storage.backfills(anyBoolean())).thenReturn(Collections.singletonList(BACKFILL_1));

    backfillTriggerManager.tick();

    final List<Instant> instants =
        ParameterUtil.rangeOfInstants(BACKFILL_1.start(), BACKFILL_1.end(),
                                      workflow.configuration().schedule());

    instants.stream().limit(concurrency).forEach(
        instant ->
            verify(triggerListener).event(workflow, Trigger.backfill(BACKFILL_1.id()), instant));

    verify(storage)
        .storeBackfill(BACKFILL_1.builder().nextTrigger(instants.get(concurrency)).build());
  }

  @Test
  public void shouldTriggerBackfillsInProgress() throws Exception {
    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);
    initWorkflow(workflow);
    when(storage.backfills(anyBoolean())).thenReturn(
        Collections.singletonList(BACKFILL_1.builder()
                                      .nextTrigger(Instant.parse("2016-12-03T00:00:00Z"))
                                      .build()));

    stateManager.trigger
        (RunState.fresh(
            WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T23")), trigger);
    stateManager.receive(
        Event.triggerExecution(
            WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T23"),
            Trigger.backfill("backfill-1")));

    backfillTriggerManager.tick();

    verify(triggerListener, only()).event(
        workflow,
        Trigger.backfill(BACKFILL_1.id()), Instant.parse("2016-12-03T00:00:00Z"));
  }

  @Test
  public void shouldTriggerBackfillsToCompletion() throws Exception {
    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);
    initWorkflow(workflow);
    when(storage.backfills(anyBoolean())).thenReturn(Collections.singletonList(BACKFILL_2));

    backfillTriggerManager.tick();

    List<Instant> instants =
        ParameterUtil.rangeOfInstants(BACKFILL_2.start(), BACKFILL_2.end(), BACKFILL_2.schedule());
    instants.forEach(
        instant ->
            verify(triggerListener).event(workflow, Trigger.backfill(BACKFILL_2.id()), instant));

    final Backfill completedBackfill =
        BACKFILL_2.builder().nextTrigger(BACKFILL_2.end()).allTriggered(true).build();
    verify(storage).storeBackfill(completedBackfill);
  }

  @Test
  public void shouldNotTriggerCompletedBackfillsAndUpdateCompleted() throws Exception {
    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);
    initWorkflow(workflow);
    Backfill backfillWithNoPartitionsLeft = BACKFILL_2.builder().nextTrigger(BACKFILL_2.end()).build();
    when(storage.backfills(anyBoolean())).thenReturn(Collections.singletonList(backfillWithNoPartitionsLeft));

    backfillTriggerManager.tick();

    verifyZeroInteractions(triggerListener);
    final Backfill completedBackfill = backfillWithNoPartitionsLeft.builder().allTriggered(true).build();
    verify(storage).storeBackfill(completedBackfill);
  }

  @Test
  public void shouldNotTriggerBackfillsIfResourceLimit() throws Exception {
    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);
    initWorkflow(workflow);
    when(storage.backfills(anyBoolean())).thenReturn(Collections.singletonList(BACKFILL_1));

    stateManager.trigger(
        RunState.fresh(
            WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T22")), trigger);
    stateManager.receive(
        Event.triggerExecution(
            WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T22"),
            Trigger.backfill("backfill-1")));
    stateManager.trigger
        (RunState.fresh(
            WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T23")), trigger);
    stateManager.receive(
        Event.triggerExecution(
            WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T23"),
            Trigger.backfill("backfill-1")));

    backfillTriggerManager.tick();

    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void shouldNotTriggerBackfillsWithMissingWorkflows() throws Exception {
    when(storage.backfills(anyBoolean())).thenReturn(Collections.singletonList(BACKFILL_1));

    backfillTriggerManager.tick();

    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void shouldTriggerAndUpdateBackfillsSynchronously() throws Exception {
    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);
    initWorkflow(workflow);
    when(storage.backfills(anyBoolean())).thenReturn(Collections.singletonList(BACKFILL_1));

    // Collect unfinished triggering futures as the trigger listener is called
    final BlockingQueue<CompletableFuture<Void>> triggerProcessingFutures =
        new LinkedBlockingQueue<>();

    when(triggerListener.event(
        any(Workflow.class), any(Trigger.class), any(Instant.class)))
        .then(a -> {
          CompletableFuture<Void> processed = new CompletableFuture<>();
          triggerProcessingFutures.add(processed);
          return processed;
        });

    // Run a single tick of the backfillTriggerManager
    executor.execute(backfillTriggerManager::tick);

    final List<Instant> instants =
        ParameterUtil.rangeOfInstants(BACKFILL_1.start(), BACKFILL_1.end(),
                                      workflow.configuration().schedule());

    // Go through each expected trigger sequentially and verify that the next partition is not
    // triggered before the future for the previous partition trigger is completed
    for (int i = 0; i < BACKFILL_1.concurrency(); i++) {
      final Instant instant = instants.get(i);

      final CompletableFuture<Void> triggerProcessed = triggerProcessingFutures.poll(1,
                                                                                     TimeUnit.MINUTES);
      assertThat(triggerProcessingFutures.isEmpty(), is(true));

      verify(triggerListener, timeout(60_000))
          .event(any(Workflow.class), any(Trigger.class), eq(instant));

      triggerProcessed.complete(null);
      verify(storage, timeout(60_000))
          .storeBackfill(BACKFILL_1.builder().nextTrigger(instants.get(i + 1)).build());
    }
  }

  @Test
  public void shouldNotContinueTriggeringBackfillsIfTriggerFails() throws Exception {
    final Workflow workflow = workflowUsingResources(WORKFLOW_ID1);
    initWorkflow(workflow);
    when(storage.backfills(anyBoolean())).thenReturn(Collections.singletonList(BACKFILL_1));

    RuntimeException rootCause = new RuntimeException("trigger listener failure!");

    when(triggerListener.event(
        any(Workflow.class), any(Trigger.class), any(Instant.class)))
        .thenThrow(rootCause);

    try {
      backfillTriggerManager.tick();
      fail();
    } catch (Exception e) {
      assertThat(Throwables.getRootCause(e), is(sameInstance(rootCause)));
    }

    verify(triggerListener, timeout(60_000))
        .event(any(Workflow.class), any(Trigger.class), any(Instant.class));

    verifyNoMoreInteractions(triggerListener);
  }

  private void initWorkflow(Workflow workflow) {
    workflowCache.store(workflow);
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
}
