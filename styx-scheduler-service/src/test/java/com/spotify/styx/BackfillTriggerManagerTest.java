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

import static com.spotify.styx.util.ParameterUtil.toParameter;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.spotify.futures.CompletableFutures;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import com.spotify.styx.storage.TransactionFunction;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.ParameterUtil;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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
import org.mockito.ArgumentCaptor;
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

  private static final Time TIME =  () -> Instant.parse("2016-12-02T22:00:00Z");

  private List<Resource> resourceLimits = Lists.newArrayList();

  @Mock TriggerListener triggerListener;
  @Mock Storage storage;
  @Mock StorageTransaction transaction;
  @Mock StyxConfig config;
  @Mock StateManager stateManager;

  private BackfillTriggerManager backfillTriggerManager;

  private ExecutorService executor = Executors.newCachedThreadPool();

  private Map<String, Backfill> backfills;

  private Map<WorkflowInstance, RunState> activeStates;

  @Before
  public void setUp() throws Exception {
    backfills = new LinkedHashMap<>(); // we need to keep entry order
    activeStates = new HashMap<>();

    when(triggerListener.event(any(Workflow.class), any(Trigger.class), any(Instant.class)))
        .then(a -> {
          Workflow workflow = a.getArgumentAt(0, Workflow.class);
          Instant instant = a.getArgumentAt(2, Instant.class);

          final String parameter = toParameter(workflow.configuration().schedule(), instant);
          final WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(),
                                                                            parameter);
          RunState runState = RunState.fresh(workflowInstance);
          activeStates.put(workflowInstance, runState);

          return CompletableFuture.completedFuture(null);
        });

    when(stateManager.getActiveStatesByTriggerId(anyString())).thenReturn(activeStates);

    when(storage.resources()).thenReturn(resourceLimits);
    when(config.globalConcurrency()).thenReturn(Optional.empty());
    when(storage.config()).thenReturn(config);

    ArgumentCaptor<Backfill> backfillArgumentCaptor = ArgumentCaptor.forClass(Backfill.class);
    when(transaction.store(backfillArgumentCaptor.capture())).then(answer -> {
      backfills.put(backfillArgumentCaptor.getValue().id(), backfillArgumentCaptor.getValue());
      return backfillArgumentCaptor.getValue();
    });
    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
    when(transaction.backfill(stringCaptor.capture()))
        .then(answer -> Optional.of(backfills.get(stringCaptor.getValue())));

    when(storage.backfills(anyBoolean())).then(a -> new ArrayList<>(backfills.values()));

    when(storage.runInTransaction(any())).then(
        a -> a.getArgumentAt(0, TransactionFunction.class).apply(transaction));

    backfillTriggerManager = new BackfillTriggerManager(stateManager, storage,
                                                        triggerListener, Stats.NOOP, TIME,
                                                        (x) -> {});
  }

  @After
  public void tearDown() {
    executor.shutdownNow();
  }

  @Test
  public void shouldTriggerBackfillsNew() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    final int concurrency = BACKFILL_1.concurrency();

    backfills.put(BACKFILL_1.id(), BACKFILL_1);

    backfillTriggerManager.tick();

    final List<Instant> instants =
        ParameterUtil.rangeOfInstants(BACKFILL_1.start(), BACKFILL_1.end(),
                                      workflow.configuration().schedule());

    instants.stream().limit(concurrency).forEach(instant ->
            verify(triggerListener).event(workflow, Trigger.backfill(BACKFILL_1.id()), instant));

    verify(transaction)
        .store(BACKFILL_1.builder().nextTrigger(instants.get(concurrency)).build());
  }

  @Test
  public void shouldTriggerBackfillsInProgress() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);
    backfills.put(BACKFILL_1.id(), BACKFILL_1.builder()
        .nextTrigger(Instant.parse("2016-12-03T00:00:00Z"))
        .build());

    final WorkflowInstance wfi1 = WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T23");
    activeStates.put(wfi1, RunState.fresh(wfi1));

    backfillTriggerManager.tick();

    verify(triggerListener, only()).event(
        workflow,
        Trigger.backfill(BACKFILL_1.id()), Instant.parse("2016-12-03T00:00:00Z"));
  }

  @Test
  public void shouldTriggerBackfillsToCompletion() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    backfills.put(BACKFILL_2.id(), BACKFILL_2);

    backfillTriggerManager.tick();

    List<Instant> instants =
        ParameterUtil.rangeOfInstants(BACKFILL_2.start(), BACKFILL_2.end(), BACKFILL_2.schedule());
    instants.forEach(instant ->
            verify(triggerListener).event(workflow, Trigger.backfill(BACKFILL_2.id()), instant));

    final Backfill completedBackfill =
        BACKFILL_2.builder().nextTrigger(BACKFILL_2.end()).allTriggered(true).build();
    verify(transaction).store(completedBackfill);
  }

  @Test
  public void shouldNotTriggerBackfillsIfNoCapacity() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    backfills.put(BACKFILL_1.id(), BACKFILL_1);

    final WorkflowInstance wfi1 = WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T22");
    final WorkflowInstance wfi2 = WorkflowInstance.create(WORKFLOW_ID1, "2016-12-02T23");
    activeStates.put(wfi1, RunState.fresh(wfi1));
    activeStates.put(wfi2, RunState.fresh(wfi2));

    backfillTriggerManager.tick();

    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void shouldNotTriggerIfAllTriggered() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    final Backfill completedBackfill =
        BACKFILL_2.builder().nextTrigger(BACKFILL_2.end()).allTriggered(true).build();

    backfills.put(completedBackfill.id(), completedBackfill);

    backfillTriggerManager.tick();

    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void shouldMarkedAsAllTriggeredIfEndOfBackfillEncountered() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    final Backfill completedBackfill =
        BACKFILL_2.builder().nextTrigger(BACKFILL_2.end()).allTriggered(false).build();

    backfills.put(completedBackfill.id(), completedBackfill);

    backfillTriggerManager.tick();

    verify(transaction).store(completedBackfill.builder().allTriggered(true).build());
    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void shouldNotTriggerIfFailedToGetBackfills() throws Exception {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    doThrow(new IOException()).when(storage).backfills(anyBoolean());

    backfillTriggerManager.tick();

    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void shouldNotTriggerBackfillsWithMissingWorkflows() throws Exception {
    backfills.put(BACKFILL_1.id(), BACKFILL_1);
    when(storage.workflow(BACKFILL_1.workflowId())).thenReturn(Optional.empty());

    backfillTriggerManager.tick();

    verifyZeroInteractions(triggerListener);
    verify(storage).storeBackfill(BACKFILL_1.builder().halted(true).build());
  }

  @Test
  public void shouldNotTriggerBackfillsAndStoreBackfillWithMissingWorkflows() throws Exception {
    backfills.put(BACKFILL_1.id(), BACKFILL_1);
    when(storage.workflow(BACKFILL_1.workflowId())).thenReturn(Optional.empty());
    doThrow(new IOException()).when(storage).storeBackfill(any());

    backfillTriggerManager.tick();

    verifyZeroInteractions(triggerListener);
    verify(storage).storeBackfill(BACKFILL_1.builder().halted(true).build());
  }

  @Test
  public void shouldNotTriggerBackfillsWhenFailedToReadWorkflow() throws Exception {
    backfills.put(BACKFILL_1.id(), BACKFILL_1);
    when(storage.workflow(BACKFILL_1.workflowId())).thenThrow(new IOException());

    backfillTriggerManager.tick();

    verifyZeroInteractions(triggerListener);
    verify(storage, never()).storeBackfill(BACKFILL_1.builder().halted(true).build());
  }

  @Test
  public void shouldNotTriggerBackfillsIfRunInTransactionFails() throws Exception {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    doThrow(new IOException()).when(storage).runInTransaction(any());

    backfills.put(BACKFILL_1.id(), BACKFILL_1);

    backfillTriggerManager.tick();

    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void shouldTriggerAndUpdateBackfillsSynchronously() throws Exception {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    backfills.put(BACKFILL_1.id(), BACKFILL_1);

    // Collect unfinished triggering futures as the trigger listener is called
    final BlockingQueue<CompletableFuture<Void>> triggerProcessingFutures =
        new LinkedBlockingQueue<>();

    doAnswer(a -> {
      CompletableFuture<Void> processed = new CompletableFuture<>();
      Workflow workflow1 = a.getArgumentAt(0, Workflow.class);
      Instant instant = a.getArgumentAt(2, Instant.class);

      final String parameter = toParameter(workflow1.configuration().schedule(), instant);
      final WorkflowInstance workflowInstance = WorkflowInstance.create(workflow1.id(),
                                                                        parameter);
      RunState runState = RunState.fresh(workflowInstance);
      activeStates.put(workflowInstance, runState);

      triggerProcessingFutures.add(processed);
      return processed;
    }).when(triggerListener)
        .event(any(Workflow.class), any(Trigger.class), any(Instant.class));

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
      verify(transaction, timeout(60_000))
          .store(BACKFILL_1.builder().nextTrigger(instants.get(i + 1)).build());
    }
  }

  @Test
  public void shouldContinueTriggeringNextBackfillWhenUnknownExecutionException()
      throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    final int concurrency = BACKFILL_1.concurrency();

    backfills.put(BACKFILL_1.id(), BACKFILL_1);
    backfills.put(BACKFILL_2.id(), BACKFILL_2);

    doReturn(CompletableFutures.exceptionallyCompletedFuture(new RuntimeException()))
        .when(triggerListener)
        .event(any(Workflow.class), any(Trigger.class), eq(Instant.parse("2016-12-02T22:00:00Z")));

    backfillTriggerManager.tick();

    final List<Instant> instants =
        ParameterUtil.rangeOfInstants(BACKFILL_2.start(), BACKFILL_2.end(),
                                      workflow.configuration().schedule());

    instants.stream().limit(concurrency).forEach(instant ->
            verify(triggerListener).event(workflow, Trigger.backfill(BACKFILL_2.id()), instant));

    final Backfill completedBackfill =
        BACKFILL_2.builder().nextTrigger(BACKFILL_2.end()).allTriggered(true).build();
    verify(transaction).store(completedBackfill);
  }

  @Test
  public void shouldContinueTriggeringNextBackfillWhenOtherException() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    final int concurrency = BACKFILL_1.concurrency();

    backfills.put(BACKFILL_1.id(), BACKFILL_1);
    backfills.put(BACKFILL_2.id(), BACKFILL_2);

    doThrow(new RuntimeException())
        .when(triggerListener)
        .event(any(Workflow.class), any(Trigger.class), eq(Instant.parse("2016-12-02T22:00:00Z")));

    backfillTriggerManager.tick();

    final List<Instant> instants =
        ParameterUtil.rangeOfInstants(BACKFILL_2.start(), BACKFILL_2.end(),
                                      workflow.configuration().schedule());

    instants.stream().limit(concurrency).forEach(instant ->
                                                     verify(triggerListener).event(workflow, Trigger.backfill(BACKFILL_2.id()), instant));

    final Backfill completedBackfill =
        BACKFILL_2.builder().nextTrigger(BACKFILL_2.end()).allTriggered(true).build();
    verify(transaction).store(completedBackfill);
  }

  @Test
  public void shouldIgnoreIfAlreadyInitialized() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    final int concurrency = BACKFILL_1.concurrency();

    backfills.put(BACKFILL_1.id(), BACKFILL_1);

    doReturn(CompletableFutures
                 .exceptionallyCompletedFuture(new AlreadyInitializedException("")))
        .when(triggerListener)
        .event(any(Workflow.class), any(Trigger.class), eq(Instant.parse("2016-12-02T22:00:00Z")));

    backfillTriggerManager.tick();

    final List<Instant> instants =
        ParameterUtil.rangeOfInstants(BACKFILL_1.start(), BACKFILL_1.end(),
                                      workflow.configuration().schedule());

    instants.stream().limit(concurrency).forEach(instant ->
            verify(triggerListener).event(workflow, Trigger.backfill(BACKFILL_1.id()), instant));

    verify(transaction)
        .store(BACKFILL_1.builder().nextTrigger(instants.get(concurrency)).build());
  }

  private void initWorkflow(Workflow workflow) throws IOException {
    when(storage.workflow(workflow.id())).thenReturn(Optional.of(workflow));
  }

  private static Workflow createWorkflow(WorkflowId id) {
    return Workflow.create(
        id.componentId(),
        WorkflowConfiguration.builder()
            .id(id.id())
            .schedule(Schedule.HOURS)
            .build());
  }
}
