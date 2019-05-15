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
import static com.spotify.styx.util.TimeUtil.instantsInRange;
import static com.spotify.styx.util.TimeUtil.previousInstant;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.only;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.TriggerParameters;
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
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BackfillTriggerManagerTest {

  private static final TriggerParameters TRIGGER_PARAMETERS = TriggerParameters.builder()
      .env("FOO", "foo",
          "BAR", "bar")
      .build();

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
      .triggerParameters(TRIGGER_PARAMETERS)
      .build();

  private static final Backfill BACKFILL_2 = Backfill.newBuilder()
      .id("backfill-2")
      .start(Instant.parse("2016-12-02T22:00:00Z"))
      .end(Instant.parse("2016-12-05T22:00:00Z"))
      .reverse(true)
      .workflowId(WORKFLOW_ID1)
      .concurrency(2)
      .schedule(Schedule.HOURS)
      .nextTrigger(Instant.parse("2016-12-05T21:00:00Z"))
      .triggerParameters(TRIGGER_PARAMETERS)
      .build();

  private static final Backfill BACKFILL_3 = Backfill.newBuilder()
      .id("backfill-3")
      .start(Instant.parse("2016-12-02T00:00:00Z"))
      .end(Instant.parse("2016-12-02T03:00:00Z"))
      .workflowId(WORKFLOW_ID1)
      .concurrency(3)
      .schedule(Schedule.HOURS)
      .nextTrigger(Instant.parse("2016-12-02T00:00:00Z"))
      .triggerParameters(TRIGGER_PARAMETERS)
      .build();

  private static final Backfill BACKFILL_4 = Backfill.newBuilder()
      .id("backfill-4")
      .start(Instant.parse("2016-12-02T00:00:00Z"))
      .end(Instant.parse("2016-12-02T03:00:00Z"))
      .workflowId(WORKFLOW_ID1)
      .concurrency(3)
      .schedule(Schedule.HOURS)
      .reverse(true)
      .nextTrigger(Instant.parse("2016-12-02T02:00:00Z"))
      .triggerParameters(TRIGGER_PARAMETERS)
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

    doAnswer(a -> {
      Workflow workflow = a.getArgument(0);
      Instant instant = a.getArgument(2);

      final String parameter = toParameter(workflow.configuration().schedule(), instant);
      final WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(),
          parameter);
      RunState runState = RunState.fresh(workflowInstance);
      activeStates.put(workflowInstance, runState);
      return null;
    }).when(triggerListener).event(any(), any(), any(), any());

    when(stateManager.getActiveStatesByTriggerId(anyString())).thenReturn(activeStates);

    ArgumentCaptor<Backfill> backfillArgumentCaptor = ArgumentCaptor.forClass(Backfill.class);
    when(transaction.store(backfillArgumentCaptor.capture())).then(answer -> {
      backfills.put(backfillArgumentCaptor.getValue().id(), backfillArgumentCaptor.getValue());
      return backfillArgumentCaptor.getValue();
    });
    ArgumentCaptor<String> stringCaptor = ArgumentCaptor.forClass(String.class);
    when(transaction.backfill(stringCaptor.capture()))
        .then(answer -> Optional.of(backfills.get(stringCaptor.getValue())));

    when(storage.backfills(anyBoolean())).then(a -> new ArrayList<>(backfills.values()));

    when(storage.runInTransactionWithRetries(any())).then(
        a -> a.<TransactionFunction>getArgument(0).apply(transaction));

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

    final List<Instant> instants = instantsInRange(BACKFILL_1.start(), BACKFILL_1.end(),
        workflow.configuration().schedule());

    instants.stream().limit(concurrency).forEach(instant ->
            verify(triggerListener).event(workflow, Trigger.backfill(BACKFILL_1.id()), instant, TRIGGER_PARAMETERS));

    verify(transaction)
        .store(BACKFILL_1.builder().nextTrigger(instants.get(concurrency)).build());
  }

  @Test
  public void shouldTriggerBackfillsNewReversed() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    final int concurrency = BACKFILL_2.concurrency();

    backfills.put(BACKFILL_2.id(), BACKFILL_2);

    backfillTriggerManager.tick();

    final List<Instant> instants = Lists.reverse(instantsInRange(BACKFILL_2.start(), BACKFILL_2.end(),
            workflow.configuration().schedule()));

    instants.stream().limit(concurrency).forEach(instant ->
        verify(triggerListener).event(workflow, Trigger.backfill(BACKFILL_2.id()), instant, TRIGGER_PARAMETERS));

    verify(transaction)
        .store(BACKFILL_2.builder().nextTrigger(instants.get(concurrency)).build());
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
        Trigger.backfill(BACKFILL_1.id()), Instant.parse("2016-12-03T00:00:00Z"), TRIGGER_PARAMETERS);
  }

  @Test
  public void shouldTriggerBackfillsInProgressReversed() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);
    backfills.put(BACKFILL_2.id(), BACKFILL_2.builder()
        .nextTrigger(Instant.parse("2016-12-03T00:00:00Z"))
        .build());

    final WorkflowInstance wfi1 = WorkflowInstance.create(WORKFLOW_ID1, "2016-12-03T01");
    activeStates.put(wfi1, RunState.fresh(wfi1));

    backfillTriggerManager.tick();

    verify(triggerListener, only()).event(
        workflow,
        Trigger.backfill(BACKFILL_2.id()), Instant.parse("2016-12-03T00:00:00Z"), TRIGGER_PARAMETERS);
  }

  @Test
  public void shouldTriggerBackfillsToCompletion() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    backfills.put(BACKFILL_3.id(), BACKFILL_3);

    backfillTriggerManager.tick();

    List<Instant> instants =
        instantsInRange(BACKFILL_3.start(), BACKFILL_3.end(), BACKFILL_3.schedule());
    instants.forEach(instant -> inOrder(triggerListener).verify(triggerListener)
        .event(workflow, Trigger.backfill(BACKFILL_3.id()), instant, TRIGGER_PARAMETERS));

    final Backfill completedBackfill =
        BACKFILL_3.builder().nextTrigger(BACKFILL_3.end()).allTriggered(true).build();
    verify(transaction).store(completedBackfill);
  }

  @Test
  public void shouldTriggerBackfillsToCompletionReversed() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    backfills.put(BACKFILL_4.id(), BACKFILL_4);

    backfillTriggerManager.tick();

    List<Instant> instants =
        Lists.reverse(instantsInRange(BACKFILL_4.start(), BACKFILL_4.end(), BACKFILL_4.schedule()));
    instants.forEach(instant -> inOrder(triggerListener).verify(triggerListener)
        .event(workflow, Trigger.backfill(BACKFILL_4.id()), instant, TRIGGER_PARAMETERS));

    final Backfill completedBackfill =
        BACKFILL_4.builder().nextTrigger(previousInstant(BACKFILL_4.start(), BACKFILL_4.schedule())).allTriggered(true).build();
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
  public void shouldNotTriggerBackfillsIfNoCapacityReversed() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    backfills.put(BACKFILL_2.id(), BACKFILL_2);

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
        BACKFILL_3.builder().nextTrigger(BACKFILL_3.end()).allTriggered(true).build();

    backfills.put(completedBackfill.id(), completedBackfill);

    backfillTriggerManager.tick();

    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void shouldNotTriggerIfAllTriggeredReversed() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    final Backfill completedBackfill = BACKFILL_4.builder()
        .nextTrigger(previousInstant(BACKFILL_4.start(), BACKFILL_4.schedule()))
        .allTriggered(true)
        .build();

    backfills.put(completedBackfill.id(), completedBackfill);

    backfillTriggerManager.tick();

    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void shouldMarkedAsAllTriggeredIfEndOfBackfillEncountered() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    final Backfill completedBackfill =
        BACKFILL_3.builder().nextTrigger(BACKFILL_3.end()).allTriggered(false).build();

    backfills.put(completedBackfill.id(), completedBackfill);

    backfillTriggerManager.tick();

    verify(transaction).store(completedBackfill.builder().allTriggered(true).build());
    verifyZeroInteractions(triggerListener);
  }

  @Test
  public void shouldMarkedAsAllTriggeredIfEndOfBackfillEncounteredReversed() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    final Backfill completedBackfill = BACKFILL_4.builder()
        .nextTrigger(previousInstant(BACKFILL_4.start(), BACKFILL_4.schedule()))
        .allTriggered(false)
        .build();

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

    doThrow(new IOException()).when(storage).runInTransactionWithRetries(any());

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
      Workflow workflow1 = a.getArgument(0);
      Instant instant = a.getArgument(2);

      final String parameter = toParameter(workflow1.configuration().schedule(), instant);
      final WorkflowInstance workflowInstance = WorkflowInstance.create(workflow1.id(),
                                                                        parameter);
      RunState runState = RunState.fresh(workflowInstance);
      activeStates.put(workflowInstance, runState);

      triggerProcessingFutures.add(processed);
      return processed.join();
    }).when(triggerListener)
        .event(any(), any(), any(), any());

    // Run a single tick of the backfillTriggerManager
    executor.execute(backfillTriggerManager::tick);

    final List<Instant> instants = instantsInRange(BACKFILL_1.start(), BACKFILL_1.end(),
        workflow.configuration().schedule());

    // Go through each expected trigger sequentially and verify that the next partition is not
    // triggered before the future for the previous partition trigger is completed
    for (int i = 0; i < BACKFILL_1.concurrency(); i++) {
      final Instant instant = instants.get(i);

      final CompletableFuture<Void> triggerProcessed = triggerProcessingFutures.poll(1,
                                                                                     TimeUnit.MINUTES);
      assertThat(triggerProcessingFutures.isEmpty(), is(true));

      verify(triggerListener, timeout(60_000))
          .event(any(), any(), eq(instant), any());

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
    backfills.put(BACKFILL_3.id(), BACKFILL_3);

    doThrow(new RuntimeException())
        .when(triggerListener)
        .event(any(), any(), eq(Instant.parse("2016-12-02T22:00:00Z")), eq(TRIGGER_PARAMETERS));

    backfillTriggerManager.tick();

    final List<Instant> instants = instantsInRange(BACKFILL_3.start(), BACKFILL_3.end(),
        workflow.configuration().schedule());

    instants.stream().limit(concurrency).forEach(instant ->
            verify(triggerListener).event(workflow, Trigger.backfill(BACKFILL_3.id()), instant, TRIGGER_PARAMETERS));

    final Backfill completedBackfill =
        BACKFILL_3.builder().nextTrigger(BACKFILL_3.end()).allTriggered(true).build();
    verify(transaction).store(completedBackfill);
  }

  @Test
  public void shouldContinueTriggeringNextBackfillWhenOtherException() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    final int concurrency = BACKFILL_1.concurrency();

    backfills.put(BACKFILL_1.id(), BACKFILL_1);
    backfills.put(BACKFILL_3.id(), BACKFILL_3);

    doThrow(new RuntimeException())
        .when(triggerListener)
        .event(any(), any(), eq(Instant.parse("2016-12-02T22:00:00Z")), eq(TRIGGER_PARAMETERS));

    backfillTriggerManager.tick();

    final List<Instant> instants = instantsInRange(BACKFILL_3.start(), BACKFILL_3.end(),
        workflow.configuration().schedule());

    instants.stream().limit(concurrency).forEach(instant ->
                                                     verify(triggerListener).event(workflow, Trigger.backfill(
                                                         BACKFILL_3.id()), instant, TRIGGER_PARAMETERS));

    final Backfill completedBackfill =
        BACKFILL_3.builder().nextTrigger(BACKFILL_3.end()).allTriggered(true).build();
    verify(transaction).store(completedBackfill);
  }

  @Test
  public void shouldIgnoreIfAlreadyInitialized() throws IOException {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    final int concurrency = BACKFILL_1.concurrency();

    backfills.put(BACKFILL_1.id(), BACKFILL_1);

    doThrow(new AlreadyInitializedException(""))
        .when(triggerListener)
        .event(any(), any(), eq(Instant.parse("2016-12-02T22:00:00Z")), eq(TRIGGER_PARAMETERS));

    backfillTriggerManager.tick();

    final List<Instant> instants = instantsInRange(BACKFILL_1.start(), BACKFILL_1.end(),
        workflow.configuration().schedule());

    instants.stream().limit(concurrency).forEach(instant ->
            verify(triggerListener).event(workflow, Trigger.backfill(BACKFILL_1.id()), instant, TRIGGER_PARAMETERS));

    verify(transaction)
        .store(BACKFILL_1.builder().nextTrigger(instants.get(concurrency)).build());
  }

  @Test
  public void shouldNotTriggerNextPartitionAndProgressIfBackfillHalted() throws Exception {
    final Workflow workflow = createWorkflow(WORKFLOW_ID1);
    initWorkflow(workflow);

    backfills.put(BACKFILL_1.id(), BACKFILL_1.builder().halted(true).build());

    final boolean moveOn = backfillTriggerManager.triggerNextPartitionAndProgress(transaction,
        BACKFILL_1.id(), workflow, BACKFILL_1.nextTrigger(), 10, false);
    assertFalse(moveOn);
    verifyNoMoreInteractions(triggerListener);
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
