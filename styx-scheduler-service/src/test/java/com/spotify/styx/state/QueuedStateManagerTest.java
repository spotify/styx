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

package com.spotify.styx.state;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Throwables;
import com.spotify.styx.RepeatRule;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.serialization.PersistentWorkflowInstanceState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import com.spotify.styx.storage.TransactionException;
import com.spotify.styx.storage.TransactionFunction;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueuedStateManagerTest {

  private static final Instant NOW = Instant.parse("2017-01-02T01:02:03Z");
  private static final WorkflowInstance INSTANCE = WorkflowInstance.create(
      TestData.WORKFLOW_ID, "2016-05-01");
  private static final PersistentWorkflowInstanceState INSTANCE_NEW_STATE =
      PersistentWorkflowInstanceState.builder()
          .counter(18)
          .state(State.NEW)
          .data(StateData.zero())
          .timestamp(NOW)
          .build();
  private static final Workflow WORKFLOW =
      Workflow.create("foo", TestData.FULL_WORKFLOW_CONFIGURATION);
  private static final Trigger TRIGGER1 = Trigger.unknown("trig1");
  private static final BiConsumer<SequenceEvent, RunState> eventConsumer = (e, s) -> {};

  private final ExecutorService outputHandlerExecutor = Executors.newFixedThreadPool(16);
  private final ExecutorService eventConsumerExecutor = Executors.newSingleThreadExecutor();


  private QueuedStateManager stateManager;

  @Captor ArgumentCaptor<RunState> runStateCaptor;

  @Rule public RepeatRule repeatRule = new RepeatRule();

  @Mock Storage storage;
  @Mock StorageTransaction transaction;
  @Mock OutputHandler outputHandler;
  @Mock Time time;

  @Before
  public void setUp() throws Exception {
    when(time.get()).thenReturn(NOW);
    when(storage.runInTransaction(any())).thenAnswer(
        a -> a.getArgumentAt(0, TransactionFunction.class).apply(transaction));
    doNothing().when(outputHandler).transitionInto(runStateCaptor.capture());
    stateManager = new QueuedStateManager(
        time, outputHandlerExecutor, storage, eventConsumer, eventConsumerExecutor, outputHandler);
  }

  @After
  public void tearDown() throws Exception {
    if (stateManager != null) {
      stateManager.close();
    }
  }

  @Test
  public void shouldInitializeAndTriggerWFInstance() throws Exception {
    final PersistentWorkflowInstanceState instanceStateFresh =
        INSTANCE_NEW_STATE.toBuilder().counter(-1).build();
    when(storage.getLatestStoredCounter(INSTANCE)).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    when(transaction.activeState(INSTANCE)).thenReturn(Optional.of(instanceStateFresh));

    stateManager.trigger(INSTANCE, TRIGGER1)
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).insertActiveState(INSTANCE, instanceStateFresh);
    verify(storage).writeEvent(SequenceEvent.create(
        Event.triggerExecution(INSTANCE, TRIGGER1), 0, NOW.toEpochMilli()));
  }

  @Test
  public void shouldReInitializeWFInstanceFromNextCounter() throws Exception {
    when(storage.getLatestStoredCounter(INSTANCE)).thenReturn(Optional.of(INSTANCE_NEW_STATE.counter()));
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    when(transaction.activeState(INSTANCE)).thenReturn(Optional.of(INSTANCE_NEW_STATE));

    stateManager.trigger(INSTANCE, TRIGGER1)
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).insertActiveState(INSTANCE, INSTANCE_NEW_STATE);
    verify(storage).writeEvent(SequenceEvent.create(
        Event.triggerExecution(INSTANCE, TRIGGER1), INSTANCE_NEW_STATE.counter() + 1, NOW.toEpochMilli()));
  }

  @Test
  public void shouldNotBeActiveAfterHalt() throws Exception {
    when(transaction.activeState(INSTANCE)).thenReturn(
        Optional.of(PersistentWorkflowInstanceState
            .builder()
            .counter(17)
            .timestamp(NOW.minusMillis(1))
            .state(State.PREPARE)
            .data(StateData.zero())
            .build()));

    Event event = Event.halt(INSTANCE);
    stateManager.receive(event)
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).deleteActiveState(INSTANCE);

    verify(storage).writeEvent(SequenceEvent.create(event, 18, NOW.toEpochMilli()));
  }

  @Test
  public void shouldFailTriggerWFIfAlreadyActive() throws Exception {
    reset(storage);
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    final Exception rootCause = new IllegalStateException("Already exists!");
    when(storage.runInTransaction(any())).thenAnswer(a -> {
      a.getArgumentAt(0, TransactionFunction.class)
          .apply(transaction);
      throw new TransactionException(false, rootCause);
    });

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(rootCause));
    }
  }

  @Test(expected = IsClosedException.class)
  public void shouldRejectTriggerIfClosed() throws Exception {
    stateManager.close();
    stateManager.trigger(INSTANCE, TRIGGER1);
  }

  @Test(expected = IsClosedException.class)
  public void shouldRejectEventIfClosed() throws Exception {
    stateManager.close();
    stateManager.receive(Event.timeTrigger(INSTANCE));
  }

  @Test
  public void shouldCloseGracefully() throws Exception {

    when(transaction.activeState(INSTANCE)).thenReturn(
        Optional.of(PersistentWorkflowInstanceState
            .builder()
            .counter(17)
            .timestamp(NOW.minusMillis(1))
            .state(State.QUEUED)
            .data(StateData.zero())
            .build()));

    CompletableFuture<Void> barrier = new CompletableFuture<>();

    reset(storage);

    when(storage.runInTransaction(any())).thenAnswer(a -> {
      barrier.get();
      return a.getArgumentAt(0, TransactionFunction.class).apply(transaction);
    });

    CompletableFuture<Void> f = stateManager.receive(Event.dequeue(INSTANCE))
        .toCompletableFuture();

    CompletableFuture.runAsync(() -> {
      try {
        stateManager.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    verify(storage, timeout(30_000)).runInTransaction(any());

    barrier.complete(null);

    f.get(1, MINUTES);

    verify(outputHandler).transitionInto(any());
    assertThat(runStateCaptor.getValue().state(), is(State.PREPARE));
  }

  @Test
  public void shouldWriteEvents() throws Exception {
    Event event = Event.started(INSTANCE);

    when(transaction.activeState(INSTANCE))
        .then(a -> Optional.of(PersistentWorkflowInstanceState.builder()
            .timestamp(NOW.minusMillis(1))
            .counter(17)
            .state(State.SUBMITTED)
            .data(StateData.zero())
            .build()));

    stateManager.receive(event)
        .toCompletableFuture().get(1, MINUTES);

    verify(storage).writeEvent(SequenceEvent.create(event, 18, NOW.toEpochMilli()));
  }

  @Test
  public void shouldRemoveStateIfTerminal() throws Exception {
    when(transaction.activeState(INSTANCE)).thenReturn(
        Optional.of(PersistentWorkflowInstanceState
            .builder()
            .counter(17)
            .timestamp(NOW.minusMillis(1))
            .state(State.TERMINATED)
            .data(StateData.zero())
            .build()));

    Event event = Event.success(INSTANCE);
    stateManager.receive(event)
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).deleteActiveState(INSTANCE);

    verify(storage).writeEvent(SequenceEvent.create(event, 18, NOW.toEpochMilli()));
  }


  @Test
  public void shouldWriteActiveStateOnEvent() throws Exception {
    when(transaction.activeState(INSTANCE)).thenReturn(Optional.of(PersistentWorkflowInstanceState.builder()
        .counter(17)
        .timestamp(NOW.minusMillis(1))
        .state(State.QUEUED)
        .data(StateData.zero())
        .build()));

    stateManager.receive(Event.dequeue(INSTANCE))
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).updateActiveState(INSTANCE, PersistentWorkflowInstanceState.builder()
        .counter(18)
        .timestamp(NOW)
        .state(State.PREPARE)
        .data(StateData.zero())
        .build());
  }

  @Test
  public void shouldNotStoreEventOnIllegalStateTransition() throws Exception {
    when(transaction.activeState(INSTANCE)).thenReturn(Optional.of(PersistentWorkflowInstanceState.builder()
        .counter(17)
        .timestamp(NOW.minusMillis(1))
        .state(State.QUEUED)
        .data(StateData.zero())
        .build()));

    CompletableFuture<Void> f = stateManager.receive(Event.terminate(INSTANCE, Optional.empty()))
        .toCompletableFuture();

    try {
      f.get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(IllegalStateException.class));
    }

    verify(transaction, never()).updateActiveState(any(), any());
  }

  @Test
  public void triggerShouldHandleThrowingOutputHandler() throws Exception {
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    when(transaction.activeState(INSTANCE)).thenReturn(Optional.of(INSTANCE_NEW_STATE));
    final RuntimeException rootCause = new RuntimeException("foo!");
    doThrow(rootCause).when(outputHandler).transitionInto(any());
    CompletableFuture<Void> f = stateManager.trigger(INSTANCE, TRIGGER1).toCompletableFuture();
    try {
      f.get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(rootCause));
    }
  }

  @Test
  public void receiveShouldHandleThrowingOutputHandler() throws Exception {
    when(transaction.activeState(INSTANCE)).thenReturn(Optional.of(PersistentWorkflowInstanceState.builder()
        .counter(17)
        .timestamp(NOW.minusMillis(1))
        .state(State.QUEUED)
        .data(StateData.zero())
        .build()));

    final RuntimeException rootCause = new RuntimeException("foo!");
    doThrow(rootCause).when(outputHandler).transitionInto(any());
    CompletableFuture<Void> f = stateManager.receive(Event.dequeue(INSTANCE)).toCompletableFuture();
    try {
      f.get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(rootCause));
    }
  }
}
