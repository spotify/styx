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
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.datastore.DatastoreException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.spotify.styx.RepeatRule;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import com.spotify.styx.storage.TransactionException;
import com.spotify.styx.storage.TransactionFunction;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.CounterCapacityException;
import com.spotify.styx.util.CounterSnapshot;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.ShardedCounter;
import com.spotify.styx.util.Time;
import eu.javaspecialists.tjsn.concurrency.stripedexecutor.StripedExecutorService;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class QueuedStateManagerTest {

  private static final Instant NOW = Instant.parse("2017-01-02T01:02:03Z");
  private static final WorkflowInstance INSTANCE = WorkflowInstance.create(
      TestData.WORKFLOW_ID, "2016-05-01");
  private static final RunState INSTANCE_NEW_STATE =
      RunState.create(INSTANCE, State.NEW, StateData.zero(), NOW, 18L);
  private static final Workflow WORKFLOW =
      Workflow.create("foo", TestData.FULL_WORKFLOW_CONFIGURATION);
  private static final Trigger TRIGGER1 = Trigger.unknown("trig1");
  private static final StateData STATE_DATA_1 =
      StateData.newBuilder().resourceIds(ImmutableSet.of("resource1")).build();
  private static final BiConsumer<SequenceEvent, RunState> eventConsumer = (e, s) -> {};

  private final StripedExecutorService eventTransitionExecutor = new StripedExecutorService(16);
  private final ExecutorService eventConsumerExecutor = Executors.newSingleThreadExecutor();


  private QueuedStateManager stateManager;

  @Captor private ArgumentCaptor<RunState> runStateCaptor;

  @Rule public RepeatRule repeatRule = new RepeatRule();

  @Mock private Storage storage;
  @Mock private StorageTransaction transaction;
  @Mock private OutputHandler outputHandler;
  @Mock private Time time;
  @Mock private ShardedCounter shardedCounter;

  @Mock private Logger logger;

  @Before
  public void setUp() throws Exception {
    when(time.get()).thenReturn(NOW);
    when(storage.runInTransaction(any())).thenAnswer(
        a -> a.getArgumentAt(0, TransactionFunction.class).apply(transaction));
    doNothing().when(outputHandler).transitionInto(runStateCaptor.capture());
    stateManager = new QueuedStateManager(
        time, eventTransitionExecutor, storage, eventConsumer,
        eventConsumerExecutor, OutputHandler.fanOutput(outputHandler), shardedCounter, logger);
  }

  @After
  public void tearDown() throws Exception {
    if (stateManager != null) {
      stateManager.close();
    }
  }

  @Test
  public void shouldInitializeAndTriggerWFInstance() throws Exception {
    final RunState instanceStateFresh =
        RunState.create(INSTANCE, State.NEW, StateData.zero(), NOW, -1);
    when(storage.getLatestStoredCounter(INSTANCE)).thenReturn(Optional.empty());
    when(storage.readActiveState(INSTANCE))
        .thenReturn(Optional.of(RunState.create(INSTANCE, State.NEW, NOW, -1)));
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    when(transaction.readActiveState(INSTANCE)).thenReturn(Optional.of(instanceStateFresh));

    stateManager.trigger(INSTANCE, TRIGGER1)
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).writeActiveState(INSTANCE, instanceStateFresh);
    verify(storage).writeEvent(SequenceEvent.create(
        Event.triggerExecution(INSTANCE, TRIGGER1), 0, NOW.toEpochMilli()));
  }

  @Test
  public void shouldReInitializeWFInstanceFromNextCounter() throws Exception {
    when(storage.getLatestStoredCounter(INSTANCE)).thenReturn(Optional.of(INSTANCE_NEW_STATE.counter()));
    when(storage.readActiveState(INSTANCE)).thenReturn(Optional.of(INSTANCE_NEW_STATE));
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    when(transaction.readActiveState(INSTANCE)).thenReturn(Optional.of(INSTANCE_NEW_STATE));

    stateManager.trigger(INSTANCE, TRIGGER1)
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).writeActiveState(INSTANCE, INSTANCE_NEW_STATE);
    verify(storage).writeEvent(SequenceEvent.create(
        Event.triggerExecution(INSTANCE, TRIGGER1), INSTANCE_NEW_STATE.counter() + 1, NOW.toEpochMilli()));
  }

  @Test
  public void shouldNotBeActiveAfterHalt() throws Exception {
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.PREPARE,
            StateData.newBuilder().resourceIds(ImmutableSet.of()).build(), NOW, 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);
    when(storage.readActiveState(INSTANCE)).thenReturn(runState);

    Event event = Event.halt(INSTANCE);
    stateManager.receive(event)
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).deleteActiveState(INSTANCE);
    verify(storage).writeEvent(SequenceEvent.create(event, 18, NOW.toEpochMilli()));
  }

  @Test
  public void shouldNotFailWhenMissingResourceIdsWhenTransitionFromPrepareToError()
      throws Exception {
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.PREPARE, StateData.zero(), NOW, 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);
    when(storage.readActiveState(INSTANCE)).thenReturn(runState);

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
    DatastoreException datastoreException = new DatastoreException(1, "", "");
    TransactionException transactionException = spy(new TransactionException(datastoreException));

    when(transactionException.isAlreadyExists()).thenReturn(true);
    doThrow(transactionException).when(transaction).writeActiveState(any(), any());
    when(storage.runInTransaction(any())).thenAnswer(a ->
        a.getArgumentAt(0, TransactionFunction.class).apply(transaction));

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), is(instanceOf(AlreadyInitializedException.class)));
    }
  }

  @Test
  public void shouldFailTriggerWFIfOnConflict() throws Exception {
    reset(storage);
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    final DatastoreException datastoreException = new DatastoreException(1, "", "");
    final TransactionException transactionException = spy(new TransactionException(datastoreException));
    when(transactionException.isConflict()).thenReturn(true);
    when(storage.runInTransaction(any())).thenAnswer(a -> {
      a.getArgumentAt(0, TransactionFunction.class)
          .apply(transaction);
      throw transactionException;
    });

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause().getCause(), is(instanceOf(TransactionException.class)));
      TransactionException cause = (TransactionException) e.getCause().getCause();
      assertTrue(cause.isConflict());
    }
  }

  @Test
  public void shouldFailTriggerIfGetLatestCounterFails() throws Exception {
    when(storage.getLatestStoredCounter(any())).thenThrow(new IOException());

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(instanceOf(IOException.class)));
    }
  }

  @Test
  public void shouldFailTriggerIfWorkflowNotFound() throws Exception {
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.empty());

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(instanceOf(IllegalArgumentException.class)));
    }
  }

  @Test
  public void shouldFailTriggerIfIOExceptionFromTransaction() throws Exception {
    reset(storage);
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.empty());
    when(storage.runInTransaction(any())).thenThrow(new IOException());

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(instanceOf(IOException.class)));
    }
  }

  @Test
  public void shouldFailTriggerIfIsClosedOnReceive() throws Exception {
    reset(storage);
    stateManager = spy(new QueuedStateManager(
        time, eventTransitionExecutor, storage, eventConsumer,
        eventConsumerExecutor, outputHandler, shardedCounter));
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.empty());
    doThrow(new IsClosedException()).when(stateManager).receive(any());

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(instanceOf(IsClosedException.class)));
    }
  }

  @Test
  public void shouldFailTriggerIfIsClosedOnReceiveAndFailDeleteActiveState() throws Exception {
    reset(storage);
    stateManager = spy(new QueuedStateManager(
        time, eventTransitionExecutor, storage, eventConsumer,
        eventConsumerExecutor, outputHandler, shardedCounter));
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    doThrow(new IOException()).when(storage).deleteActiveState(any());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.empty());
    doThrow(new IsClosedException()).when(stateManager).receive(any());

    try {
      stateManager.trigger(INSTANCE, TRIGGER1)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(instanceOf(IsClosedException.class)));
    }
  }

  @Test(expected = IsClosedException.class)
  public void shouldRejectTriggertIfClosed() throws Exception {
    stateManager.close();
    stateManager.trigger(INSTANCE, TRIGGER1);
  }

  @Test(expected = IsClosedException.class)
  public void shouldRejectEventIfClosed() throws Exception {
    when(storage.readActiveState(INSTANCE))
        .thenReturn(Optional.of(RunState.create(INSTANCE, State.NEW, NOW, -1)));
    stateManager.close();
    stateManager.receive(Event.timeTrigger(INSTANCE));
  }

  @Test
  public void shouldCloseGracefully() throws Exception {
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.QUEUED, StateData.zero(), NOW.minusMillis(1), 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);

    CompletableFuture<Void> barrier = new CompletableFuture<>();

    reset(storage);
    when(storage.readActiveState(INSTANCE)).thenReturn(runState);
    when(storage.runInTransaction(any())).thenAnswer(a -> {
      barrier.get();
      return a.getArgumentAt(0, TransactionFunction.class).apply(transaction);
    });

    CompletableFuture<Void> f = stateManager.receive(Event.dequeue(INSTANCE, ImmutableSet.of()))
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
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.SUBMITTED, StateData.zero(), NOW, 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);
    when(storage.readActiveState(INSTANCE)).thenReturn(runState);

    stateManager.receive(event)
        .toCompletableFuture().get(1, MINUTES);

    verify(storage).writeEvent(SequenceEvent.create(event, 18, NOW.toEpochMilli()));
  }

  @Test
  public void shouldFailReceiveEventWithHigherCounter() throws Exception {
    Event event = Event.started(INSTANCE);

    when(transaction.readActiveState(INSTANCE)).thenReturn(
        Optional.of(RunState.create(INSTANCE, State.SUBMITTED, StateData.zero(), NOW, 17)));
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.of(17L));

    try {
      stateManager.receive(event, 16)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), is(instanceOf(StaleEventException.class)));
    }

    verify(storage, never()).writeEvent(any());
  }

  @Test
  public void shouldFailReceiveEventWithLowerCounter() throws Exception {
    Event event = Event.started(INSTANCE);
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.SUBMITTED, StateData.zero(), NOW.minusMillis(1), 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.of(17L));

    try {
      stateManager.receive(event, 18)
          .toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), is(instanceOf(RuntimeException.class)));
      assertThat(e.getCause().getMessage(),
          startsWith("Unexpected current counter is less than last observed one for"));
    }

    verify(storage, never()).writeEvent(any());
  }

  @Test
  public void shouldRemoveStateIfTerminal() throws Exception {
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.TERMINATED, StateData.zero(),
            NOW, 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);
    when(storage.readActiveState(INSTANCE)).thenReturn(runState);

    Event event = Event.success(INSTANCE);
    stateManager.receive(event)
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).deleteActiveState(INSTANCE);
    verify(storage).writeEvent(SequenceEvent.create(event, 18, NOW.toEpochMilli()));
  }

  @Test
  public void shouldHaveZeroQueuedEvent() throws Exception {
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.TERMINATED, StateData.zero(), NOW, 17L));
    when(transaction.readActiveState(INSTANCE)).thenReturn(
        runState);
    when(storage.readActiveState(INSTANCE)).thenReturn(runState);

    assertThat(stateManager.queuedEvents(), is(0L));

    Event event = Event.success(INSTANCE);
    stateManager.receive(event)
        .toCompletableFuture().get(1, MINUTES);

    assertThat(stateManager.queuedEvents(), is(0L));
    verify(transaction).deleteActiveState(INSTANCE);
    verify(storage).writeEvent(SequenceEvent.create(event, 18, NOW.toEpochMilli()));
  }

  @Test
  public void shouldWriteActiveStateOnEvent() throws Exception {
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.QUEUED, StateData.zero(), NOW, 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);
    when(storage.readActiveState(INSTANCE)).thenReturn(runState);

    stateManager.receive(Event.dequeue(INSTANCE, ImmutableSet.of()))
        .toCompletableFuture().get(1, MINUTES);

    verify(transaction).updateActiveState(INSTANCE, RunState.create(INSTANCE, State.PREPARE,
        StateData.newBuilder().resourceIds(ImmutableSet.of()).build(), NOW, 18));
  }

  @Test
  public void shouldPreventIllegalStateTransition() throws Exception {
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.QUEUED, StateData.zero(), NOW.minusMillis(1), 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);
    when(storage.readActiveState(INSTANCE)).thenReturn(runState);

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
  public void shouldFailReceiveForUnknownActiveWFInstance() throws Exception {
    when(transaction.readActiveState(INSTANCE)).thenReturn(Optional.empty());
    when(storage.readActiveState(INSTANCE)).thenReturn(Optional.empty());

    try {
      stateManager.receive(Event.terminate(INSTANCE, Optional.empty())).toCompletableFuture().get();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
    }
    verify(transaction, never()).updateActiveState(any(), any());
  }

  @Test
  public void shouldGetRunState() throws Exception {
    RunState runState = RunState.create(
        INSTANCE, State.QUEUED, StateData.zero(), NOW.minusMillis(1), 17);
    when(storage.readActiveState(INSTANCE)).thenReturn(Optional.of(runState));

    RunState returnedRunState = stateManager.getActiveState(INSTANCE).get();

    assertThat(runState, equalTo(returnedRunState));
  }

  @Test
  public void shouldGetRunStates() throws Exception {
    RunState runState = RunState.create(
        INSTANCE, State.QUEUED, StateData.zero(), NOW.minusMillis(1), 17);
    Map<WorkflowInstance, RunState> states = Maps.newConcurrentMap();
    states.put(INSTANCE, runState);
    when(storage.readActiveStates()).thenReturn(states);

    Map<WorkflowInstance, RunState> returnedRunStates = stateManager.getActiveStates();

    assertThat(returnedRunStates.get(INSTANCE), is(runState));
    assertThat(returnedRunStates.size(), is(1));
  }

  @Test
  public void shouldGetRunStatesByTriggerId() throws Exception {
    Map<WorkflowInstance, RunState> states = Maps.newConcurrentMap();
    StateData stateData = StateData.newBuilder()
        .trigger(Trigger.adhoc("foobar"))
        .build();
    RunState runState = RunState.create(
        INSTANCE, State.QUEUED, stateData, NOW.minusMillis(1), 17);
    states.put(INSTANCE, runState);
    when(storage.readActiveStatesByTriggerId("foobar")).thenReturn(states);
    Map<WorkflowInstance, RunState> returnedRunStates = stateManager.getActiveStatesByTriggerId("foobar");

    assertThat(returnedRunStates.get(INSTANCE), is(runState));
    assertThat(returnedRunStates.size(), is(1));
  }

  @Test
  public void triggerShouldHandleThrowingOutputHandler() throws Exception {
    Optional<RunState> runState = Optional.of(RunState.create(INSTANCE, State.NEW, NOW, -1));
    when(storage.readActiveState(INSTANCE)).thenReturn(runState);
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.of(-1L));
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);
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
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.QUEUED, StateData.zero(), NOW.minusMillis(1), 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);
    when(storage.readActiveState(INSTANCE)).thenReturn(runState);
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.of(17L));

    final RuntimeException rootCause = new RuntimeException("foo!");
    doThrow(rootCause).when(outputHandler).transitionInto(any());
    CompletableFuture<Void> f = stateManager.receive(Event.dequeue(INSTANCE, ImmutableSet.of())).toCompletableFuture();
    try {
      f.get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(rootCause));
    }
  }

  @Test
  public void receiveShouldLogTransactionConflict() throws Exception {
    final TransactionException cause = new TransactionException(
        new DatastoreException(10, "foo", "bar"));
    reset(storage);
    when(storage.runInTransaction(any())).thenThrow(cause);
    final Event event = Event.started(INSTANCE);
    try {
      stateManager.receive(event, 4711L).get();
      fail();
    } catch (ExecutionException ignore) {
    }
    verify(logger).debug(
        "Transaction conflict during workflow instance transition. Aborted: {}, counter={}",
        event, 4711L);
  }

  @Test
  public void receiveShouldLogTransactionFailure() throws Exception {
    final TransactionException cause = new TransactionException(
        new DatastoreException(new IOException("netsplit!")));
    reset(storage);
    when(storage.runInTransaction(any())).thenThrow(cause);
    final Event event = Event.started(INSTANCE);
    try {
      stateManager.receive(event, 4711L).get();
      fail();
    } catch (ExecutionException ignore) {
    }
    verify(logger).debug(
        "Transaction failure during workflow instance transition: {}, counter={}",
        event, 4711L, cause);
  }

  @Test
  public void receiveShouldLogFailure() throws Exception {
    final Exception cause = new Exception("fubared");
    reset(storage);
    when(storage.runInTransaction(any())).thenThrow(cause);
    final Event event = Event.started(INSTANCE);
    try {
      stateManager.receive(event, 4711L).get();
      fail();
    } catch (ExecutionException ignore) {
    }
    verify(logger).debug(
        "Failure during workflow instance transition: {}, counter={}",
        event, 4711L, cause);
  }

  @Test
  public void triggerShouldLogTransactionConflict() throws Exception {
    final TransactionException cause = new TransactionException(
        new DatastoreException(10, "foo", "bar"));
    reset(storage);
    when(storage.getLatestStoredCounter(INSTANCE)).thenReturn(Optional.empty());
    when(storage.runInTransaction(any())).thenThrow(cause);
    try {
      stateManager.trigger(INSTANCE, Trigger.natural()).toCompletableFuture().get();
      fail();
    } catch (ExecutionException ignore) {
    }
    verify(logger).debug("Transaction conflict when triggering workflow instance. Aborted: {}",
        INSTANCE);
  }

  @Test
  public void triggerShouldLogTransactionFailure() throws Exception {
    final TransactionException cause = new TransactionException(
        new DatastoreException(new IOException("netsplit!")));
    reset(storage);
    when(storage.getLatestStoredCounter(INSTANCE)).thenReturn(Optional.empty());
    when(storage.runInTransaction(any())).thenThrow(cause);
    try {
      stateManager.trigger(INSTANCE, Trigger.natural()).toCompletableFuture().get();
      fail();
    } catch (ExecutionException ignore) {
    }
    verify(logger).debug("Transaction failure when triggering workflow instance: {}: {}",
        INSTANCE, cause.getMessage(), cause);
  }

  @Test
  public void triggerShouldLogFailure() throws Exception {
    final Exception cause = new Exception("fubared");
    reset(storage);
    when(storage.getLatestStoredCounter(INSTANCE)).thenReturn(Optional.empty());
    when(storage.runInTransaction(any())).thenThrow(cause);
    try {
      stateManager.trigger(INSTANCE, Trigger.natural()).toCompletableFuture().get();
      fail();
    } catch (ExecutionException ignore) {
    }
    verify(logger).debug("Failure when triggering workflow instance: {}: {}",
        INSTANCE, cause.getMessage(), cause);
  }

  @Test
  public void shouldThrowRuntimeException() throws Exception {
    final IOException exception = new IOException();
    Optional<RunState> runState = Optional.of(RunState.create(INSTANCE, State.NEW, NOW, -1));
    when(storage.readActiveState(INSTANCE)).thenReturn(runState);
    doThrow(exception).when(storage).runInTransaction(any());
    CompletableFuture<Void> f = stateManager.receive(Event.dequeue(INSTANCE, ImmutableSet.of())).toCompletableFuture();
    try {
      f.get(1, MINUTES);
      fail();
    } catch (ExecutionException e) {
      assertThat(Throwables.getRootCause(e), is(exception));
    }
  }

  @Test
  public void shouldUpdateResourceCountersOnDequeue() throws Exception {
    givenState(INSTANCE, State.QUEUED);
    receiveEvent(Event.dequeue(INSTANCE, ImmutableSet.of("resource1")));
    verify(transaction).updateCounter(shardedCounter, "resource1", 1);
  }

  @Test
  public void shouldFailToUpdateResourceCountersOnDequeueDueToCapacity() throws Exception {
    givenState(INSTANCE, State.QUEUED);
    doThrow(new CounterCapacityException("foo"))
        .when(transaction).updateCounter(shardedCounter, "resource1", 1);
    final CounterSnapshot counterSnapshot = mock(CounterSnapshot.class);
    when(counterSnapshot.getLimit()).thenReturn(1L);
    when(shardedCounter.getCounterSnapshot("resource1")).thenReturn(counterSnapshot);

    final Set<Resource> resources = ImmutableSet.of(Resource.create("resource1", 1));
    final Event dequeueEvent = Event.dequeue(INSTANCE,
        resources.stream().map(Resource::id).collect(toSet()));
    final Event infoEvent = Event.info(INSTANCE,
        Message.info(String.format("Resource limit reached for: %s", resources)));
    final QueuedStateManager spied = spy(stateManager);
    doNothing().when(spied).receiveIgnoreClosed(eq(infoEvent), anyLong());

    try {
      spied.receive(dequeueEvent).toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (Exception e) {
      // expected exception
    }

    verify(spied).receiveIgnoreClosed(eq(infoEvent), anyLong());
  }

  @Test
  public void shouldFailToUpdateResourceCountersOnDequeueDueToCapacityAndNoInfoEventSent() throws Exception {
    final Set<Resource> resources = ImmutableSet.of(Resource.create("resource1", 1));
    final Message message = Message.info(String.format("Resource limit reached for: %s", resources));
    final RunState runState = RunState.create(INSTANCE, State.QUEUED,
        STATE_DATA_1.builder().messages(message).build(),
        NOW.minusMillis(1), 17);
    when(transaction.readActiveState(INSTANCE)).thenReturn(Optional.of(runState));
    when(storage.readActiveState(INSTANCE)).thenReturn(Optional.of(runState));

    doThrow(new CounterCapacityException("foo"))
        .when(transaction).updateCounter(shardedCounter, "resource1", 1);

    final Event dequeueEvent = Event.dequeue(INSTANCE,
        resources.stream().map(Resource::id).collect(toSet()));
    final QueuedStateManager spied = spy(stateManager);

    try {
      spied.receive(dequeueEvent).toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (Exception e) {
      // expected exception
    }

    verify(spied, never()).receiveIgnoreClosed(eq(Event.info(INSTANCE, message)), anyLong());
  }

  @Test
  public void shouldFailToUpdateResourceCountersOnDequeueDueToConflict() throws Exception {
    givenState(INSTANCE, State.QUEUED);
    doThrow(new RuntimeException())
        .when(transaction).updateCounter(shardedCounter, "resource1", 1);

    final Set<Resource> resources = ImmutableSet.of(Resource.create("resource1", 1));
    final Event dequeueEvent = Event.dequeue(INSTANCE,
        resources.stream().map(Resource::id).collect(toSet()));
    final Event infoEvent = Event.info(INSTANCE,
        Message.info(String.format("Resource limit reached for: %s", resources)));
    final QueuedStateManager spied = spy(stateManager);

    try {
      spied.receive(dequeueEvent).toCompletableFuture().get(1, MINUTES);
      fail();
    } catch (Exception e) {
      // expected exception
    }

    verify(spied, never()).receiveIgnoreClosed(eq(infoEvent), anyLong());
  }

  @Test
  public void shouldNotUpdateResourceCountersOnSubmit() throws Exception {
    givenState(INSTANCE, State.PREPARE);
    receiveEvent(Event.submit(INSTANCE, ExecutionDescription.forImage("docker-image"),"styx-run-1"));
    verify(transaction, never()).updateCounter(eq(shardedCounter), anyString(), anyInt());
  }

  @Test
  public void shouldNotUpdateResourceCountersOnSubmitted() throws Exception {
    givenState(INSTANCE, State.SUBMITTING);
    receiveEvent(Event.submitted(INSTANCE,"styx-run-1"));
    verify(transaction, never()).updateCounter(eq(shardedCounter), anyString(), anyInt());
  }

  @Test
  public void shouldNotUpdateResourceCountersOnStarted() throws Exception {
    givenState(INSTANCE, State.SUBMITTED);
    receiveEvent(Event.started(INSTANCE));
    verify(transaction, never()).updateCounter(eq(shardedCounter), anyString(), anyInt());
  }

  @Test
  public void shouldUpdateResourceCountersOnTerminate() throws Exception {
    givenState(INSTANCE, State.RUNNING);
    receiveEvent(Event.terminate(INSTANCE, Optional.of(1)));
    verify(transaction).updateCounter(shardedCounter, "resource1", -1);
  }

  @Test
  public void shouldNotUpdateResourceCountersOnStopAfterTerminated() throws Exception {
    givenState(INSTANCE, State.TERMINATED);
    receiveEvent(Event.stop(INSTANCE));
    verify(transaction, never()).updateCounter(eq(shardedCounter), anyString(), anyInt());
  }

  @Test
  public void shouldNotUpdateResourceCountersOnSuccess() throws Exception {
    givenState(INSTANCE, State.TERMINATED);
    receiveEvent(Event.success(INSTANCE));
    verify(transaction, never()).updateCounter(eq(shardedCounter), anyString(), anyInt());
  }

  @Test
  public void shouldUpdateResourceCountersOnHaltWhileRunning() throws Exception {
    givenState(INSTANCE, State.RUNNING);
    receiveEvent(Event.halt(INSTANCE));
    verify(transaction).updateCounter(shardedCounter, "resource1", -1);
  }

  @Test
  public void shouldUpdateResourceCountersOnTimeoutWhileRunning() throws Exception {
    givenState(INSTANCE, State.RUNNING);
    receiveEvent(Event.timeout(INSTANCE));
    verify(transaction).updateCounter(shardedCounter, "resource1", -1);
  }

  @Test
  public void shouldNotUpdateResourceCountersOnTimeoutWhileQueued() throws Exception {
    givenState(INSTANCE, State.QUEUED);
    receiveEvent(Event.timeout(INSTANCE));
    verify(transaction, never()).updateCounter(eq(shardedCounter), anyString(), anyInt());
  }

  @Test
  public void shouldNotUpdateResourceCountersOnStopAfterFailed() throws Exception {
    givenState(INSTANCE, State.FAILED);
    receiveEvent(Event.stop(INSTANCE));
    verify(transaction, never()).updateCounter(eq(shardedCounter), anyString(), anyInt());
  }

  @Test
  public void shouldNotUpdateResourceCountersOnRunErrorWhileQueued() throws Exception {
    givenState(INSTANCE, State.QUEUED);
    receiveEvent(Event.runError(INSTANCE, "random error"));
    verify(transaction, never()).updateCounter(eq(shardedCounter), anyString(), anyInt());
  }

  @Test
  public void shouldUpdateResourceCountersOnRunErrorWhileRunning() throws Exception {
    givenState(INSTANCE, State.RUNNING);
    receiveEvent(Event.runError(INSTANCE, "random error"));
    verify(transaction).updateCounter(shardedCounter, "resource1", -1);
  }

  @Test
  public void shouldNotUpdateResourceCountersOnRetryAfterFailed() throws Exception {
    givenState(INSTANCE, State.FAILED);
    receiveEvent(Event.retryAfter(INSTANCE,10));
    verify(transaction, never()).updateCounter(eq(shardedCounter), anyString(), anyInt());
  }

  @Test
  public void shouldNotUpdateResourceCountersOnRetryAfterTerminated() throws Exception {
    givenState(INSTANCE, State.TERMINATED);
    receiveEvent(Event.retryAfter(INSTANCE, 10));
    verify(transaction, never()).updateCounter(eq(shardedCounter), anyString(), anyInt());
  }

  @Test
  public void shouldReceiveEventIgnoreClosed() throws IOException, IsClosedException {
    final QueuedStateManager spied = spy(stateManager);
    spied.close();

    spied.receiveIgnoreClosed(Event.started(INSTANCE));
    verify(spied).ensureRunning();
    verifyNoMoreInteractions(storage);
  }

  @Test
  public void shouldReceiveEventIgnoreClosedWithCounter() throws IOException, IsClosedException {
    final QueuedStateManager spied = spy(stateManager);
    spied.close();

    spied.receiveIgnoreClosed(Event.started(INSTANCE), 17);
    verify(spied).ensureRunning();
    verifyNoMoreInteractions(storage);
  }

  public void givenState(WorkflowInstance instance, State state) throws IOException {
    final RunState runState = RunState.create(instance, state, STATE_DATA_1, NOW.minusMillis(1), 17);
    when(transaction.readActiveState(instance)).thenReturn(Optional.of(runState));
    when(storage.readActiveState(INSTANCE)).thenReturn(Optional.of(runState));
  }

  public void receiveEvent(Event event) throws Exception {
    stateManager.receive(event).toCompletableFuture().get(1, MINUTES);
  }
}
