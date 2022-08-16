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

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.datastore.DatastoreException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.storage.DatastoreIOException;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import com.spotify.styx.storage.TransactionException;
import com.spotify.styx.storage.TransactionFunction;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.CounterCapacityException;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.ShardedCounter;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;

@RunWith(MockitoJUnitRunner.class)
public class PersistentStateManagerTest {

  private static final Instant NOW = Instant.parse("2017-01-02T01:02:03Z");
  private static final Workflow WORKFLOW =
      Workflow.create("foo", TestData.FULL_WORKFLOW_CONFIGURATION);
  private static final Trigger TRIGGER1 = Trigger.unknown("trig1");
  private static final TriggerParameters PARAMETERS = TriggerParameters.builder()
      .env("FOO", "foo",
          "BAR", "bar")
      .build();
  private static final StateData STATE_DATA_1 = StateData.newBuilder()
      .resourceIds(ImmutableSet.of("resource1"))
      .build();
  private static final BiConsumer<SequenceEvent, RunState> eventConsumer = (e, s) -> {};
  private static final WorkflowInstance INSTANCE = WorkflowInstance.create(
      TestData.WORKFLOW_ID, "2016-05-01");
  private static final RunState INSTANCE_NEW_STATE =
      RunState.create(INSTANCE, State.NEW, StateData.zero(), NOW, 18L);
  private static final RunState INSTANCE_QUEUED_STATE =
      INSTANCE_NEW_STATE.transition(Event.triggerExecution(INSTANCE, TRIGGER1, PARAMETERS), () -> NOW);

  private final ExecutorService executor = Executors.newWorkStealingPool();
  private final ExecutorService eventConsumerExecutor = Executors.newSingleThreadExecutor();


  private PersistentStateManager stateManager;

  @Rule public ExpectedException exception = ExpectedException.none();

  @Captor private ArgumentCaptor<RunState> runStateCaptor;

  @Mock private Storage storage;
  @Mock private StorageTransaction transaction;
  @Mock private OutputHandler outputHandler;
  @Mock private Time time;
  @Mock private ShardedCounter shardedCounter;

  @Mock private Logger logger;

  @Before
  public void setUp() throws Exception {
    when(time.get()).thenReturn(NOW);
    when(storage.runInTransactionWithRetries(any())).thenAnswer(
        a -> a.<TransactionFunction>getArgument(0).apply(transaction));
    doNothing().when(outputHandler).transitionInto(runStateCaptor.capture(), any());
    stateManager = new PersistentStateManager(
        time, executor, storage, eventConsumer,
        eventConsumerExecutor, OutputHandler.fanOutput(List.of(outputHandler)), shardedCounter, logger);
  }

  @After
  public void tearDown() {
    executor.shutdownNow();
    eventConsumerExecutor.shutdownNow();
  }

  @Test
  public void tickShouldCallOutputHandlers() throws IOException {
    var instance1 = WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-05-01");
    var instance2 = WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-05-02");
    var runState1 = RunState.create(instance1, State.SUBMITTING, StateData.zero(), NOW.minusMillis(2), 17);
    var runState2 = RunState.create(instance2, State.TERMINATED, StateData.zero(), NOW.minusMillis(1), 4711);

    when(storage.listActiveInstances()).thenReturn(Set.of(instance1, instance2));
    when(storage.readActiveState(instance1)).thenReturn(Optional.of(runState1));
    when(storage.readActiveState(instance2)).thenReturn(Optional.of(runState2));

    stateManager.tick();

    verify(outputHandler).transitionInto(runState1, stateManager);
    verify(outputHandler).transitionInto(runState2, stateManager);
  }

  @Test
  public void tickShouldTolerateOutputHandlerFailure() throws IOException {
    var instance1 = WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-05-01");
    var instance2 = WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-05-02");
    var runState1 = RunState.create(instance1, State.SUBMITTING, StateData.zero(), NOW.minusMillis(2), 17);
    var runState2 = RunState.create(instance2, State.TERMINATED, StateData.zero(), NOW.minusMillis(1), 4711);

    when(storage.listActiveInstances()).thenReturn(Set.of(instance1, instance2));
    when(storage.readActiveState(instance1)).thenReturn(Optional.of(runState1));
    when(storage.readActiveState(instance2)).thenReturn(Optional.of(runState2));

    var cause = new RuntimeException("fail!");
    doThrow(cause).when(outputHandler).transitionInto(runState1, stateManager);

    stateManager.tick();

    verify(outputHandler).transitionInto(runState1, stateManager);
    verify(outputHandler).transitionInto(runState2, stateManager);

    verify(logger).error("Error ticking instance: {}", instance1, cause );
  }

  @Test
  public void tickShouldTolerateOutputHandlerStateTransitionConflict() throws IOException {
    var instance1 = WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-05-01");
    var instance2 = WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-05-02");
    var runState1 = RunState.create(instance1, State.SUBMITTING, StateData.zero(), NOW.minusMillis(2), 17);
    var runState2 = RunState.create(instance2, State.TERMINATED, StateData.zero(), NOW.minusMillis(1), 4711);

    when(storage.listActiveInstances()).thenReturn(Set.of(instance1, instance2));
    when(storage.readActiveState(instance1)).thenReturn(Optional.of(runState1));
    when(storage.readActiveState(instance2)).thenReturn(Optional.of(runState2));

    var cause = new StateTransitionConflictException("conflict!");
    doThrow(cause).when(outputHandler).transitionInto(runState1, stateManager);

    stateManager.tick();

    verify(outputHandler).transitionInto(runState1, stateManager);
    verify(outputHandler).transitionInto(runState2, stateManager);

    verify(logger).debug("State transition conflict when ticking instance: {}", instance1, cause);
  }

  @Test
  public void tickShouldTolerateOutputHandlerCounterCapacityException() throws IOException {
    var instance1 = WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-05-01");
    var instance2 = WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-05-02");
    var runState1 = RunState.create(instance1, State.SUBMITTING, StateData.zero(), NOW.minusMillis(2), 17);
    var runState2 = RunState.create(instance2, State.TERMINATED, StateData.zero(), NOW.minusMillis(1), 4711);

    when(storage.listActiveInstances()).thenReturn(Set.of(instance1, instance2));
    when(storage.readActiveState(instance1)).thenReturn(Optional.of(runState1));
    when(storage.readActiveState(instance2)).thenReturn(Optional.of(runState2));

    var cause = new CounterCapacityException("no capacity");
    doThrow(cause).when(outputHandler).transitionInto(runState1, stateManager);

    stateManager.tick();

    verify(outputHandler).transitionInto(runState1, stateManager);
    verify(outputHandler).transitionInto(runState2, stateManager);

    verify(logger).debug("Counter capacity exhausted when ticking instance: {}", instance1, cause);
  }

  @Test
  public void tickShouldTolerateOutputHandlerIllegalArgumentException() throws IOException {
    var instance1 = WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-05-01");
    var instance2 = WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-05-02");
    var runState1 = RunState.create(instance1, State.SUBMITTING, StateData.zero(), NOW.minusMillis(2), 17);
    var runState2 = RunState.create(instance2, State.TERMINATED, StateData.zero(), NOW.minusMillis(1), 4711);

    when(storage.listActiveInstances()).thenReturn(Set.of(instance1, instance2));
    when(storage.readActiveState(instance1)).thenReturn(Optional.of(runState1));
    when(storage.readActiveState(instance2)).thenReturn(Optional.of(runState2));

    var cause = new IllegalArgumentException("unknown workflow");
    doThrow(cause).when(outputHandler).transitionInto(runState1, stateManager);

    stateManager.tick();

    verify(outputHandler).transitionInto(runState1, stateManager);
    verify(outputHandler).transitionInto(runState2, stateManager);

    verify(logger).debug("Illegal argument when ticking instance: {}", instance1, cause);
  }

  @Test
  public void shouldInitializeAndTriggerWFInstance() throws Exception {
    when(storage.getLatestStoredCounter(INSTANCE)).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));

    var initialState = RunState.create(INSTANCE, State.NEW, StateData.zero(), NOW, -1);
    var expectedState = initialState.transition(Event.triggerExecution(INSTANCE, TRIGGER1, PARAMETERS), time);
    stateManager.trigger(INSTANCE, TRIGGER1, PARAMETERS);

    verify(transaction).writeActiveState(INSTANCE, expectedState);
    verify(storage).writeEvent(SequenceEvent.create(
        Event.triggerExecution(INSTANCE, TRIGGER1, PARAMETERS), 0, NOW.toEpochMilli()));
  }

  @Test
  public void shouldReInitializeWFInstanceFromNextCounter() throws Exception {
    when(storage.getLatestStoredCounter(INSTANCE)).thenReturn(Optional.of(INSTANCE_NEW_STATE.counter()));
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));

    stateManager.trigger(INSTANCE, TRIGGER1, PARAMETERS);

    verify(storage).getLatestStoredCounter(INSTANCE);
    verify(transaction).writeActiveState(INSTANCE, INSTANCE_QUEUED_STATE);
    verify(storage).writeEvent(SequenceEvent.create(
        Event.triggerExecution(INSTANCE, TRIGGER1, PARAMETERS),
        INSTANCE_QUEUED_STATE.counter(), NOW.toEpochMilli()));
  }

  @Test
  public void shouldNotBeActiveAfterHalt() throws Exception {
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.PREPARE,
            StateData.newBuilder().resourceIds(ImmutableSet.of()).build(), NOW, 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);

    Event event = Event.halt(INSTANCE);
    stateManager.receive(event);

    verify(transaction).deleteActiveState(INSTANCE);
    verify(storage).writeEvent(SequenceEvent.create(event, 18, NOW.toEpochMilli()));
  }

  @Test
  public void shouldNotFailWhenMissingResourceIdsWhenTransitionFromPrepareToError()
      throws Exception {
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.PREPARE, StateData.zero(), NOW, 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);

    Event event = Event.halt(INSTANCE);
    stateManager.receive(event);

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
    when(storage.runInTransactionWithRetries(any())).thenAnswer(a ->
        a.<TransactionFunction>getArgument(0).apply(transaction));

    try {
      stateManager.trigger(INSTANCE, TRIGGER1, PARAMETERS);
      fail();
    } catch (AlreadyInitializedException ignore) {
    }
  }

  @Test
  public void shouldFailTriggerIfGetLatestCounterFails() throws Exception {
    var cause = new IOException();
    when(storage.getLatestStoredCounter(any())).thenThrow(cause);
    exception.expectCause(is(cause));
    stateManager.trigger(INSTANCE, TRIGGER1, PARAMETERS);
  }

  @Test
  public void shouldFailTriggerIfWorkflowNotFound() throws Exception {
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.empty());
    exception.expect(instanceOf(IllegalArgumentException.class));
    exception.expectMessage("Workflow not found: " + INSTANCE.workflowId().toKey());
    stateManager.trigger(INSTANCE, TRIGGER1, PARAMETERS);
  }

  @Test
  public void shouldFailTriggerOnExceptionFromTransaction() throws Exception {
    reset(storage);
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.empty());
    var cause = new IOException("fail!");
    when(storage.runInTransactionWithRetries(any())).thenThrow(cause);
    exception.expectCause(is(cause));
    stateManager.trigger(INSTANCE, TRIGGER1, PARAMETERS);
  }

  @Test
  public void shouldRejectTriggerIfIsClosed() throws Exception {
    stateManager.close();
    exception.expect(IsClosedException.class);
    stateManager.trigger(INSTANCE, TRIGGER1, PARAMETERS);
  }

  @Test
  public void shouldRejectEventIfClosed() throws Exception {
    stateManager.close();
    exception.expect(IsClosedException.class);
    stateManager.receive(Event.timeTrigger(INSTANCE));
  }

  @Test
  public void shouldWriteEvents() throws Exception {
    Event event = Event.started(INSTANCE);
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.SUBMITTED, StateData.zero(), NOW, 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);

    stateManager.receive(event);

    verify(storage).writeEvent(SequenceEvent.create(event, 18, NOW.toEpochMilli()));
  }

  @Test
  public void shouldFailReceiveEventWithHigherCounter() throws Exception {
    Event event = Event.started(INSTANCE);

    when(transaction.readActiveState(INSTANCE)).thenReturn(
        Optional.of(RunState.create(INSTANCE, State.SUBMITTED, StateData.zero(), NOW, 17)));

    try {
      stateManager.receive(event, 16);
      fail();
    } catch (StateTransitionConflictException ignore) {
    }

    verify(storage, never()).writeEvent(any());
  }

  @Test
  public void shouldFailReceiveEventWithLowerCounter() throws Exception {
    Event event = Event.started(INSTANCE);
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.SUBMITTED, StateData.zero(), NOW.minusMillis(1), 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);

    try {
      stateManager.receive(event, 18);
      fail();
    } catch (RuntimeException e) {
      assertThat(e.getMessage(), startsWith("Unexpected current counter is less than last observed one for"));
    }

    verify(storage, never()).writeEvent(any());
  }

  @Test
  public void shouldRemoveStateIfTerminal() throws Exception {
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.TERMINATED, StateData.zero(),
            NOW, 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);

    Event event = Event.success(INSTANCE);
    stateManager.receive(event);

    verify(transaction).deleteActiveState(INSTANCE);
    verify(storage).writeEvent(SequenceEvent.create(event, 18, NOW.toEpochMilli()));
  }

  @Test
  public void shouldWriteActiveStateOnEvent() throws Exception {
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.QUEUED, StateData.zero(), NOW, 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);

    stateManager.receive(Event.dequeue(INSTANCE, ImmutableSet.of()));

    verify(transaction).updateActiveState(INSTANCE, RunState.create(INSTANCE, State.PREPARE,
        StateData.newBuilder().resourceIds(ImmutableSet.of()).build(), NOW, 18));
  }

  @Test
  public void shouldPreventIllegalStateTransition() throws Exception {
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.QUEUED, StateData.zero(), NOW.minusMillis(1), 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);


    try {
      stateManager.receive(Event.terminate(INSTANCE, Optional.empty()));
      fail();
    } catch (IllegalStateException ignore) {
    }

    verify(transaction, never()).updateActiveState(any(), any());
  }

  @Test
  public void shouldFailReceiveForUnknownActiveWFInstance() throws Exception {
    when(transaction.readActiveState(INSTANCE)).thenReturn(Optional.empty());

    try {
      stateManager.receive(Event.terminate(INSTANCE, Optional.empty()));
      fail();
    } catch (IllegalArgumentException ignore) {
    }
    verify(transaction, never()).updateActiveState(any(), any());
  }

  @Test
  public void shouldGetRunState() throws Exception {
    RunState runState = RunState.create(
        INSTANCE, State.QUEUED, StateData.zero(), NOW.minusMillis(1), 17);
    when(storage.readActiveState(INSTANCE)).thenReturn(Optional.of(runState));

    RunState returnedRunState = stateManager.getActiveState(INSTANCE).orElseThrow();

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
    when(storage.getLatestStoredCounter(any())).thenReturn(Optional.of(-1L));
    when(transaction.workflow(INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    final RuntimeException rootCause = new RuntimeException("foo!");
    doThrow(rootCause).when(outputHandler).transitionInto(any(), any());
    try {
      stateManager.trigger(INSTANCE, TRIGGER1, PARAMETERS);
      fail();
    } catch (Exception e) {
      assertThat(Throwables.getRootCause(e), is(rootCause));
    }
  }

  @Test
  public void receiveShouldPropagateOutputHandlerException() throws Exception {
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.QUEUED, StateData.zero(), NOW.minusMillis(1), 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);

    final RuntimeException rootCause = new RuntimeException("foo!");
    doThrow(rootCause).when(outputHandler).transitionInto(any(), any());
    try {
      stateManager.receive(Event.dequeue(INSTANCE, ImmutableSet.of()));
      fail();
    } catch (Exception e) {
      assertThat(Throwables.getRootCause(e), is(rootCause));
    }
  }

  @Test
  public void receiveShouldHandleOutputHandlerThrowingStateTransitionConflict() throws Exception {
    Optional<RunState> runState = Optional.of(
        RunState.create(INSTANCE, State.QUEUED, StateData.zero(), NOW.minusMillis(1), 17));
    when(transaction.readActiveState(INSTANCE)).thenReturn(runState);

    final StateTransitionConflictException rootCause = new StateTransitionConflictException("conflict!");
    doThrow(rootCause).when(outputHandler).transitionInto(any(), any());
    stateManager.receive(Event.dequeue(INSTANCE, ImmutableSet.of()));
    verify(logger).debug("State transition conflict when invoking output handler: {}", INSTANCE, rootCause);
  }

  @Test
  public void receiveShouldLogFailure() throws Exception {
    final Exception cause = new Exception("fubared");
    reset(storage);
    when(storage.runInTransactionWithRetries(any())).thenThrow(cause);
    final Event event = Event.started(INSTANCE);
    try {
      stateManager.receive(event, 4711L);
      fail();
    } catch (Exception ignore) {
    }
    verify(logger).debug(
        "Failed workflow instance transition: {}, counter={}",
        event, 4711L, cause);
  }

  @Test
  public void triggerShouldLogTransactionConflict() throws Exception {
    final TransactionException cause = new TransactionException(
        new DatastoreException(10, "foo", "bar"));
    reset(storage);
    when(storage.getLatestStoredCounter(INSTANCE)).thenReturn(Optional.empty());
    when(storage.runInTransactionWithRetries(any())).thenThrow(cause);
    try {
      stateManager.trigger(INSTANCE, Trigger.natural(), PARAMETERS);
      fail();
    } catch (Exception ignore) {
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
    when(storage.runInTransactionWithRetries(any())).thenThrow(cause);
    try {
      stateManager.trigger(INSTANCE, Trigger.natural(), PARAMETERS);
      fail();
    } catch (Exception ignore) {
    }
    verify(logger).debug("Transaction failure when triggering workflow instance: {}: {}",
        INSTANCE, cause.getMessage(), cause);
  }

  @Test
  public void triggerShouldLogFailure() throws Exception {
    final Exception cause = new Exception("fubared");
    reset(storage);
    when(storage.getLatestStoredCounter(INSTANCE)).thenReturn(Optional.empty());
    when(storage.runInTransactionWithRetries(any())).thenThrow(cause);
    try {
      stateManager.trigger(INSTANCE, Trigger.natural(), PARAMETERS);
      fail();
    } catch (Exception ignore) {
    }
    verify(logger).debug("Failure when triggering workflow instance: {}: {}",
        INSTANCE, cause.getMessage(), cause);
  }

  @Test
  public void shouldThrowRuntimeException() throws Exception {
    final IOException exception = new IOException();
    doThrow(exception).when(storage).runInTransactionWithRetries(any());
    try {
      stateManager.receive(Event.dequeue(INSTANCE, ImmutableSet.of()));
      fail();
    } catch (Exception e) {
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

    final Set<Resource> resources = ImmutableSet.of(Resource.create("resource1", 1));
    final List<String> resourceIds = resources.stream().map(Resource::id).sorted().collect(toList());
    final Event dequeueEvent = Event.dequeue(INSTANCE, ImmutableSet.copyOf(resourceIds));
    final Event infoEvent = Event.info(INSTANCE,
        Message.info(String.format("Resource limit reached for: %s", resourceIds)));
    final PersistentStateManager spied = spy(stateManager);
    doNothing().when(spied).receiveIgnoreClosed(eq(infoEvent), anyLong());

    try {
      spied.receive(dequeueEvent);
      fail();
    } catch (Exception e) {
      // expected exception
    }

    verify(spied).receiveIgnoreClosed(eq(infoEvent), anyLong());
  }

  @Test
  public void shouldFailToUpdateResourceCountersOnDequeueDueToCapacityAndNoInfoEventSent() throws Exception {
    final Set<Resource> resources = ImmutableSet.of(Resource.create("resource1", 1));
    final List<String> resourceIds = resources.stream().map(Resource::id).sorted().collect(toList());
    final Message message = Message.info(String.format("Resource limit reached for: %s", resourceIds));
    final RunState runState = RunState.create(INSTANCE, State.QUEUED,
        STATE_DATA_1.builder().messages(message).build(),
        NOW.minusMillis(1), 17);
    when(transaction.readActiveState(INSTANCE)).thenReturn(Optional.of(runState));

    doThrow(new CounterCapacityException("foo"))
        .when(transaction).updateCounter(shardedCounter, "resource1", 1);

    final Event dequeueEvent = Event.dequeue(INSTANCE,
        resources.stream().map(Resource::id).collect(toSet()));
    final PersistentStateManager spied = spy(stateManager);

    try {
      spied.receive(dequeueEvent);
      fail();
    } catch (Exception e) {
      // expected exception
    }

    verify(spied, never()).receiveIgnoreClosed(eq(Event.info(INSTANCE, message)), anyLong());
  }

  @Test
  public void shouldFailToUpdateResourceCountersOnDequeueDueToConflict() throws Exception {
    givenState(INSTANCE, State.QUEUED);

    var rootCause = new DatastoreIOException(new DatastoreException(10, "conflict!", "conflict!"));

    doThrow(rootCause).when(transaction).updateCounter(shardedCounter, "resource1", 1);

    var dequeueEvent = Event.dequeue(INSTANCE, Set.of("resource1"));

    exception.expect(RuntimeException.class);
    exception.expectCause(is(rootCause));

    stateManager.receive(dequeueEvent);
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
    receiveEvent(Event.submitted(INSTANCE,"styx-run-1", "test"));
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
    final PersistentStateManager spied = spy(stateManager);
    spied.close();

    spied.receiveIgnoreClosed(Event.started(INSTANCE));
    verify(spied).ensureRunning();
    verifyNoMoreInteractions(storage);
  }

  @Test
  public void shouldReceiveEventIgnoreClosedWithCounter() throws IOException, IsClosedException {
    final PersistentStateManager spied = spy(stateManager);
    spied.close();

    spied.receiveIgnoreClosed(Event.started(INSTANCE), 17);
    verify(spied).ensureRunning();
    verifyNoMoreInteractions(storage);
  }

  public void givenState(WorkflowInstance instance, State state) throws IOException {
    final RunState runState = RunState.create(instance, state, STATE_DATA_1, NOW.minusMillis(1), 17);
    when(transaction.readActiveState(instance)).thenReturn(Optional.of(runState));
  }

  public void receiveEvent(Event event) throws Exception {
    stateManager.receive(event);
  }
}
