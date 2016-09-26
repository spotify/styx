/*
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

import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.storage.InMemStorage;
import com.spotify.styx.testdata.TestData;

import org.junit.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

import static com.spotify.styx.state.TimeoutConfig.createWithDefaultTtl;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class QueuedStateManagerTest {

  private static final WorkflowInstance INSTANCE = WorkflowInstance.create(
      TestData.WORKFLOW_ID, "2016-05-01");
  private static final String STARTING_CLOCK_TIME = "2015-12-31T23:59:10.000Z";
  private final static String TEST_EXECUTION_ID_1 = "execution_1";
  private final static String DOCKER_IMAGE = "busybox:1.1";
  private Instant i;

  QueuedStateManager stateManager;
  InMemStorage storage = new InMemStorage();

  Stack<RunState> transitions = new Stack<>();

  private void setUp(long timeoutMillis) throws Exception {
    setUp(timeoutMillis, RunState.fresh(
        INSTANCE,
        () -> Instant.parse(STARTING_CLOCK_TIME),
        transitions::push));
  }

  private void setUp(long timeoutMillis, RunState initial) throws Exception {
    TimeoutConfig timeoutConfig = createWithDefaultTtl(ofMillis(timeoutMillis));
    i = Instant.parse(STARTING_CLOCK_TIME);
    stateManager = new QueuedStateManager(timeoutConfig,
                                          () -> i,
                                          newSingleThreadExecutor(),
                                          storage);

    stateManager.initialize(initial);
    assertTrue(stateManager.awaitIdle(1000));
  }

  @Test
  public void shouldNotBeActiveAfterHalt() throws Exception {
    setUp(0);

    stateManager.receive(Event.triggerExecution(INSTANCE, "trig"));
    stateManager.receive(Event.halt(INSTANCE));
    Thread.sleep(1000);

    Set<SequenceEvent> storedEvents = storage.readEvents(INSTANCE);
    Optional<SequenceEvent> lastStoredEvent = storedEvents.stream().reduce((a, b) -> b);
    assertThat(lastStoredEvent.isPresent(), is(true));
    assertThat(lastStoredEvent.get().event(), is(Event.halt(INSTANCE)));
    assertThat(storage.getLatestStoredCounter(INSTANCE).isPresent(), is(true));
    assertThat(storage.getLatestStoredCounter(INSTANCE).get(), is(1L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).isPresent(), is(false));
  }

  @Test
  public void shouldInitializeWFInstanceFromNextCounter() throws Exception {
    setUp(0);

    stateManager.receive(Event.triggerExecution(INSTANCE, "trig1"));
    stateManager.receive(Event.halt(INSTANCE));
    stateManager.initialize(RunState.fresh(
        INSTANCE,
        () -> Instant.parse(STARTING_CLOCK_TIME),
        transitions::push));
    stateManager.receive(Event.triggerExecution(INSTANCE, "trig2"));
    stateManager.receive(Event.created(INSTANCE, TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    stateManager.receive(Event.started(INSTANCE));
    stateManager.receive(Event.halt(INSTANCE));
    stateManager.initialize(RunState.fresh(
        INSTANCE,
        () -> Instant.parse(STARTING_CLOCK_TIME),
        transitions::push));
    stateManager.receive(Event.triggerExecution(INSTANCE, "trig3"));
    Thread.sleep(1000);

    Set<SequenceEvent> storedEvents = storage.readEvents(INSTANCE);
    Optional<SequenceEvent> lastStoredEvent = storedEvents.stream().reduce((a, b) -> b);
    assertThat(lastStoredEvent.isPresent(), is(true));
    assertThat(lastStoredEvent.get().event(), is(Event.triggerExecution(INSTANCE, "trig3")));
    assertThat(storage.getLatestStoredCounter(INSTANCE).isPresent(), is(true));
    assertThat(storage.getLatestStoredCounter(INSTANCE).get(), is(6L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).isPresent(), is(true));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).get(), is(6L));
  }

  @Test
  public void shouldFailInitializeWFIfAlreadyActive() throws Exception {
    setUp(0);

    stateManager.receive(Event.triggerExecution(INSTANCE, "trig1"));
    stateManager.initialize(RunState.fresh(
        INSTANCE,
        () -> Instant.parse(STARTING_CLOCK_TIME),
        transitions::push));
    stateManager.receive(Event.triggerExecution(INSTANCE, "trig2"));
    stateManager.receive(Event.created(INSTANCE, TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    stateManager.initialize(RunState.fresh(
        INSTANCE,
        () -> Instant.parse(STARTING_CLOCK_TIME),
        transitions::push));
    stateManager.receive(Event.triggerExecution(INSTANCE, "trig3"));
    Thread.sleep(1000);

    Set<SequenceEvent> storedEvents = storage.readEvents(INSTANCE);
    Optional<SequenceEvent> lastStoredEvent = storedEvents.stream().reduce((a, b) -> b);
    assertThat(lastStoredEvent.isPresent(), is(true));
    assertThat(lastStoredEvent.get().event(), is(Event.created(INSTANCE, TEST_EXECUTION_ID_1, DOCKER_IMAGE)));
    assertThat(storage.getLatestStoredCounter(INSTANCE).isPresent(), is(true));
    assertThat(storage.getLatestStoredCounter(INSTANCE).get(), is(1L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).isPresent(), is(true));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).get(), is(1L));
  }

  @Test
  public void shouldHandleSimultaneousTriggers1() throws Exception {
    setUp(0);

    stateManager.initialize(RunState.fresh(
        INSTANCE,
        () -> Instant.parse(STARTING_CLOCK_TIME),
        transitions::push));
    stateManager.receive(Event.triggerExecution(INSTANCE, "trig1"));
    stateManager.receive(Event.triggerExecution(INSTANCE, "trig2"));
    Thread.sleep(1000);

    Set<SequenceEvent> storedEvents = storage.readEvents(INSTANCE);
    Optional<SequenceEvent> lastStoredEvent = storedEvents.stream().reduce((a, b) -> b);
    assertThat(lastStoredEvent.isPresent(), is(true));
    assertThat(lastStoredEvent.get().event(), is(Event.triggerExecution(INSTANCE, "trig1")));
    assertThat(storage.getLatestStoredCounter(INSTANCE).isPresent(), is(true));
    assertThat(storage.getLatestStoredCounter(INSTANCE).get(), is(0L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).isPresent(), is(true));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).get(), is(0L));
  }

  @Test
  public void shouldHandleSimultaneousTriggers2() throws Exception {
    setUp(0);

    stateManager.receive(Event.triggerExecution(INSTANCE, "trig1"));
    stateManager.initialize(RunState.fresh(
        INSTANCE,
        () -> Instant.parse(STARTING_CLOCK_TIME),
        transitions::push));
    stateManager.receive(Event.triggerExecution(INSTANCE, "trig2"));
    Thread.sleep(1000);

    Set<SequenceEvent> storedEvents = storage.readEvents(INSTANCE);
    Optional<SequenceEvent> lastStoredEvent = storedEvents.stream().reduce((a, b) -> b);
    assertThat(lastStoredEvent.isPresent(), is(true));
    assertThat(lastStoredEvent.get().event(), is(Event.triggerExecution(INSTANCE, "trig1")));
    assertThat(storage.getLatestStoredCounter(INSTANCE).isPresent(), is(true));
    assertThat(storage.getLatestStoredCounter(INSTANCE).get(), is(0L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).isPresent(), is(true));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).get(), is(0L));
  }

  @Test
  public void shouldTimeoutActiveState() throws Exception {
    setUp(0);

    stateManager.triggerTimeouts();

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(transitions, hasSize(1));
    assertThat(transitions.pop().state(), is(RunState.State.FAILED));
  }

  @Test
  public void shouldNotTimeoutTerminalState() throws Exception {
    setUp(0, RunState.create(INSTANCE, RunState.State.DONE));

    stateManager.triggerTimeouts();

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(transitions, hasSize(0));
    assertThat(stateManager.get(INSTANCE).state(), is(RunState.State.DONE));
  }

  @Test
  public void shouldNotTransitionIfNotTimedOut() throws Exception {
    setUp(20_000);

    stateManager.triggerTimeouts();

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(transitions, hasSize(0));
  }

  @Test(expected = StateManager.IsClosed.class)
  public void shouldRejectInitializeIfClosed() throws Exception {
    setUp(0);

    stateManager.close();
    stateManager.initialize(RunState.fresh(INSTANCE));
  }

  @Test(expected = StateManager.IsClosed.class)
  public void shouldRejectEventIfClosed() throws Exception {
    setUp(0);

    stateManager.close();
    stateManager.receive(Event.timeTrigger(INSTANCE));
  }

  @Test
  public void shouldCloseGracefully() throws Exception {
    setUp(0);

    stateManager.receive(Event.timeTrigger(INSTANCE));
    stateManager.close();

    assertThat(transitions, hasSize(1));
    assertThat(transitions.pop().state(), is(RunState.State.SUBMITTED));
  }

  @Test
  public void shouldWriteEvents() throws Exception {
    setUp(0);

    stateManager.receive(Event.timeTrigger(INSTANCE));
    // todo: add semantic wait utility
    Thread.sleep(1000);

    i = i.plus(1, ChronoUnit.SECONDS);
    stateManager.receive(Event.started(INSTANCE));
    // todo: add semantic wait utility
    Thread.sleep(1000);

    i = i.plus(1, ChronoUnit.SECONDS);
    stateManager.receive(Event.timeout(INSTANCE));
    // todo: add semantic wait utility
    Thread.sleep(1000);

    i = i.plus(100, ChronoUnit.SECONDS);
    stateManager.receive(Event.retry(INSTANCE));

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(storage.writtenEvents, hasSize(4));

    assertThat(storage.writtenEvents.get(0),
               is(SequenceEvent.create(
                   Event.timeTrigger(INSTANCE),
                   0L,
                   Instant.parse(STARTING_CLOCK_TIME).toEpochMilli())));
    assertThat(storage.writtenEvents.get(1),
               is(SequenceEvent.create(
                   Event.started(INSTANCE),
                   1L,
                   Instant.parse(STARTING_CLOCK_TIME).plus(1, ChronoUnit.SECONDS).toEpochMilli())));
    assertThat(storage.writtenEvents.get(2),
               is(SequenceEvent.create(
                   Event.timeout(INSTANCE),
                   2L,
                   Instant.parse(STARTING_CLOCK_TIME).plus(2, ChronoUnit.SECONDS).toEpochMilli())));
    assertThat(storage.writtenEvents.get(3),
               is(SequenceEvent.create(
                   Event.retry(INSTANCE),
                   3L,
                   Instant.parse(STARTING_CLOCK_TIME).plus(102, ChronoUnit.SECONDS).toEpochMilli())));
  }

  @Test
  public void shouldRemoveStateIfTerminal() throws Exception {
    setUp(0);

    stateManager.receive(Event.timeTrigger(INSTANCE));
    stateManager.receive(Event.started(INSTANCE));
    stateManager.receive(Event.terminate(INSTANCE, 0));
    stateManager.receive(Event.success(INSTANCE));

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(stateManager.get(INSTANCE), is(nullValue()));
  }

  @Test
  public void shouldActivateStateOnInitialize() throws Exception {
    setUp(0);

    assertThat(storage.activeStatesMap, hasKey(INSTANCE));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).isPresent(), is(true));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).get(),
               is(QueuedStateManager.NO_EVENTS_PROCESSED));

    stateManager.receive(Event.timeTrigger(INSTANCE));          // 0
    stateManager.receive(Event.started(INSTANCE));              // 1
    stateManager.receive(Event.terminate(INSTANCE, 0));         // 2

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(storage.getLatestStoredCounter(INSTANCE).isPresent(), is(true));
    assertThat(storage.getLatestStoredCounter(INSTANCE).get(), is(2L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).isPresent(), is(true));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).get(), is(2L));

    stateManager.receive(Event.success(INSTANCE));

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(storage.activeStatesMap.values(), is(empty()));
  }

  @Test
  public void shouldNotStoreEventOnIllegalStateTransition() throws Exception {
    setUp(0);

    stateManager.receive(Event.timeTrigger(INSTANCE));
    // todo: add semantic wait utility
    Thread.sleep(1000);

    i = i.plus(1, ChronoUnit.SECONDS);
    stateManager.receive(Event.started(INSTANCE));
    // todo: add semantic wait utility
    Thread.sleep(1000);

    i = i.plus(1, ChronoUnit.SECONDS);
    stateManager.receive(Event.started(INSTANCE)); // causes illegal transition

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(storage.getLatestStoredCounter(INSTANCE).isPresent(), is(true));
    assertThat(storage.getLatestStoredCounter(INSTANCE).get(), is(1L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).isPresent(), is(true));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).get(), is(1L));
    assertThat(storage.writtenEvents, hasSize(2));

    assertThat(storage.writtenEvents.get(0),
               is(SequenceEvent.create(
                   Event.timeTrigger(INSTANCE),
                   0L,
                   Instant.parse(STARTING_CLOCK_TIME).toEpochMilli())));
    assertThat(storage.writtenEvents.get(1),
               is(SequenceEvent.create(
                   Event.started(INSTANCE),
                   1L,
                   Instant.parse(STARTING_CLOCK_TIME).plus(1, ChronoUnit.SECONDS).toEpochMilli())));
  }

  @Test
  public void shouldRestoreStateAtCount() throws Exception {
    stateManager = new QueuedStateManager(
        createWithDefaultTtl(ofMillis(0)), Instant::now, newSingleThreadExecutor(),
        storage);

    stateManager.restore(RunState.fresh(INSTANCE), 7L);
    stateManager.receive(Event.timeTrigger(INSTANCE));  // 8

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(storage.activeStatesMap, hasKey(INSTANCE));
    assertThat(storage.getLatestStoredCounter(INSTANCE).isPresent(), is(true));
    assertThat(storage.getLatestStoredCounter(INSTANCE).get(), is(8L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).isPresent(), is(true));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).get(), is(8L));
  }

  @Test
  public void shouldHandleThrowingOutputHandler() throws Exception {
    stateManager = new QueuedStateManager(
        createWithDefaultTtl(ofMillis(0)), Instant::now, newSingleThreadExecutor(),
        storage);

    OutputHandler throwing = (state) -> {
      throw new RuntimeException();
    };

    stateManager.initialize(RunState.fresh(INSTANCE, Instant::now, throwing));
    stateManager.receive(Event.triggerExecution(INSTANCE, "trig"));

    assertTrue(stateManager.awaitIdle(5000));
  }

  @Test
  public void testGetActiveWorkflowInstance() throws Exception {
    stateManager = new QueuedStateManager(
        createWithDefaultTtl(ofMillis(0)), Instant::now, newSingleThreadExecutor(),
        storage);

    assertThat(stateManager.isActiveWorkflowInstance(INSTANCE), is(false));

    stateManager.initialize(RunState.fresh(
        INSTANCE,
        () -> Instant.parse(STARTING_CLOCK_TIME),
        transitions::push));
    // todo: add semantic wait utility
    Thread.sleep(1000);

    assertThat(stateManager.isActiveWorkflowInstance(INSTANCE), is(true));
  }
}
