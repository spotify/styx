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

import static com.github.npathai.hamcrestopt.OptionalMatchers.hasValue;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static java.util.concurrent.ForkJoinPool.commonPool;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.spotify.styx.RepeatRule;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.storage.InMemStorage;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.IsClosedException;
import java.time.Instant;
import java.util.Optional;
import java.util.SortedSet;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

public class QueuedStateManagerTest {

  private static final WorkflowInstance INSTANCE = WorkflowInstance.create(
      TestData.WORKFLOW_ID, "2016-05-01");

  private final static String TEST_EXECUTION_ID_1 = "execution_1";
  private final static String DOCKER_IMAGE = "busybox:1.1";

  private static final Trigger TRIGGER1 = Trigger.unknown("trig1");
  private static final Trigger TRIGGER2 = Trigger.unknown("trig2");
  private static final Trigger TRIGGER3 = Trigger.unknown("trig3");
  private static final BiConsumer<SequenceEvent, RunState> eventConsumer = (e, s) -> {};

  private static final ExecutorService POOL1 = Executors.newFixedThreadPool(16);
  private static final Executor POOL2 = Executors.newSingleThreadExecutor();

  QueuedStateManager stateManager;
  InMemStorage storage = new InMemStorage();

  Stack<RunState> transitions = new Stack<>();

  @Rule
  public RepeatRule repeatRule = new RepeatRule();

  private void setUp() throws Exception {
    setUp(RunState.fresh(INSTANCE, transitions::push));
  }

  private void setUp(RunState initial) throws Exception {
    if (stateManager != null) {
      stateManager.close();
    }

    storage = new InMemStorage();
    stateManager = new QueuedStateManager(Instant::now, POOL1, storage, eventConsumer, POOL2, outputHandlers);

    stateManager.trigger(initial, trigger);
    assertTrue(stateManager.awaitIdle(1000));
  }

  @After
  public void tearDown() throws Exception {
    if (stateManager != null) {
      stateManager.close();
    }
  }

  @Test
  public void shouldNotBeActiveAfterHalt() throws Exception {
    setUp();

    stateManager.receive(Event.triggerExecution(INSTANCE, TRIGGER1));
    stateManager.receive(Event.halt(INSTANCE));

    assertTrue(stateManager.awaitIdle(1000));

    SortedSet<SequenceEvent> storedEvents = storage.readEvents(INSTANCE);
    SequenceEvent lastStoredEvent = storedEvents.last();
    assertThat(lastStoredEvent.event(), is(Event.halt(INSTANCE)));
    assertThat(storage.getLatestStoredCounter(INSTANCE), hasValue(1L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE).isPresent(), is(false));
  }

  @Test
  public void shouldInitializeWFInstanceFromNextCounter() throws Exception {
    setUp();

    stateManager.receive(Event.triggerExecution(INSTANCE, TRIGGER1));
    stateManager.receive(Event.dequeue(INSTANCE));
    stateManager.receive(Event.halt(INSTANCE));
    assertTrue(stateManager.awaitIdle(1000));

    stateManager.trigger(RunState.fresh(INSTANCE), trigger);
    stateManager.receive(Event.triggerExecution(INSTANCE, TRIGGER2));
    stateManager.receive(Event.dequeue(INSTANCE));
    stateManager.receive(Event.created(INSTANCE, TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    stateManager.receive(Event.started(INSTANCE));
    stateManager.receive(Event.halt(INSTANCE));
    assertTrue(stateManager.awaitIdle(1000));

    stateManager.trigger(RunState.fresh(INSTANCE), trigger);
    stateManager.receive(Event.triggerExecution(INSTANCE, TRIGGER3));
    assertTrue(stateManager.awaitIdle(1000));

    SortedSet<SequenceEvent> storedEvents = storage.readEvents(INSTANCE);
    SequenceEvent lastStoredEvent = storedEvents.last();
    assertThat(lastStoredEvent.event(), is(Event.triggerExecution(INSTANCE, TRIGGER3)));
    assertThat(storage.getLatestStoredCounter(INSTANCE), hasValue(8L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE), hasValue(8L));
  }

  @Test(expected = RuntimeException.class)
  public void shouldFailInitializeWFIfAlreadyActive() throws Exception {
    setUp();

    stateManager.trigger(RunState.fresh(INSTANCE), trigger);
  }

  @Test(expected = IsClosedException.class)
  public void shouldRejectInitializeIfClosed() throws Exception {
    setUp();

    stateManager.close();
    stateManager.trigger(RunState.fresh(INSTANCE), trigger);
  }

  @Test(expected = IsClosedException.class)
  public void shouldRejectEventIfClosed() throws Exception {
    setUp();

    stateManager.close();
    stateManager.receive(Event.timeTrigger(INSTANCE));
  }

  @Test
  public void shouldCloseGracefully() throws Exception {
    setUp();

    stateManager.receive(Event.timeTrigger(INSTANCE));
    stateManager.close();

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(transitions, hasSize(1));
    assertThat(transitions.pop().state(), is(RunState.State.SUBMITTED));
  }

  @Test
  public void shouldWriteEvents() throws Exception {
    setUp();

    stateManager.receive(Event.timeTrigger(INSTANCE));
    assertTrue(stateManager.awaitIdle(1000));

    stateManager.receive(Event.started(INSTANCE));
    assertTrue(stateManager.awaitIdle(1000));

    stateManager.receive(Event.timeout(INSTANCE));
    assertTrue(stateManager.awaitIdle(1000));

    stateManager.receive(Event.retryAfter(INSTANCE, 10));
    assertTrue(stateManager.awaitIdle(1000));
    assertThat(storage.writtenEvents, hasSize(4));

    assertThat(storage.writtenEvents.get(0).counter(), is(0L));
    assertThat(storage.writtenEvents.get(0).event(), is(Event.timeTrigger(INSTANCE)));
    assertThat(storage.writtenEvents.get(1).counter(), is(1L));
    assertThat(storage.writtenEvents.get(1).event(), is(Event.started(INSTANCE)));
    assertThat(storage.writtenEvents.get(2).counter(), is(2L));
    assertThat(storage.writtenEvents.get(2).event(), is(Event.timeout(INSTANCE)));
    assertThat(storage.writtenEvents.get(3).counter(), is(3L));
    assertThat(storage.writtenEvents.get(3).event(), is(Event.retryAfter(INSTANCE, 10)));
  }

  @Test
  public void shouldRemoveStateIfTerminal() throws Exception {
    setUp();

    stateManager.receive(Event.timeTrigger(INSTANCE));
    stateManager.receive(Event.started(INSTANCE));
    stateManager.receive(Event.terminate(INSTANCE, Optional.of(0)));
    stateManager.receive(Event.success(INSTANCE));

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(stateManager.get(INSTANCE), is(nullValue()));
  }

  @Test
  public void shouldWriteActiveStateOnEvent() throws Exception {
    setUp();

    assertThat(storage.activeStatesMap.isEmpty(), is(true));
    assertThat(storage.getCounterFromActiveStates(INSTANCE), isEmpty());

    stateManager.receive(Event.timeTrigger(INSTANCE))   // 0
        .toCompletableFuture().get(1, MINUTES);

    assertThat(storage.activeStatesMap, hasKey(INSTANCE));
    assertThat(storage.getCounterFromActiveStates(INSTANCE), hasValue(0L));

    stateManager.receive(Event.started(INSTANCE))       // 1
        .toCompletableFuture().get(1, MINUTES);

    assertThat(storage.getCounterFromActiveStates(INSTANCE), hasValue(1L));

    stateManager.receive(Event.terminate(INSTANCE, Optional.of(0)))  // 2
        .toCompletableFuture().get(1, MINUTES);

    assertThat(storage.getCounterFromActiveStates(INSTANCE), hasValue(2L));

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(storage.getLatestStoredCounter(INSTANCE), hasValue(2L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE), hasValue(2L));

    stateManager.receive(Event.success(INSTANCE));

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(storage.activeStatesMap.values(), is(empty()));
  }

  @Test
  public void shouldNotStoreEventOnIllegalStateTransition() throws Exception {
    setUp();

    stateManager.receive(Event.timeTrigger(INSTANCE));
    assertTrue(stateManager.awaitIdle(1000));

    stateManager.receive(Event.started(INSTANCE));
    assertTrue(stateManager.awaitIdle(1000));

    stateManager.receive(Event.started(INSTANCE)); // causes illegal transition

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(storage.getLatestStoredCounter(INSTANCE), hasValue(1L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE), hasValue(1L));
    assertThat(storage.writtenEvents, hasSize(2));

    assertThat(storage.writtenEvents.get(0).counter(), is(0L));
    assertThat(storage.writtenEvents.get(0).event(), is(Event.timeTrigger(INSTANCE)));
    assertThat(storage.writtenEvents.get(1).counter(), is(1L));
    assertThat(storage.writtenEvents.get(1).event(), is(Event.started(INSTANCE)));
  }

  @Test
  public void shouldRestoreStateAtCount() throws Exception {
    stateManager = new QueuedStateManager(Instant::now, POOL1, storage, eventConsumer, POOL2, outputHandlers);

    stateManager.restore(RunState.fresh(INSTANCE), 7L);
    stateManager.receive(Event.timeTrigger(INSTANCE));  // 8

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(storage.activeStatesMap, hasKey(INSTANCE));
    assertThat(storage.getLatestStoredCounter(INSTANCE), hasValue(8L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE), hasValue(8L));
  }

  @Test
  public void shouldHandleThrowingOutputHandler() throws Exception {
    stateManager = new QueuedStateManager(Instant::now, POOL1, storage, eventConsumer, POOL2, outputHandlers);

    OutputHandler throwing = (state) -> {
      throw new RuntimeException();
    };

    stateManager.trigger(RunState.fresh(INSTANCE, throwing), trigger);
    stateManager.receive(Event.triggerExecution(INSTANCE, TRIGGER1));

    assertTrue(stateManager.awaitIdle(5000));
  }

  @Test
  public void testGetActiveWorkflowInstance() throws Exception {
    stateManager = new QueuedStateManager(Instant::now, POOL1, storage, eventConsumer, POOL2, outputHandlers);

    assertThat(stateManager.isActiveWorkflowInstance(INSTANCE), is(false));

    stateManager.trigger(RunState.fresh(INSTANCE, transitions::push), trigger);

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(stateManager.isActiveWorkflowInstance(INSTANCE), is(true));
  }

  /**
   * Repeated many times as we're doing some sort of concurrency test. It's hard to guarantee
   * correctness, but repetition should increase our confidence.
   */
  @Test
  @RepeatRule.Repeat(times = 50)
  public void testConcurrentEvents() throws Exception {
    setUp();

    IntFunction<WorkflowInstance > wfi =
        j -> WorkflowInstance.create(TestData.WORKFLOW_ID, "id-" + j);

    Consumer<WorkflowInstance> sendEvents = instance -> {
      try {
        stateManager.receive(Event.submit(instance, ExecutionDescription.forImage(""), "id"));
        stateManager.receive(Event.submitted(instance, "id"));
        stateManager.receive(Event.started(instance));
        stateManager.receive(Event.terminate(instance, Optional.of(20)));
        stateManager.receive(Event.retryAfter(instance, 300));
        stateManager.receive(Event.dequeue(instance));
      } catch (IsClosedException ignored) {
      }
    };

    int states = 50;
    CountDownLatch initLatch = new CountDownLatch(states);
    CountDownLatch eventsLatch = new CountDownLatch(states);

    for (int j = 0; j < states; j++) {
      int jj = j;
      commonPool().execute(() -> {
        WorkflowInstance instance = wfi.apply(jj);

        try {
          stateManager.trigger(RunState.fresh(instance), trigger);
          stateManager.receive(Event.triggerExecution(instance, TRIGGER1));
          stateManager.receive(Event.dequeue(instance));
        } catch (IsClosedException ignored) {
        }

        initLatch.countDown();
      });
    }

    assertTrue(initLatch.await(1000, TimeUnit.SECONDS));

    for (int j = 0; j < states; j++) {
      WorkflowInstance instance = wfi.apply(j);

      commonPool().execute(() -> {
        for (int k = 0; k < 100; k++) {
          sendEvents.accept(instance);
        }
        eventsLatch.countDown();
      });
    }

    assertTrue(eventsLatch.await(1000, TimeUnit.SECONDS));
    assertTrue(stateManager.awaitIdle(5000));

    for (int j = 0; j < states; j++) {
      assertThat(stateManager.getQueuedEventsCount(), is(0L));

      WorkflowInstance instance = wfi.apply(j);
      RunState runState = stateManager.get(instance);
      assertThat(instance.toKey(), runState.state(), is(RunState.State.PREPARE));
      assertThat(instance.toKey(), runState.data().tries(), is(100));
    }
  }
}
