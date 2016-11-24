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
import static com.spotify.styx.state.QueuedStateManager.NO_EVENTS_PROCESSED;
import static com.spotify.styx.state.TimeoutConfig.createWithDefaultTtl;
import static java.time.Duration.ofMillis;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.ForkJoinPool.commonPool;
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
import java.time.Instant;
import java.util.Collections;
import java.util.Optional;
import java.util.SortedSet;
import java.util.Stack;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
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

  private static final ExecutorService POOL = Executors.newFixedThreadPool(16);

  QueuedStateManager stateManager;
  InMemStorage storage = new InMemStorage();

  Stack<RunState> transitions = new Stack<>();

  @Rule
  public RepeatRule repeatRule = new RepeatRule();

  private void setUp(long timeoutMillis) throws Exception {
    setUp(timeoutMillis, RunState.fresh(INSTANCE, transitions::push));
  }

  private void setUp(long timeoutMillis, RunState initial) throws Exception {
    TimeoutConfig timeoutConfig = createWithDefaultTtl(ofMillis(timeoutMillis));
    if (stateManager != null) {
      stateManager.close();
    }

    storage = new InMemStorage();
    stateManager = new QueuedStateManager(timeoutConfig, Instant::now, POOL, storage);

    stateManager.initialize(initial);
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
    setUp(0);

    stateManager.receive(Event.triggerExecution(INSTANCE, "trig"));
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
    setUp(0);

    stateManager.receive(Event.triggerExecution(INSTANCE, "trig1"));
    stateManager.receive(Event.halt(INSTANCE));
    assertTrue(stateManager.awaitIdle(1000));

    stateManager.initialize(RunState.fresh(INSTANCE));
    stateManager.receive(Event.triggerExecution(INSTANCE, "trig2"));
    stateManager.receive(Event.created(INSTANCE, TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    stateManager.receive(Event.started(INSTANCE));
    stateManager.receive(Event.halt(INSTANCE));
    assertTrue(stateManager.awaitIdle(1000));

    stateManager.initialize(RunState.fresh(INSTANCE));
    stateManager.receive(Event.triggerExecution(INSTANCE, "trig3"));
    assertTrue(stateManager.awaitIdle(1000));

    SortedSet<SequenceEvent> storedEvents = storage.readEvents(INSTANCE);
    SequenceEvent lastStoredEvent = storedEvents.last();
    assertThat(lastStoredEvent.event(), is(Event.triggerExecution(INSTANCE, "trig3")));
    assertThat(storage.getLatestStoredCounter(INSTANCE), hasValue(6L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE), hasValue(6L));
  }

  @Test(expected = RuntimeException.class)
  public void shouldFailInitializeWFIfAlreadyActive() throws Exception {
    setUp(0);

    stateManager.initialize(RunState.fresh(INSTANCE));
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

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(transitions, hasSize(1));
    assertThat(transitions.pop().state(), is(RunState.State.SUBMITTED));
  }

  @Test
  public void shouldWriteEvents() throws Exception {
    setUp(0);

    stateManager.receive(Event.timeTrigger(INSTANCE));
    assertTrue(stateManager.awaitIdle(1000));

    stateManager.receive(Event.started(INSTANCE));
    assertTrue(stateManager.awaitIdle(1000));

    stateManager.receive(Event.timeout(INSTANCE));
    assertTrue(stateManager.awaitIdle(1000));

    stateManager.receive(Event.retry(INSTANCE));
    assertTrue(stateManager.awaitIdle(1000));
    assertThat(storage.writtenEvents, hasSize(4));

    assertThat(storage.writtenEvents.get(0).counter(), is(0L));
    assertThat(storage.writtenEvents.get(0).event(), is(Event.timeTrigger(INSTANCE)));
    assertThat(storage.writtenEvents.get(1).counter(), is(1L));
    assertThat(storage.writtenEvents.get(1).event(), is(Event.started(INSTANCE)));
    assertThat(storage.writtenEvents.get(2).counter(), is(2L));
    assertThat(storage.writtenEvents.get(2).event(), is(Event.timeout(INSTANCE)));
    assertThat(storage.writtenEvents.get(3).counter(), is(3L));
    assertThat(storage.writtenEvents.get(3).event(), is(Event.retry(INSTANCE)));
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
    assertThat(storage.getCounterFromActiveStates(INSTANCE), hasValue(NO_EVENTS_PROCESSED));

    stateManager.receive(Event.timeTrigger(INSTANCE));          // 0
    stateManager.receive(Event.started(INSTANCE));              // 1
    stateManager.receive(Event.terminate(INSTANCE, 0));         // 2

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(storage.getLatestStoredCounter(INSTANCE), hasValue(2L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE), hasValue(2L));

    stateManager.receive(Event.success(INSTANCE));

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(storage.activeStatesMap.values(), is(empty()));
  }

  @Test
  public void shouldNotStoreEventOnIllegalStateTransition() throws Exception {
    setUp(0);

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
    stateManager = new QueuedStateManager(
        createWithDefaultTtl(ofMillis(0)), Instant::now, newSingleThreadExecutor(),
        storage);

    stateManager.restore(RunState.fresh(INSTANCE), 7L);
    stateManager.receive(Event.timeTrigger(INSTANCE));  // 8

    assertTrue(stateManager.awaitIdle(1000));
    assertThat(storage.activeStatesMap, hasKey(INSTANCE));
    assertThat(storage.getLatestStoredCounter(INSTANCE), hasValue(8L));
    assertThat(storage.getCounterFromActiveStates(INSTANCE), hasValue(8L));
  }

  @Test
  public void shouldHandleThrowingOutputHandler() throws Exception {
    stateManager = new QueuedStateManager(
        createWithDefaultTtl(ofMillis(0)), Instant::now, newSingleThreadExecutor(),
        storage);

    OutputHandler throwing = (state) -> {
      throw new RuntimeException();
    };

    stateManager.initialize(RunState.fresh(INSTANCE, throwing));
    stateManager.receive(Event.triggerExecution(INSTANCE, "trig"));

    assertTrue(stateManager.awaitIdle(5000));
  }

  @Test
  public void testGetActiveWorkflowInstance() throws Exception {
    stateManager = new QueuedStateManager(
        createWithDefaultTtl(ofMillis(0)), Instant::now, newSingleThreadExecutor(), storage);

    assertThat(stateManager.isActiveWorkflowInstance(INSTANCE), is(false));

    stateManager.initialize(RunState.fresh(INSTANCE, transitions::push));

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
    setUp(0);

    IntFunction<WorkflowInstance > wfi =
        j -> WorkflowInstance.create(TestData.WORKFLOW_ID, "id-" + j);

    Consumer<WorkflowInstance> sendEvents = instance -> {
      try {
        stateManager.receive(Event.submit(instance, ExecutionDescription.create(
            "", Collections.emptyList(), Optional.empty(), Optional.empty())));
        stateManager.receive(Event.submitted(instance, "id"));
        stateManager.receive(Event.started(instance));
        stateManager.receive(Event.terminate(instance, 20));
        stateManager.receive(Event.retryAfter(instance, 300));
        stateManager.receive(Event.retry(instance));
      } catch (StateManager.IsClosed ignored) {
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
          stateManager.initialize(RunState.fresh(instance));
          stateManager.receive(Event.triggerExecution(instance, "trig"));
        } catch (StateManager.IsClosed ignored) {
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
