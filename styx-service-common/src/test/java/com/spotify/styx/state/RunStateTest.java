/*-
 * -\-\-
 * Spotify Styx Common
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

import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresentAnd;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresentAndIs;
import static com.spotify.styx.state.RunState.State.DONE;
import static com.spotify.styx.state.RunState.State.ERROR;
import static com.spotify.styx.state.RunState.State.FAILED;
import static com.spotify.styx.state.RunState.State.PREPARE;
import static com.spotify.styx.state.RunState.State.QUEUED;
import static com.spotify.styx.state.RunState.State.RUNNING;
import static com.spotify.styx.state.RunState.State.SUBMITTED;
import static com.spotify.styx.state.RunState.State.SUBMITTING;
import static com.spotify.styx.state.RunState.State.TERMINATED;
import static com.spotify.styx.testdata.TestData.EXECUTION_DESCRIPTION2;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.spotify.styx.WorkflowInstanceEventFactory;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.Message.MessageLevel;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerUtil;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RunStateTest {

  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-04-04");

  private static final String TEST_ERROR_MESSAGE = "error_message";
  private static final String TEST_EXECUTION_ID_1 = "execution_1";
  private static final String TEST_EXECUTION_ID_2 = "execution_2";

  private static final String DOCKER_IMAGE = "busybox:1.1";
  private static final ExecutionDescription EXECUTION_DESCRIPTION = ExecutionDescription.builder()
      .dockerImage(DOCKER_IMAGE)
      .dockerArgs("--date", "{}", "--bar")
      .build();

  private static final Trigger UNKNOWN_TRIGGER = Trigger.unknown("trig");
  private static final Trigger NATURAL_TRIGGER1 = Trigger.natural();

  private WorkflowInstanceEventFactory eventFactory =
      new WorkflowInstanceEventFactory(WORKFLOW_INSTANCE);

  private List<RunState.State> outputs;
  private StateTransitioner transitioner;
  @Mock private Time time;
  @Mock private EventRouter eventRouter;

  static class StateTransitioner {

    private final Time time;
    private final OutputHandler outputHandler;
    private final Map<WorkflowInstance, RunState> states = Maps.newHashMap();
    private final EventRouter eventRouter;

    StateTransitioner(Time time, OutputHandler outputHandler, EventRouter eventRouter) {
      this.time = Objects.requireNonNull(time);
      this.outputHandler = Objects.requireNonNull(outputHandler);
      this.eventRouter = Objects.requireNonNull(eventRouter);
    }

    void initialize(RunState runState) {
      states.put(runState.workflowInstance(), runState);
    }

    void receive(Event event) {
      WorkflowInstance key = event.workflowInstance();
      RunState currentState = states.get(key);

      RunState nextState = currentState.transition(event, time);
      states.put(key, nextState);

      outputHandler.transitionInto(nextState, eventRouter);
    }

    public RunState get(WorkflowInstance workflowInstance) {
      return states.get(workflowInstance);
    }
  }

  @Before
  public void setUp() {
    outputs = new ArrayList<>();
    transitioner = new StateTransitioner(time, this::record, eventRouter);
    when(time.get()).thenReturn(Instant.now());
  }

  private void record(RunState state, EventRouter eventRouter) {
    outputs.add(state.state());
  }

  @Test
  public void testTransitionUpdates() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(Trigger.natural()));

    verify(time).get();
    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).counter(), is(0L));
  }

  @Test
  public void testTriggerAndRetryAfter() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, "exec1"));
    transitioner.receive(eventFactory.submitted("exec1"));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(777));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), isPresentAndIs(777L));

    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, "exec2"));
    transitioner.receive(eventFactory.submitted("exec2"));
    transitioner.receive(eventFactory.started());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RUNNING));
  }

  @Test
  public void testRunErrorOnCreating() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, "exec1"));
    transitioner.receive(eventFactory.submitted("exec1"));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(777));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), isPresentAndIs(777L));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(1));

    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.retryAfter(999));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), isPresentAndIs(999L));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(2));
  }

  @Test
  public void testSetTrigger() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(NATURAL_TRIGGER1));

    assertThat(
        transitioner.get(WORKFLOW_INSTANCE).data().triggerId(),
        isPresentAndIs(TriggerUtil.NATURAL_TRIGGER_ID));
    assertThat(
        transitioner.get(WORKFLOW_INSTANCE).data().trigger(),
        isPresentAndIs(NATURAL_TRIGGER1));
  }

  @Test
  public void testSetExecutionId() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RUNNING));
    assertThat(
        transitioner.get(WORKFLOW_INSTANCE).data().executionId(),
        equalTo(Optional.of(TEST_EXECUTION_ID_1)));

    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(999));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_2));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_2));
    transitioner.receive(eventFactory.started());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RUNNING));
    assertThat(
        transitioner.get(WORKFLOW_INSTANCE).data().executionId(),
        equalTo(Optional.of(TEST_EXECUTION_ID_2)));
    assertThat(outputs, contains(QUEUED, PREPARE, SUBMITTING, SUBMITTED, RUNNING, TERMINATED, QUEUED,
                                 PREPARE, SUBMITTING, SUBMITTED, RUNNING));
  }

  @Test
  public void testSubmitSetsExecutionId() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(SUBMITTING));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().executionId().orElseThrow(), equalTo(TEST_EXECUTION_ID_1));

    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(SUBMITTED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().executionId().orElseThrow(), equalTo(TEST_EXECUTION_ID_1));

    transitioner.receive(eventFactory.started());
    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RUNNING));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().executionId().orElseThrow(), equalTo(TEST_EXECUTION_ID_1));

    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(999));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_2));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().executionId().orElseThrow(), equalTo(TEST_EXECUTION_ID_2));
  }

  @Test
  public void testSetsRetryDelay() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.retryAfter(777));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), isPresentAndIs(777L));

    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(999));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), isPresentAndIs(999L));
    assertThat(outputs, contains(QUEUED, PREPARE, FAILED, QUEUED,
        PREPARE, SUBMITTING, SUBMITTED, RUNNING, TERMINATED, QUEUED));
  }

  @Test
  public void testRetryDelayFromQueued() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.retryAfter(777));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), isPresentAndIs(777L));

    transitioner.receive(eventFactory.retryAfter(0));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), isPresentAndIs(0L));

    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));

    assertThat(outputs, contains(QUEUED, PREPARE, FAILED, QUEUED, QUEUED, PREPARE));
  }

  @Test
  public void testRetryAfterFromRunError() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.retryAfter(0));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(1));
    assertThat(outputs, contains(QUEUED, PREPARE, SUBMITTING, SUBMITTED, FAILED, QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(1));
  }

  @Test
  public void testManyRetriesAfterFromRunError() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.retryAfter(0));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.retryAfter(0));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(2));
    assertThat(outputs, contains(QUEUED, PREPARE, SUBMITTING, SUBMITTED, RUNNING, FAILED, QUEUED, PREPARE,
        SUBMITTING, SUBMITTED, RUNNING, FAILED, QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(2));
  }

  @Test
  public void testMissingDependenciesAddsToCost() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(RunState.MISSING_DEPS_EXIT_CODE));
    transitioner.receive(eventFactory.retryAfter(0));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_2));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_2));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(RunState.MISSING_DEPS_EXIT_CODE));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(TERMINATED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryCost(), equalTo(0.2));
    assertThat(outputs, contains(QUEUED, PREPARE, SUBMITTING, SUBMITTED, RUNNING, TERMINATED, QUEUED, PREPARE,
        SUBMITTING, SUBMITTED, RUNNING, TERMINATED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(0));
  }

  @Test
  public void testMissingDependenciesIncrementsTries() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(RunState.MISSING_DEPS_EXIT_CODE));
    transitioner.receive(eventFactory.retryAfter(0));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_2));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_2));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(RunState.MISSING_DEPS_EXIT_CODE));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(TERMINATED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(2));
    assertThat(outputs, contains(QUEUED, PREPARE, SUBMITTING, SUBMITTED, RUNNING, TERMINATED, QUEUED, PREPARE,
                                 SUBMITTING, SUBMITTED, RUNNING, TERMINATED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(0));
  }

  @Test
  public void testErrorsAddsToCost() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(0));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_2));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_2));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(TERMINATED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryCost(), equalTo(2.0));
    assertThat(outputs, contains(QUEUED, PREPARE, SUBMITTING, SUBMITTED, RUNNING, TERMINATED, QUEUED, PREPARE,
                                 SUBMITTING, SUBMITTED, RUNNING, TERMINATED));
  }

  @Test
  public void testFatalFromRunError() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.stop());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(ERROR));
    assertThat(outputs, contains(QUEUED, PREPARE, FAILED, ERROR));
  }

  @Test
  public void testSuccessFromTerminated() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(0));
    transitioner.receive(eventFactory.success());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(DONE));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(1));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().lastExit(), isPresentAndIs(0));
    assertThat(outputs, contains(QUEUED, PREPARE, SUBMITTING, SUBMITTED, RUNNING, TERMINATED, DONE));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(0));
  }

  @Test
  public void testRetryAfterFromTerminated() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(0));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(1));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().lastExit(), isPresentAndIs(1));
    assertThat(outputs, contains(QUEUED, PREPARE, SUBMITTING, SUBMITTED, RUNNING, TERMINATED, QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(1));
  }

  @Test
  public void testManyRetriesAfterFromTerminated() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(0));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_2));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_2));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(7));
    transitioner.receive(eventFactory.retryAfter(0));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(2));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().lastExit(), isPresentAndIs(7));
    assertThat(outputs, contains(QUEUED, PREPARE, SUBMITTING, SUBMITTED, RUNNING, TERMINATED, QUEUED, PREPARE,
                                 SUBMITTING, SUBMITTED, RUNNING, TERMINATED, QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(2));
  }

  @Test
  public void testFatalFromTerminated() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.stop());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(ERROR));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().lastExit(), isPresentAndIs(1));
    assertThat(outputs, contains(QUEUED, PREPARE, SUBMITTING, SUBMITTED, RUNNING, TERMINATED, ERROR));
  }

  @Test
  public void testRetryAfterFromStartedThenRunError() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.retryAfter(0));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(1));
    assertThat(outputs, contains(QUEUED, PREPARE, SUBMITTING, SUBMITTED, RUNNING, FAILED, QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(1));
  }

  @Test
  public void testFatalFromStartedThenRunError() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.stop());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(ERROR));
    assertThat(outputs, contains(QUEUED, PREPARE, SUBMITTING, SUBMITTED, RUNNING, FAILED, ERROR));
  }

  @Test
  public void testFailedFromTimeout() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.timeout());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(FAILED));
    assertThat(outputs, contains(QUEUED, PREPARE, SUBMITTING, SUBMITTED, RUNNING, FAILED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(0));
  }

  @Test
  public void testRetriggerOfPartition() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.stop());
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_2));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_2));
    transitioner.receive(eventFactory.started());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RUNNING));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(1));
    assertThat(outputs, contains(QUEUED, PREPARE, SUBMITTING, SUBMITTED, RUNNING, FAILED, ERROR,
                                 QUEUED, PREPARE, SUBMITTING, SUBMITTED, RUNNING));
  }

  @Test
  public void testRunErrorEmitsMessage() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(20));
    transitioner.receive(eventFactory.retryAfter(0));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.runError("Error"));

    final Message expectedMessage = Message.create(MessageLevel.ERROR, "Error");
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().messages(), contains(expectedMessage));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().message().orElseThrow(), is(expectedMessage));
  }

  @Test
  public void testKeepsLastMessage() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.info(Message.info("info message")));
    transitioner.receive(eventFactory.info(Message.warning("warning message")));

    final Message expectedMessage = Message.warning("warning message");
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().messages(), contains(expectedMessage));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().message().orElseThrow(), is(expectedMessage));
  }

  @Test
  public void testInfoTransitionsToSameState() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.info(Message.info("hello")));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(0));
    assertThat(outputs, contains(QUEUED, QUEUED));
  }

  @Test
  public void testRunErrorFromQueuedState() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.runError("Unknown resources"));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(FAILED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(0));
    assertThat(outputs, contains(QUEUED, FAILED));
  }

  @Test
  public void testStoresExecutedDockerImage() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));

    assertThat(
        transitioner.get(WORKFLOW_INSTANCE).data().executionDescription().orElseThrow().dockerImage(),
        equalTo(DOCKER_IMAGE));
  }

  @Test
  public void testStoresLastExecutedDockerImage() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(0));
    transitioner.receive(eventFactory.dequeue(ImmutableSet.of()));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION2, TEST_EXECUTION_ID_2));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_2));

    assertThat(
        transitioner.get(WORKFLOW_INSTANCE).data().executionDescription().orElseThrow().dockerImage(),
        equalTo(EXECUTION_DESCRIPTION2.dockerImage()));
  }

  @Test
  public void testStoresResourcesFromDequeueThroughRunError() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().resourceIds(), isEmpty());

    transitioner.receive(eventFactory.dequeue(ImmutableSet.of("r1")));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, "exec1"));
    transitioner.receive(eventFactory.submitted("exec1"));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(FAILED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().resourceIds(), isPresentAnd(contains("r1")));

    transitioner.receive(eventFactory.retryAfter(12));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().resourceIds(), isEmpty());

    transitioner.receive(eventFactory.dequeue(ImmutableSet.of("r2")));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().resourceIds(), isPresentAnd(contains("r2")));
  }

  @Test
  public void testStoresResourcesFromDequeueThroughTerminate() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().resourceIds(), isEmpty());

    transitioner.receive(eventFactory.dequeue(ImmutableSet.of("r1")));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, "exec1"));
    transitioner.receive(eventFactory.submitted("exec1"));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(RunState.MISSING_DEPS_EXIT_CODE));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(TERMINATED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().resourceIds(), isPresentAnd(contains("r1")));
  }

  @Test
  public void testStoresResourcesFromDequeueThroughTimeout() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().resourceIds(), isEmpty());

    transitioner.receive(eventFactory.dequeue(ImmutableSet.of("r1")));
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, "exec1"));
    transitioner.receive(eventFactory.timeout());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(FAILED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().resourceIds(), isPresentAnd(contains("r1")));
  }

  @Test
  public void testStoresNoResourcesWhenNotDequeued() {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().resourceIds(), isEmpty());

    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(FAILED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().resourceIds(), isEmpty());
  }
}
