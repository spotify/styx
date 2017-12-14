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

import static com.github.npathai.hamcrestopt.OptionalMatchers.hasValue;
import static com.spotify.styx.state.RunState.State.DONE;
import static com.spotify.styx.state.RunState.State.ERROR;
import static com.spotify.styx.state.RunState.State.FAILED;
import static com.spotify.styx.state.RunState.State.PREPARE;
import static com.spotify.styx.state.RunState.State.QUEUED;
import static com.spotify.styx.state.RunState.State.RUNNING;
import static com.spotify.styx.state.RunState.State.SUBMITTED;
import static com.spotify.styx.state.RunState.State.SUBMITTING;
import static com.spotify.styx.state.RunState.State.TERMINATED;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import com.spotify.styx.WorkflowInstanceEventFactory;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.TriggerUtil;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

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

  List<RunState.State> outputs = new LinkedList<>();
  StateTransitioner transitioner = new StateTransitioner();

  private void record(RunState state) {
    outputs.add(state.state());
  }

  @Test // for backwards compatibility
  public void testTimeTriggerAndRetry() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.timeTrigger());
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(777));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), hasValue(777L));

    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.started());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RUNNING));
  }

  @Test
  public void testTimeTriggerAndRetry2() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue());
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, "exec1"));
    transitioner.receive(eventFactory.submitted("exec1"));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(777));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), hasValue(777L));

    transitioner.receive(eventFactory.dequeue());
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, "exec2"));
    transitioner.receive(eventFactory.submitted("exec2"));
    transitioner.receive(eventFactory.started());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RUNNING));
  }

  @Test
  public void testRunErrorOnCreating() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.timeTrigger());
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(777));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), hasValue(777L));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(1));

    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.retryAfter(999));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), hasValue(999L));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(2));
  }

  @Test
  public void testSetTrigger() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(NATURAL_TRIGGER1));

    assertThat(
        transitioner.get(WORKFLOW_INSTANCE).data().triggerId(),
        hasValue(TriggerUtil.NATURAL_TRIGGER_ID));
    assertThat(
        transitioner.get(WORKFLOW_INSTANCE).data().trigger(),
        hasValue(NATURAL_TRIGGER1));
  }

  @Test
  public void testSetExecutionId() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RUNNING));
    assertThat(
        transitioner.get(WORKFLOW_INSTANCE).data().executionId(),
        equalTo(Optional.of(TEST_EXECUTION_ID_1)));

    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(999));
    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_2, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RUNNING));
    assertThat(
        transitioner.get(WORKFLOW_INSTANCE).data().executionId(),
        equalTo(Optional.of(TEST_EXECUTION_ID_2)));
    assertThat(outputs, contains(QUEUED, SUBMITTED, RUNNING, TERMINATED, QUEUED,
                                 PREPARE, SUBMITTED, RUNNING));
  }

  @Test
  public void testSubmitSetsExecutionId() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue());
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(SUBMITTING));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().executionId().get(), equalTo(TEST_EXECUTION_ID_1));

    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(SUBMITTED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().executionId().get(), equalTo(TEST_EXECUTION_ID_1));

    transitioner.receive(eventFactory.started());
    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RUNNING));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().executionId().get(), equalTo(TEST_EXECUTION_ID_1));

    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(999));
    transitioner.receive(eventFactory.dequeue());
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_2));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().executionId().get(), equalTo(TEST_EXECUTION_ID_2));
  }

  @Test
  public void testSetsRetryDelay() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.retryAfter(777));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), hasValue(777L));

    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(999));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), hasValue(999L));
    assertThat(outputs, contains(QUEUED, SUBMITTED, FAILED, QUEUED, PREPARE, SUBMITTED,
                                 RUNNING, TERMINATED, QUEUED));
  }

  @Test
  public void testRetryDelayFromQueued() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue());
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.retryAfter(777));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), hasValue(777L));

    transitioner.receive(eventFactory.retryAfter(0));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryDelayMillis(), hasValue(0L));

    transitioner.receive(eventFactory.dequeue());

    assertThat(outputs, contains(QUEUED, PREPARE, FAILED, QUEUED, QUEUED, PREPARE));
  }

  @Test
  public void testRetryFromRunError() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));


    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(SUBMITTED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(2));
    assertThat(outputs, contains(QUEUED, SUBMITTED, FAILED, PREPARE, SUBMITTED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(1));
  }

  @Test
  public void testManyRetriesFromRunError() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(SUBMITTED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(3));
    assertThat(outputs, contains(QUEUED, SUBMITTED, FAILED, PREPARE, SUBMITTED, FAILED, PREPARE,
                                 SUBMITTED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(2));
  }

  @Test
  public void testMissingDependenciesAddsToCost() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(RunState.MISSING_DEPS_EXIT_CODE));
    transitioner.receive(eventFactory.retryAfter(0));
    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(RunState.MISSING_DEPS_EXIT_CODE));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(TERMINATED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryCost(), equalTo(0.2));
    assertThat(outputs, contains(QUEUED, SUBMITTED, RUNNING, TERMINATED, QUEUED, PREPARE,
                                 SUBMITTED, RUNNING, TERMINATED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(0));
  }

  @Test
  public void testMissingDependenciesIncrementsTries() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(RunState.MISSING_DEPS_EXIT_CODE));
    transitioner.receive(eventFactory.retryAfter(0));
    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(RunState.MISSING_DEPS_EXIT_CODE));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(TERMINATED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(2));
    assertThat(outputs, contains(QUEUED, SUBMITTED, RUNNING, TERMINATED, QUEUED, PREPARE,
                                 SUBMITTED, RUNNING, TERMINATED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(0));
  }

  @Test
  public void testErrorsAddsToCost() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retryAfter(0));
    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(TERMINATED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().retryCost(), equalTo(2.0));
    assertThat(outputs, contains(QUEUED, SUBMITTED, RUNNING, TERMINATED, QUEUED, PREPARE,
                                 SUBMITTED, RUNNING, TERMINATED));
  }

  @Test
  public void testFatalFromRunError() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.stop());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RunState.State.ERROR));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(1));
    assertThat(outputs, contains(QUEUED, SUBMITTED, FAILED, ERROR));
  }

  @Test
  public void testSuccessFromTerm() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(0));
    transitioner.receive(eventFactory.success());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RunState.State.DONE));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(1));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().lastExit(), hasValue(0));
    assertThat(outputs, contains(QUEUED, SUBMITTED, RUNNING, TERMINATED, DONE));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(0));
  }

  @Test
  public void testRetryFromTerm() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(SUBMITTED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(2));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().lastExit(), hasValue(1));
    assertThat(outputs, contains(QUEUED, SUBMITTED, RUNNING, TERMINATED, PREPARE, SUBMITTED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(1));
  }

  @Test
  public void testManyRetriesFromTerm() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(7));
    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(SUBMITTED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(3));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().lastExit(), hasValue(7));
    assertThat(outputs, contains(QUEUED, SUBMITTED, RUNNING, TERMINATED, PREPARE, SUBMITTED,
                                 RUNNING, TERMINATED, PREPARE, SUBMITTED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(2));
  }

  @Test
  public void testFatalFromTerm() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.stop());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RunState.State.ERROR));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(1));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().lastExit(), hasValue(1));
    assertThat(outputs, contains(QUEUED, SUBMITTED, RUNNING, TERMINATED, ERROR));
  }

  @Test
  public void testRetryFromStartedThenRunError() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(SUBMITTED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(2));
    assertThat(outputs, contains(QUEUED, SUBMITTED, RUNNING, FAILED, PREPARE, SUBMITTED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(1));
  }

  @Test
  public void testFatalFromStartedThenRunError() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.stop());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RunState.State.ERROR));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(1));
    assertThat(outputs, contains(QUEUED, SUBMITTED, RUNNING, FAILED, ERROR));
  }

  @Test
  public void testFailedFromTimeout() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.timeout());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RunState.State.FAILED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(1));
    assertThat(outputs, contains(QUEUED, SUBMITTED, RUNNING, FAILED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().consecutiveFailures(), equalTo(0));
  }

  @Test
  public void testRetriggerOfPartition() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.runError(TEST_ERROR_MESSAGE));
    transitioner.receive(eventFactory.stop());
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_2, DOCKER_IMAGE));
    transitioner.receive(eventFactory.started());

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(RunState.State.RUNNING));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(1));
    assertThat(outputs, contains(QUEUED, SUBMITTED, RUNNING, FAILED, ERROR,
                                 QUEUED, SUBMITTED, RUNNING));
  }

  @Test
  public void testAccumulatesMessages() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.dequeue());
    transitioner.receive(eventFactory.submit(EXECUTION_DESCRIPTION, TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.submitted(TEST_EXECUTION_ID_1));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(20));
    transitioner.receive(eventFactory.retryAfter(0));
    transitioner.receive(eventFactory.dequeue());
    transitioner.receive(eventFactory.runError("Error"));

    assertThat(
        transitioner.get(WORKFLOW_INSTANCE).data().messages(),
        contains(
            Message.create(Message.MessageLevel.WARNING, "Exit code: 20"),
            Message.create(Message.MessageLevel.ERROR, "Error")
        ));
  }

  @Test
  public void testAccumulatesMessagesWithInfo() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.info(Message.info("info message")));
    transitioner.receive(eventFactory.info(Message.warning("warning message")));

    assertThat(
        transitioner.get(WORKFLOW_INSTANCE).data().messages(),
        contains(
            Message.info("info message"),
            Message.warning("warning message")));
  }

  @Test
  public void testInfoTransitionsToSameState() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.info(Message.info("hello")));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(QUEUED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(0));
    assertThat(outputs, contains(QUEUED, QUEUED));
  }

  @Test
  public void testRunErrorFromQueuedState() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.runError("Unknown resources"));

    assertThat(transitioner.get(WORKFLOW_INSTANCE).state(), equalTo(FAILED));
    assertThat(transitioner.get(WORKFLOW_INSTANCE).data().tries(), equalTo(0));
    assertThat(outputs, contains(QUEUED, FAILED));
  }

  @Test
  public void testStoresExecutedDockerImage() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE + "1"));

    assertThat(
        transitioner.get(WORKFLOW_INSTANCE).data().executionDescription().get().dockerImage(),
        equalTo(DOCKER_IMAGE + "1"));
  }

  @Test
  public void testStoresLastExecutedDockerImage() throws Exception {
    transitioner.initialize(RunState.fresh(WORKFLOW_INSTANCE, this::record));
    transitioner.receive(eventFactory.triggerExecution(UNKNOWN_TRIGGER));
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE + "1"));
    transitioner.receive(eventFactory.started());
    transitioner.receive(eventFactory.terminate(1));
    transitioner.receive(eventFactory.retry());
    transitioner.receive(eventFactory.created(TEST_EXECUTION_ID_1, DOCKER_IMAGE + "2"));

    assertThat(
        transitioner.get(WORKFLOW_INSTANCE).data().executionDescription().get().dockerImage(),
        equalTo(DOCKER_IMAGE + "2"));
  }
}
