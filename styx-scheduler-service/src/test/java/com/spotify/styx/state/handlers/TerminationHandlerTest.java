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

package com.spotify.styx.state.handlers;

import static com.spotify.styx.model.Schedule.HOURS;
import static com.spotify.styx.state.RunState.State.FAILED;
import static com.spotify.styx.state.RunState.State.TERMINATED;
import static com.spotify.styx.state.handlers.TerminationHandler.MAX_RETRY_COST;
import static com.spotify.styx.testdata.TestData.HOURLY_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.VALID_SHA;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.MoreExecutors;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.RetryUtil;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.IntStream;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TerminationHandlerTest {

  private static final Instant NOW = Instant.now();
  private static final long COUNTER = 17;

  @Mock Storage storage;
  @Mock StateManager stateManager;
  @Mock RetryUtil retryUtil;

  private TerminationHandler terminationHandler;

  private static final Workflow WORKFLOW =
      Workflow.create(TestData.WORKFLOW_ID.componentId(), HOURLY_WORKFLOW_CONFIGURATION);
  private static final Workflow WORKFLOW_WITH_RETRY_CONDITION =
      Workflow.create(TestData.WORKFLOW_ID.componentId(), WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .commitSha(VALID_SHA)
          .dockerImage("busybox")
          .schedule(HOURS)
          .retryCondition("exitCode == 1")
          .build());
  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-04-04");

  @Before
  public void setUp() throws IOException {
    when(storage.workflow(WORKFLOW_INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW));
    terminationHandler = new TerminationHandler(retryUtil, storage, stateManager);
  }

  @Test
  public void shouldCompleteOnZeroExitCode() {
    StateData data = data(1, 1.0, Optional.of(0));
    RunState zeroTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, NOW, COUNTER);
    terminationHandler.transitionInto(zeroTerm);
    verify(stateManager).receiveIgnoreClosed(Event.success(WORKFLOW_INSTANCE), COUNTER);
  }

  @Test
  public void shouldScheduleRetryOnNonZero() {
    StateData data = data(1, 1.0, Optional.of(1));
    RunState nonZeroTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, NOW, COUNTER);
    Duration expectedDelay = Duration.ofMillis(4711);
    when(retryUtil.calculateDelay(anyInt())).thenReturn(expectedDelay);
    terminationHandler.transitionInto(nonZeroTerm);
    verify(stateManager).receiveIgnoreClosed(Event.retryAfter(WORKFLOW_INSTANCE, expectedDelay.toMillis()), COUNTER);
  }

  @Test
  public void shouldScheduleRetryOnMissingExitCode() {
    StateData data = data(1, 1.0, Optional.empty());
    RunState nonZeroTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, NOW, COUNTER);
    Duration expectedDelay = Duration.ofMillis(4711);
    when(retryUtil.calculateDelay(anyInt())).thenReturn(expectedDelay);
    terminationHandler.transitionInto(nonZeroTerm);
    verify(stateManager).receiveIgnoreClosed(Event.retryAfter(WORKFLOW_INSTANCE, expectedDelay.toMillis()), COUNTER);
  }

  @Test
  public void shouldScheduleRetryOnFail() {
    StateData data = data(1, 1.0, Optional.of(1));
    RunState failed = RunState.create(WORKFLOW_INSTANCE, FAILED, data, NOW, COUNTER);
    Duration expectedDelay = Duration.ofMillis(4711);
    when(retryUtil.calculateDelay(anyInt())).thenReturn(expectedDelay);
    terminationHandler.transitionInto(failed);
    verify(stateManager).receiveIgnoreClosed(Event.retryAfter(WORKFLOW_INSTANCE, expectedDelay.toMillis()), COUNTER);
  }

  @Test
  public void shouldStopOnNonZeroMaxRetriesReached() {
    StateData data = data(400, MAX_RETRY_COST, Optional.of(1));
    RunState maxedTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, NOW, COUNTER);
    terminationHandler.transitionInto(maxedTerm);
    verify(stateManager).receiveIgnoreClosed(Event.stop(WORKFLOW_INSTANCE), COUNTER);
  }

  @Test
  public void shouldStopOnFailMaxRetriesReached() {
    StateData data = data(400, MAX_RETRY_COST, Optional.of(1));
    RunState maxedTerm = RunState.create(WORKFLOW_INSTANCE, FAILED, data, NOW, COUNTER);
    terminationHandler.transitionInto(maxedTerm);
    verify(stateManager).receiveIgnoreClosed(Event.stop(WORKFLOW_INSTANCE), COUNTER);
  }

  @Test
  public void shouldScheduleRetryOf10MinutesOnMissingDependencies() {
    StateData data = data(1, 1.0, Optional.of(20));
    RunState missingDeps = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, NOW, COUNTER);
    terminationHandler.transitionInto(missingDeps);
    verify(stateManager).receiveIgnoreClosed(
        Event.retryAfter(WORKFLOW_INSTANCE, Duration.ofMinutes(10).toMillis()), COUNTER);
  }

  @Test
  public void shouldStopOnFailFastExitCodeReceived() {
    var data = data(1, 1.0, Optional.of(50));
    var failed = RunState.create(WORKFLOW_INSTANCE, FAILED, data, NOW, COUNTER);
    terminationHandler.transitionInto(failed);
    verify(stateManager).receiveIgnoreClosed(Event.stop(WORKFLOW_INSTANCE), COUNTER);
  }

  @Test
  public void shouldStopOnWorkflowNotFound() throws IOException {
    var data = data(1, 1.0, Optional.empty());
    var failed = RunState.create(WORKFLOW_INSTANCE, FAILED, data, NOW, COUNTER);
    when(storage.workflow(WORKFLOW_INSTANCE.workflowId())).thenReturn(Optional.empty());
    terminationHandler.transitionInto(failed);
    verify(stateManager).receiveIgnoreClosed(Event.stop(WORKFLOW_INSTANCE), COUNTER);
  }

  @Test
  public void shouldScheduleRetryOnReadingWorkflowFailed() throws IOException {
    var data = data(1, 1.0, Optional.of(1));
    var failed = RunState.create(WORKFLOW_INSTANCE, FAILED, data, NOW, COUNTER);
    Duration expectedDelay = Duration.ofMillis(4711);
    when(retryUtil.calculateDelay(anyInt())).thenReturn(expectedDelay);
    when(storage.workflow(WORKFLOW_INSTANCE.workflowId())).thenThrow(new IOException());
    terminationHandler.transitionInto(failed);
    verify(stateManager).receiveIgnoreClosed(Event.retryAfter(WORKFLOW_INSTANCE, expectedDelay.toMillis()), COUNTER);
  }

  @Test
  public void shouldScheduleRetryOnRetryConditionMet() throws IOException {
    var data = data(1, 1.0, Optional.of(1));
    var nonZeroTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, NOW, COUNTER);
    Duration expectedDelay = Duration.ofMillis(4711);
    when(retryUtil.calculateDelay(anyInt())).thenReturn(expectedDelay);
    when(storage.workflow(WORKFLOW_INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW_WITH_RETRY_CONDITION));
    terminationHandler.transitionInto(nonZeroTerm);
    verify(stateManager).receiveIgnoreClosed(Event.retryAfter(WORKFLOW_INSTANCE, expectedDelay.toMillis()), COUNTER);
  }

  @Test
  public void shouldStopOnRetryConditionNotMet() throws IOException {
    var data = data(1, 1.0, Optional.of(2));
    var nonZeroTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, NOW, COUNTER);
    when(storage.workflow(WORKFLOW_INSTANCE.workflowId())).thenReturn(Optional.of(WORKFLOW_WITH_RETRY_CONDITION));
    terminationHandler.transitionInto(nonZeroTerm);
    verify(stateManager).receiveIgnoreClosed(Event.stop(WORKFLOW_INSTANCE), COUNTER);
  }

  @Test
  public void shouldEvaluateToTrue() {
    var executorService = new ForkJoinPool(32);
    var results = IntStream.range(0, 100)
        .mapToObj(i -> CompletableFuture.supplyAsync(() -> terminationHandler.retryConditionMet(
            RunState.create(WORKFLOW_INSTANCE, FAILED,
                StateData.newBuilder()
                    .tries(3)
                    .consecutiveFailures(2)
                    .trigger(Trigger.natural())
                    .build()),
            Optional.of(1),
            "exitCode == 1 && (tries < 3 || consecutiveFailures < 4) && triggerType == \"natural\""), executorService))
        .collect(toList())
        .stream()
        .map(CompletableFuture::join)
        .collect(toList());
    results.forEach(result -> assertThat(result, is(true)));
  }

  @Test
  public void shouldFailToEvaluate() {
    var result = terminationHandler.retryConditionMet(
        RunState.create(WORKFLOW_INSTANCE, FAILED,
            StateData.zero()),
        Optional.of(1),
        "foo -> bar");
    assertThat(result, is(false));
  }

  @Test
  public void shouldFailToEvaluateDueToTimeout() {
    var result = terminationHandler.retryConditionMet(
        RunState.create(WORKFLOW_INSTANCE, FAILED,
            StateData.zero()),
        Optional.of(1),
        "Thread.sleep(2 * 1000); true");
    assertThat(result, is(false));
  }

  @Test(expected = InterruptedException.class)
  public void shouldFailToEvaluateDueToThreadInterrupted() throws InterruptedException {
    var terminationHandler = new TerminationHandler(retryUtil, storage, stateManager,
        MoreExecutors.newDirectExecutorService());
    Thread.currentThread().interrupt();
    var result = terminationHandler.retryConditionMet(
        RunState.create(WORKFLOW_INSTANCE, FAILED,
            StateData.zero()),
        Optional.of(1),
        "true");
    assertThat(result, is(false));
    assertThat(Thread.currentThread().isInterrupted(), is(true));
    Thread.sleep(5000);
  }

  @Test
  //@Ignore("require enabling security manager: -Djava.security.manager -Djava.security.policy=security.policy")
  public void shouldFailToEvaluateDueToMissingPermission() {
    var result = terminationHandler.retryConditionMet(
        RunState.create(WORKFLOW_INSTANCE, FAILED,
            StateData.zero()),
        Optional.of(1),
        "System.exit(1); true");
    assertThat(result, is(false));
  }

  @Test
  public void shouldEvaluateToFalseWhenMissingExitCode() {
    var result = terminationHandler.retryConditionMet(
        RunState.create(WORKFLOW_INSTANCE, FAILED,
            StateData.zero()),
        Optional.empty(),
        "exitCode == 1");
    assertThat(result, is(false));
  }

  @Test
  public void shouldEvaluateToFalseWhenMissingTriggerType() {
    var result = terminationHandler.retryConditionMet(
        RunState.create(WORKFLOW_INSTANCE, FAILED,
            StateData.newBuilder()
                .tries(3)
                .consecutiveFailures(2)
                .build()),
        Optional.of(1),
        "exitCode == 1 && (tries < 3 || consecutiveFailures < 4) && triggerType == \"natural\"");
    assertThat(result, is(false));
  }

  @Test
  public void shouldEvaluateToFalseIfRetryConditionIsNotBooleanExpression() {
    var result = terminationHandler.retryConditionMet(
        RunState.create(WORKFLOW_INSTANCE, FAILED, StateData.zero()),
        Optional.of(1),
        "21 * 2");
    assertThat(result, is(false));
  }

  private StateData data(int tries, double cost, Optional<Integer> lastExit) {
    return StateData.newBuilder()
        .tries(tries)
        .retryCost(cost)
        .lastExit(lastExit)
        .build();
  }
}
