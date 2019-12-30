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

import static com.spotify.styx.state.RunState.State.FAILED;
import static com.spotify.styx.state.RunState.State.TERMINATED;
import static com.spotify.styx.state.handlers.TerminationHandler.MAX_RETRY_COST;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.RetryUtil;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TerminationHandlerTest {

  private static final Instant NOW = Instant.now();
  private static final long COUNTER = 17;

  @Mock EventRouter eventRouter;
  @Mock RetryUtil retryUtil;

  private TerminationHandler terminationHandler;

  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-04-04");

  @Before
  public void setUp() {
    terminationHandler = new TerminationHandler(retryUtil);
  }

  @Test
  public void shouldCompleteOnZeroExitCode() {
    StateData data = data(1, 1.0, Optional.of(0));
    RunState zeroTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, NOW, COUNTER);
    terminationHandler.transitionInto(zeroTerm, eventRouter);
    verify(eventRouter).receiveIgnoreClosed(Event.success(WORKFLOW_INSTANCE), COUNTER);
  }

  @Test
  public void shouldScheduleRetryOnNonZero() {
    StateData data = data(1, 1.0, Optional.of(1));
    RunState nonZeroTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, NOW, COUNTER);
    Duration expectedDelay = Duration.ofMillis(4711);
    when(retryUtil.calculateDelay(anyInt())).thenReturn(expectedDelay);
    terminationHandler.transitionInto(nonZeroTerm, eventRouter);
    verify(eventRouter).receiveIgnoreClosed(Event.retryAfter(WORKFLOW_INSTANCE, expectedDelay.toMillis()), COUNTER);
  }

  @Test
  public void shouldScheduleRetryOnMissingExitCode() {
    StateData data = data(1, 1.0, Optional.empty());
    RunState nonZeroTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, NOW, COUNTER);
    Duration expectedDelay = Duration.ofMillis(4711);
    when(retryUtil.calculateDelay(anyInt())).thenReturn(expectedDelay);
    terminationHandler.transitionInto(nonZeroTerm, eventRouter);
    verify(eventRouter).receiveIgnoreClosed(Event.retryAfter(WORKFLOW_INSTANCE, expectedDelay.toMillis()), COUNTER);
  }

  @Test
  public void shouldScheduleRetryOnFail() {
    StateData data = data(1, 1.0, Optional.of(1));
    RunState failed = RunState.create(WORKFLOW_INSTANCE, FAILED, data, NOW, COUNTER);
    Duration expectedDelay = Duration.ofMillis(4711);
    when(retryUtil.calculateDelay(anyInt())).thenReturn(expectedDelay);
    terminationHandler.transitionInto(failed, eventRouter);
    verify(eventRouter).receiveIgnoreClosed(Event.retryAfter(WORKFLOW_INSTANCE, expectedDelay.toMillis()), COUNTER);
  }

  @Test
  public void shouldStopOnNonZeroMaxRetriesReached() {
    StateData data = data(400, MAX_RETRY_COST, Optional.of(1));
    RunState maxedTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, NOW, COUNTER);
    terminationHandler.transitionInto(maxedTerm, eventRouter);
    verify(eventRouter).receiveIgnoreClosed(Event.stop(WORKFLOW_INSTANCE), COUNTER);
  }

  @Test
  public void shouldStopOnFailMaxRetriesReached() {
    StateData data = data(400, MAX_RETRY_COST, Optional.of(1));
    RunState maxedTerm = RunState.create(WORKFLOW_INSTANCE, FAILED, data, NOW, COUNTER);
    terminationHandler.transitionInto(maxedTerm, eventRouter);
    verify(eventRouter).receiveIgnoreClosed(Event.stop(WORKFLOW_INSTANCE), COUNTER);
  }

  @Test
  public void shouldScheduleRetryOf10MinutesOnMissingDependencies() {
    StateData data = data(1, 1.0, Optional.of(20));
    RunState missingDeps = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, NOW, COUNTER);
    terminationHandler.transitionInto(missingDeps, eventRouter);
    verify(eventRouter).receiveIgnoreClosed(
        Event.retryAfter(WORKFLOW_INSTANCE, Duration.ofMinutes(10).toMillis()), COUNTER);
  }

  @Test
  public void shouldFailOnFailFastExitCodeReceived() {
    StateData data = data(1, 1.0, Optional.of(50));
    RunState maxedTerm = RunState.create(WORKFLOW_INSTANCE, FAILED, data, NOW, COUNTER);
    terminationHandler.transitionInto(maxedTerm, eventRouter);
    verify(eventRouter).receiveIgnoreClosed(Event.stop(WORKFLOW_INSTANCE), COUNTER);
  }

  private StateData data(int tries, double cost, Optional<Integer> lastExit) {
    return StateData.newBuilder()
        .tries(tries)
        .retryCost(cost)
        .lastExit(lastExit)
        .build();
  }
}
