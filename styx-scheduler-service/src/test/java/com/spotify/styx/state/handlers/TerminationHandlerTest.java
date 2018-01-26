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

import static com.github.npathai.hamcrestopt.OptionalMatchers.hasValue;
import static com.spotify.styx.state.RunState.State.DONE;
import static com.spotify.styx.state.RunState.State.ERROR;
import static com.spotify.styx.state.RunState.State.FAILED;
import static com.spotify.styx.state.RunState.State.QUEUED;
import static com.spotify.styx.state.RunState.State.TERMINATED;
import static com.spotify.styx.state.handlers.TerminationHandler.MAX_RETRY_COST;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.RetryUtil;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

public class TerminationHandlerTest {

  private static final Duration BASE_DELAY = Duration.ofMillis(500);
  private static final int MAX_EXPONENT = 6;

  private List<RunState> transitions = Lists.newArrayList();

  private OutputHandler outputHandler;
  private StateManager stateManager = new SyncStateManager();

  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-04-04");

  @Before
  public void setUp() throws Exception {
    RetryUtil retryUtil = new RetryUtil(BASE_DELAY, MAX_EXPONENT);
    outputHandler = new TerminationHandler(retryUtil, stateManager);
  }

  @Test
  public void shouldCompleteOnZeroExitCode() throws Exception {
    StateData data = data(1, 1.0, Optional.of(0));
    RunState zeroTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, transitions::add);
    stateManager.trigger(zeroTerm, trigger);
    outputHandler.transitionInto(zeroTerm);

    RunState nextState = transitions.get(0);
    assertThat(nextState.state(), is(DONE));
  }

  @Test
  public void shouldScheduleRetryOnNonZero() throws Exception {
    StateData data = data(1, 1.0, Optional.of(1));
    RunState nonZeroTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, transitions::add);
    stateManager.trigger(nonZeroTerm, trigger);
    outputHandler.transitionInto(nonZeroTerm);

    RunState nextState = transitions.get(0);
    assertThat(nextState.state(), is(QUEUED));
  }

  @Test
  public void shouldScheduleRetryOnMissingExitCode() throws Exception {
    StateData data = data(1, 1.0, Optional.empty());
    RunState nonZeroTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, transitions::add);
    stateManager.trigger(nonZeroTerm, trigger);
    outputHandler.transitionInto(nonZeroTerm);

    RunState nextState = transitions.get(0);
    assertThat(nextState.state(), is(QUEUED));
  }

  @Test
  public void shouldScheduleRetryOnFail() throws Exception {
    StateData data = data(1, 1.0, Optional.of(1));
    RunState failed = RunState.create(WORKFLOW_INSTANCE, FAILED, data, transitions::add);
    stateManager.trigger(failed, trigger);
    outputHandler.transitionInto(failed);

    RunState nextState = transitions.get(0);
    assertThat(nextState.state(), is(QUEUED));
  }

  @Test
  public void shouldFailOnNonZeroMaxRetriesReached() throws Exception {
    StateData data = data(400, MAX_RETRY_COST, Optional.of(1));
    RunState maxedTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, transitions::add);
    stateManager.trigger(maxedTerm, trigger);
    outputHandler.transitionInto(maxedTerm);

    RunState nextState = transitions.get(0);
    assertThat(nextState.state(), is(ERROR));
  }

  @Test
  public void shouldFailOnFailMaxRetriesReached() throws Exception {
    StateData data = data(400, MAX_RETRY_COST, Optional.of(1));
    RunState maxedTerm = RunState.create(WORKFLOW_INSTANCE, FAILED, data, transitions::add);
    stateManager.trigger(maxedTerm, trigger);
    outputHandler.transitionInto(maxedTerm);

    RunState nextState = transitions.get(0);
    assertThat(nextState.state(), is(ERROR));
  }

  @Test
  public void shouldScheduleRetryOf10MinutesOnMissingDependencies() throws Exception {
    StateData data = data(1, 1.0, Optional.of(20));
    RunState missingDeps = RunState.create(WORKFLOW_INSTANCE, TERMINATED, data, transitions::add);
    stateManager.trigger(missingDeps, trigger);
    outputHandler.transitionInto(missingDeps);

    RunState nextState = transitions.get(0);

    assertThat(nextState.state(), is(QUEUED));
    assertThat(nextState.data().retryDelayMillis(), hasValue(Duration.ofMinutes(10).toMillis()));
  }

  @Test
  public void shouldFailOnFailFastExitCodeReceived() throws Exception {
    StateData data = data(1, 1.0, Optional.of(50));
    RunState maxedTerm = RunState.create(WORKFLOW_INSTANCE, FAILED, data, transitions::add);
    stateManager.trigger(maxedTerm, trigger);
    outputHandler.transitionInto(maxedTerm);

    RunState nextState = transitions.get(0);
    assertThat(nextState.state(), is(ERROR));
  }

  private StateData data(int tries, double cost, Optional<Integer> lastExit) {
    return StateData.newBuilder()
        .tries(tries)
        .retryCost(cost)
        .lastExit(lastExit)
        .build();
  }
}
