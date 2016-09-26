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
package com.spotify.styx.state.handlers;

import com.google.common.collect.Lists;

import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.testdata.TestData;

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.spotify.styx.state.RunState.State.AWAITING_RETRY;
import static com.spotify.styx.state.RunState.State.DONE;
import static com.spotify.styx.state.RunState.State.ERROR;
import static com.spotify.styx.state.RunState.State.FAILED;
import static com.spotify.styx.state.RunState.State.TERMINATED;
import static com.spotify.styx.state.handlers.TerminationHandler.MAX_RETRY_COST;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

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
    outputHandler = new TerminationHandler(BASE_DELAY, MAX_EXPONENT, stateManager);
  }

  @Test
  public void shouldCompleteOnZeroExitCode() throws Exception {
    RunState zeroTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, 1, 1.0, 0, transitions::add);
    stateManager.initialize(zeroTerm);
    outputHandler.transitionInto(zeroTerm);

    RunState nextState = transitions.get(0);
    assertThat(nextState.state(), is(DONE));
  }

  @Test
  public void shouldScheduleRetryOnNonZero() throws Exception {
    RunState nonZeroTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, 1, 1.0, 1, transitions::add);
    stateManager.initialize(nonZeroTerm);
    outputHandler.transitionInto(nonZeroTerm);

    RunState nextState = transitions.get(0);
    assertThat(nextState.state(), is(AWAITING_RETRY));
  }

  @Test
  public void shouldScheduleRetryOnFail() throws Exception {
    RunState failed = RunState.create(WORKFLOW_INSTANCE, FAILED, 1, 1.0, 1, transitions::add);
    stateManager.initialize(failed);
    outputHandler.transitionInto(failed);

    RunState nextState = transitions.get(0);
    assertThat(nextState.state(), is(AWAITING_RETRY));
  }

  @Test
  public void shouldScheduleRetryWithBackoff() throws Exception {
    List<Long> delays = new ArrayList<>();
    int runs = 10000;
    for (int i = 0; i < runs; i++) {
      RunState tenthTry = RunState.create(WORKFLOW_INSTANCE, TERMINATED, MAX_EXPONENT, 1);
      stateManager.initialize(tenthTry);
      outputHandler.transitionInto(tenthTry);
      delays.add(stateManager.get(WORKFLOW_INSTANCE).retryDelayMillis());
    }

    double average = delays.stream()
        .mapToLong(i -> i)
        .average()
        .getAsDouble();

    double expected = BASE_DELAY.toMillis() * (1 << (MAX_EXPONENT - 1));
    double diff = Math.abs(expected - average);

    assertThat(diff, lessThan(expected * 0.05));
  }

  @Test
  public void shouldFailOnNonZeroMaxRetriesReached() throws Exception {
    RunState maxedTerm = RunState.create(WORKFLOW_INSTANCE, TERMINATED, 400,
                                         MAX_RETRY_COST, 1, transitions::add);
    stateManager.initialize(maxedTerm);
    outputHandler.transitionInto(maxedTerm);

    RunState nextState = transitions.get(0);
    assertThat(nextState.state(), is(ERROR));
  }

  @Test
  public void shouldFailOnFailMaxRetriesReached() throws Exception {
    RunState maxedTerm = RunState.create(WORKFLOW_INSTANCE, FAILED, 400,
                                         MAX_RETRY_COST, 1, transitions::add);
    stateManager.initialize(maxedTerm);
    outputHandler.transitionInto(maxedTerm);

    RunState nextState = transitions.get(0);
    assertThat(nextState.state(), is(ERROR));
  }

  @Test
  public void shouldScheduleRetryOf10MinutesOnMissingDependencies() throws Exception {
    RunState missingDeps = RunState.create(WORKFLOW_INSTANCE, TERMINATED, 1, 1.0, 20, transitions::add);
    stateManager.initialize(missingDeps);
    outputHandler.transitionInto(missingDeps);

    RunState nextState = transitions.get(0);

    assertThat(nextState.state(), is(AWAITING_RETRY));
    assertThat(nextState.retryDelayMillis(), is(Duration.ofMinutes(10).toMillis()));
  }
}
