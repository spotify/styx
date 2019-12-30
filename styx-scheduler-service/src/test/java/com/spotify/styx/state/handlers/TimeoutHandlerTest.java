/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

import static com.spotify.styx.state.TimeoutConfig.createWithDefaultTtl;
import static com.spotify.styx.testdata.TestData.WORKFLOW_ID;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static com.spotify.styx.testdata.TestData.WORKFLOW_WITH_RESOURCES;
import static java.time.Duration.ofSeconds;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.TimeoutConfig;
import com.spotify.styx.util.Time;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.function.Supplier;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class TimeoutHandlerTest {

  private Instant now = Instant.parse("2016-12-02T22:00:00Z");
  private Time time = () -> now;
  private long counter = 17;

  @Mock private EventRouter eventRouter;
  @Mock private Supplier<Map<WorkflowId, Workflow>> workflows;

  private TimeoutHandler timeoutHandler;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(workflows.get()).thenReturn(Map.of(WORKFLOW_ID, WORKFLOW_WITH_RESOURCES));
  }

  private void setUpWithTimeoutSeconds(int timeoutSeconds) {
    TimeoutConfig timeoutConfig = createWithDefaultTtl(ofSeconds(timeoutSeconds));
    timeoutHandler = new TimeoutHandler(timeoutConfig, time, workflows);
  }

  @Parameters(source = State.class)
  @Test
  public void shouldTimeoutActiveState(State state) {
    assumeFalse(state.isTerminal());
    setUpWithTimeoutSeconds(5);
    var runState = RunState.create(WORKFLOW_INSTANCE, state, now, counter);
    now = now.plus(5, ChronoUnit.SECONDS);
    timeoutHandler.transitionInto(runState, eventRouter);
    verify(eventRouter).receiveIgnoreClosed(Event.timeout(WORKFLOW_INSTANCE), counter);
  }

  @Parameters(source = State.class)
  @Test
  public void shouldNotTimeoutTerminalState(State state) {
    assumeTrue(state.isTerminal());
    setUpWithTimeoutSeconds(0);
    now = now.plus(5, ChronoUnit.SECONDS);
    var runState = RunState.create(WORKFLOW_INSTANCE, state, now, counter);
    timeoutHandler.transitionInto(runState, eventRouter);
    verify(eventRouter, never()).receiveIgnoreClosed(any());
  }

  @Parameters(source = State.class)
  @Test
  public void shouldNotTransitionIfNotTimedOut(State state) {
    setUpWithTimeoutSeconds(20);
    var runState = RunState.create(WORKFLOW_INSTANCE, State.RUNNING, now, counter);
    timeoutHandler.transitionInto(runState, eventRouter);
    verify(eventRouter, never()).receiveIgnoreClosed(any());
  }
}
