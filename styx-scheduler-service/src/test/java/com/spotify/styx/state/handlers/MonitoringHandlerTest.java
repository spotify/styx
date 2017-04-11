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

import static org.mockito.Mockito.verify;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.MonitoringHandler;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.Time;
import java.time.Instant;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

public class MonitoringHandlerTest {

  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-04-04T08");

  private OutputHandler outputHandler;
  private Time time;
  private Stats stats;
  private StateManager stateManager = new SyncStateManager();
  private Instant i;

  @Before
  public void setUp() throws Exception {
    time = () -> i;
    stats = Mockito.mock(Stats.class);
    i = Instant.parse("2015-12-31T23:59:10.000Z");
    outputHandler = new MonitoringHandler(time, stats);
  }

  @Test
  public void shouldUpdateOnDefaultCase() throws Exception {
    RunState state = RunState.create(WORKFLOW_INSTANCE, RunState.State.NEW, time, outputHandler);
    stateManager.initialize(state);

    stateManager.receive(Event.timeTrigger(state.workflowInstance()));

    i = Instant.parse("2015-12-31T23:59:12.000Z");
    stateManager.receive(Event.started(state.workflowInstance()));

    verify(stats).submitToRunningTime(Matchers.eq(2L));
  }

  @Test
  public void shouldMarkExitCode() throws Exception {
    RunState state = RunState.create(WORKFLOW_INSTANCE, RunState.State.NEW, time, outputHandler);
    stateManager.initialize(state);

    stateManager.receive(Event.triggerExecution(state.workflowInstance(), Trigger.natural()));
    stateManager.receive(Event.dequeue(state.workflowInstance()));
    stateManager.receive(Event.submit(state.workflowInstance(), TestData.EXECUTION_DESCRIPTION));
    stateManager.receive(Event.submitted(state.workflowInstance(), "exec-1"));
    stateManager.receive(Event.started(state.workflowInstance()));
    stateManager.receive(Event.terminate(state.workflowInstance(), Optional.of(20)));

    verify(stats).exitCode(state.workflowInstance().workflowId(), 20);
  }

  @Test
  public void shouldUpdateAfterAFail() throws Exception {
    RunState state = RunState.create(WORKFLOW_INSTANCE, RunState.State.NEW, time, outputHandler);
    stateManager.initialize(state);

    stateManager.receive(Event.timeTrigger(state.workflowInstance()));
    stateManager.receive(Event.runError(state.workflowInstance(), "error"));
    stateManager.receive(Event.retryAfter(state.workflowInstance(), 0));

    i = Instant.parse("2015-12-31T23:59:15.000Z");
    stateManager.receive(Event.dequeue(state.workflowInstance()));
    stateManager.receive(Event.created(state.workflowInstance(), "test_execution_id", "img"));

    i = Instant.parse("2015-12-31T23:59:16.000Z");
    stateManager.receive(Event.started(state.workflowInstance()));

    verify(stats).submitToRunningTime(Matchers.eq(1L));
  }
}
