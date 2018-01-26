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

import static com.spotify.styx.testdata.TestData.EXECUTION_DESCRIPTION;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowId;
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
    outputHandler = new MonitoringHandler(stats);
  }

  @Test
  public void shouldMarkExitCode() throws Exception {
    RunState state = RunState.create(WORKFLOW_INSTANCE, RunState.State.NEW, time, outputHandler);
    stateManager.trigger(state, trigger);

    stateManager.receive(Event.triggerExecution(state.workflowInstance(), Trigger.natural()));
    stateManager.receive(Event.dequeue(state.workflowInstance()));
    stateManager.receive(Event.submit(state.workflowInstance(), EXECUTION_DESCRIPTION, "exec-1"));
    stateManager.receive(Event.submitted(state.workflowInstance(), "exec-1"));
    stateManager.receive(Event.started(state.workflowInstance()));
    stateManager.receive(Event.terminate(state.workflowInstance(), Optional.of(20)));

    verify(stats).recordExitCode(state.workflowInstance().workflowId(), 20);
  }

  @Test
  public void shouldNotMarkExitCodeIfNotPresent() throws Exception {
    RunState state = RunState.create(WORKFLOW_INSTANCE, RunState.State.NEW, time, outputHandler);
    stateManager.trigger(state, trigger);

    stateManager.receive(Event.triggerExecution(state.workflowInstance(), Trigger.natural()));
    stateManager.receive(Event.dequeue(state.workflowInstance()));
    stateManager.receive(Event.submit(state.workflowInstance(), EXECUTION_DESCRIPTION, "exec-1"));
    stateManager.receive(Event.submitted(state.workflowInstance(), "exec-1"));
    stateManager.receive(Event.started(state.workflowInstance()));
    stateManager.receive(Event.terminate(state.workflowInstance(), Optional.empty()));

    verify(stats, never()).recordExitCode(any(WorkflowId.class), anyInt());
  }
}
