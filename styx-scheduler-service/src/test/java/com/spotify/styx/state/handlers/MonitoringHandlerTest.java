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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.MonitoringHandler;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.testdata.TestData;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MonitoringHandlerTest {

  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-04-04T08");

  @Mock Stats stats;

  private MonitoringHandler monitoringHandler;

  @Before
  public void setUp() throws Exception {
    stats = Mockito.mock(Stats.class);
    monitoringHandler = new MonitoringHandler(stats);
  }

  @Test
  public void shouldMarkExitCode() throws Exception {
    RunState state = RunState.create(WORKFLOW_INSTANCE, State.TERMINATED,
        StateData.newBuilder().lastExit(20).build());

    monitoringHandler.transitionInto(state);

    verify(stats).recordExitCode(state.workflowInstance().workflowId(), 20);
  }

  @Test
  public void shouldNotMarkExitCodeIfNotPresent() throws Exception {
    RunState state = RunState.create(WORKFLOW_INSTANCE, State.TERMINATED);

    monitoringHandler.transitionInto(state);

    verify(stats, never()).recordExitCode(any(WorkflowId.class), anyInt());
  }
}
