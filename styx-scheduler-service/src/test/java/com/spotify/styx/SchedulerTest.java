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

package com.spotify.styx;

import static com.spotify.styx.state.TimeoutConfig.createWithDefaultTtl;
import static java.time.Duration.ofSeconds;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.state.TimeoutConfig;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.Time;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import org.junit.Test;

public class SchedulerTest {

  private static final WorkflowInstance INSTANCE = WorkflowInstance.create(
      TestData.WORKFLOW_ID, "2016-12-02");

  StateManager stateManager;
  Scheduler scheduler;

  Instant now = Instant.parse("2016-12-02T22:00:00Z");
  Time time = () -> now;

  private void setUp(int timeoutSeconds, RunState runState) throws StateManager.IsClosed {
    TimeoutConfig timeoutConfig = createWithDefaultTtl(ofSeconds(timeoutSeconds));

    stateManager = new SyncStateManager();
    scheduler = new Scheduler(time, timeoutConfig, stateManager);

    stateManager.initialize(runState);
  }

  @Test
  public void shouldTimeoutActiveState() throws Exception {
    setUp(5, RunState.fresh(INSTANCE, time));

    now = now.plus(5, ChronoUnit.SECONDS);
    scheduler.tick();

    assertThat(stateManager.get(INSTANCE).state(), is(State.FAILED));
  }

  @Test
  public void shouldNotTimeoutTerminalState() throws Exception {
    setUp(0, RunState.create(INSTANCE, State.DONE, time));

    scheduler.tick();

    assertThat(stateManager.get(INSTANCE).state(), is(State.DONE));
  }

  @Test
  public void shouldNotTransitionIfNotTimedOut() throws Exception {
    setUp(20, RunState.fresh(INSTANCE, time));

    scheduler.tick();

    assertThat(stateManager.get(INSTANCE).state(), is(State.NEW));
  }

  @Test
  public void shouldTriggerRetryIfDelayHasPassed() throws Exception {
    StateData stateData = StateData.builder().retryDelayMillis(15_000).build();
    setUp(20, RunState.create(INSTANCE, State.QUEUED, stateData, time));

    now = now.plus(15, ChronoUnit.SECONDS);
    scheduler.tick();

    assertThat(stateManager.get(INSTANCE).state(), is(State.PREPARE));
  }
}
