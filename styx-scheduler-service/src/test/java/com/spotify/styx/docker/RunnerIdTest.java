/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2019 Spotify AB
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

package com.spotify.styx.docker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import java.util.function.Supplier;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class RunnerIdTest {

  private static final String TEST_RUNNER_ID = "test";
  private static final StateData STATE_DATA = StateData.newBuilder()
      .runnerId(TEST_RUNNER_ID)
      .build();
  private static final String DOCKER_RUNNER_ID = "defaultDocker";
  private static final String FLYTE_RUNNER_ID = "defaultFlyte";
  private static final StyxConfig STYX_CONFIG = StyxConfig.newBuilder()
      .globalDockerRunnerId(DOCKER_RUNNER_ID)
      .globalFlyteRunnerId(FLYTE_RUNNER_ID)
      .build();

  @Test
  @Parameters(method = "runnersId")
  public void testGetRunnerIdFromConfigSupplierWhenSubmitting(RunnerId runnerId, String id) {
    var runState = createRunState(State.SUBMITTING, STATE_DATA);

    assertThat(runnerId.apply(runState), is(id));
  }

  @Test
  @Parameters(method = "runnersId")
  public void testGetRunnerIdFromRunState(RunnerId runnerId, String id) {
    var runState = createRunState(State.SUBMITTED, STATE_DATA);

    assertThat(runnerId.apply(runState), is(TEST_RUNNER_ID));
  }

  @Test
  @Parameters(method = "runnersId")
  public void testGetRunnerIdFromConfigSupplier(RunnerId runnerId, String id) {
    var runState = createRunState(State.QUEUED, StateData.zero());

    assertThat(runnerId.apply(runState), is(id));
  }

  private RunState createRunState(State state, StateData stateData) {
    return RunState.create(mock(WorkflowInstance.class), state, stateData);
  }

  private static Object[] runnersId() {
    Supplier<StyxConfig> configSupplier = () -> STYX_CONFIG;
    return new Object[] {
        new Object[] { RunnerId.dockerRunnerId(configSupplier), DOCKER_RUNNER_ID },
        new Object[] { RunnerId.flyteRunnerId(configSupplier), FLYTE_RUNNER_ID }
    };
  }
}
