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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DockerRunnerIdTest {

  private static final String TEST_RUNNER_ID = "test";

  private DockerRunnerId dockerRunnerId;

  @Mock private Supplier<StyxConfig> configSupplier;

  @Before
  public void setUp() {
    when(configSupplier.get()).thenReturn(StyxConfig.newBuilder().globalDockerRunnerId("default").build());
    dockerRunnerId = new DockerRunnerId(configSupplier);
  }

  @Test
  public void testGetRunnerIdFromConfigSupplierWhenSubmitting() {
    var runState = RunState.create(mock(WorkflowInstance.class), State.SUBMITTING, StateData.newBuilder()
        .runnerId(TEST_RUNNER_ID)
        .build());
    assertThat(dockerRunnerId.apply(runState), is("default"));
  }

  @Test
  public void testGetRunnerIdFromRunState() {
    var runState = RunState.create(mock(WorkflowInstance.class), State.SUBMITTED, StateData.newBuilder()
        .runnerId(TEST_RUNNER_ID)
        .build());
    assertThat(dockerRunnerId.apply(runState), is(TEST_RUNNER_ID));
  }

  @Test
  public void testGetRunnerIdFromConfigSupplier() {
    var runState = RunState.create(mock(WorkflowInstance.class), State.QUEUED, StateData.zero());
    assertThat(dockerRunnerId.apply(runState), is("default"));
  }
}
