/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2020 Spotify AB
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

package com.spotify.styx.flyte;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.styx.docker.AbstractRoutingRunnerTest;
import com.spotify.styx.model.FlyteExecConf;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RoutingFlyteRunnerTest extends AbstractRoutingRunnerTest<FlyteRunner> {

  private static final String EXEC_NAME = "exec-name";

  @Mock private FlyteExecutionId executionId;
  @Mock private FlyteExecConf execConf;

  private FlyteRunner flyteRunner;

  @Before
  public void setUp() {
    flyteRunner = new RoutingFlyteRunner(this::create, runnerId);
    when(runnerId.apply(runState)).thenReturn("default");
  }

  @Test
  public void shouldCreateRunnerOnCreateExecution() throws FlyteRunner.CreateExecutionException {
    flyteRunner.createExecution(runState, EXEC_NAME, execConf);

    assertThatCreateCountersContains("default");
    verify(createdRunners.get("default")).createExecution(runState, EXEC_NAME, execConf);
  }

  @Test
  public void shouldCreateRunnerOnTerminateExecution() {
    flyteRunner.terminateExecution(runState, executionId);

    assertThatCreateCountersContains("default");
    verify(createdRunners.get("default")).terminateExecution(runState, executionId);
  }

  @Test
  public void testUsesCreatesRunnerOnPoll() {
    flyteRunner.poll(executionId, runState);

    assertThatCreateCountersContains("default");
    verify(createdRunners.get("default")).poll(executionId, runState);
  }

  @Test
  public void testCreatesOnlyOneRunnerPerRunnerId() throws Exception {
    flyteRunner.createExecution(runState, EXEC_NAME, execConf);
    flyteRunner.createExecution(runState, EXEC_NAME, execConf);

    assertThatCreateCountersContains("default");
  }

  @Test
  public void testSwitchesRunners() throws Exception {
    when(runnerId.apply(runState)).thenReturn("id-1", "id-2");

    flyteRunner.createExecution(runState, EXEC_NAME, execConf);
    flyteRunner.createExecution(runState, EXEC_NAME, execConf);

    assertThatCreateCountersContains("id-1", "id-2");
  }

  @Test
  public void testCreatedRunnersAreClosed() throws Exception {
    when(runnerId.apply(runState)).thenReturn("id-1", "id-2");

    flyteRunner.createExecution(runState, EXEC_NAME, execConf);
    flyteRunner.createExecution(runState, EXEC_NAME, execConf);
    flyteRunner.close();

    assertThatCreateCountersContains("id-1", "id-2");
    for (var runner : createdRunners.values()) {
      verify(runner).close();
    }
  }

  @Override
  protected FlyteRunner mockRunner() {
    return mock(FlyteRunner.class);
  }
}
