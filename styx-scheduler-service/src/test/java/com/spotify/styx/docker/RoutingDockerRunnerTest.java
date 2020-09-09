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

package com.spotify.styx.docker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.styx.docker.DockerRunner.RunSpec;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RoutingDockerRunnerTest extends AbstractRoutingRunnerTest<DockerRunner> {

  @Mock private RunSpec runSpec;

  private DockerRunner dockerRunner;

  @Before
  public void setUp() {
    dockerRunner = new RoutingDockerRunner(this::create, runnerId);
    when(runnerId.apply(runState)).thenReturn("default");
  }

  @Test
  public void testUsesCreatesRunnerOnStart() throws Exception {
    dockerRunner.start(runState, runSpec);

    assertThatCreateCountersContains("default");
    verify(createdRunners.get("default")).start(runState, runSpec);
  }

  @Test
  public void testUsesCreatesRunnerOnPoll() {
    dockerRunner.poll(runState);

    assertThatCreateCountersContains("default");
    verify(createdRunners.get("default")).poll(runState);
  }

  @Test
  public void testCleanup() throws Exception {
    dockerRunner.start(runState, runSpec);
    dockerRunner.cleanup();

    assertThatCreateCountersContains("default");
    verify(createdRunners.get("default")).cleanup();
  }

  @Test
  public void testCreatesOnlyOneRunnerPerDockerId() throws Exception {
    dockerRunner.start(runState, runSpec);
    dockerRunner.start(runState, runSpec);

    assertThatCreateCountersContains("default");
  }

  @Test
  public void testSwitchesDockerRunner() throws Exception {
    when(runnerId.apply(runState)).thenReturn("id-1", "id-2");

    dockerRunner.start(runState, runSpec);
    dockerRunner.start(runState, runSpec);

    assertThatCreateCountersContains("id-1", "id-2");
  }

  @Test
  public void testCreatedRunnersAreClosed() throws Exception {
    when(runnerId.apply(runState)).thenReturn("id-1", "id-2");

    dockerRunner.start(runState, runSpec);
    dockerRunner.start(runState, runSpec);
    dockerRunner.close();

    assertThatCreateCountersContains("id-1", "id-2");
    for (DockerRunner runner : createdRunners.values()) {
      verify(runner).close();
    }
  }

  @Override
  protected DockerRunner mockRunner() {
    return mock(DockerRunner.class);
  }
}
