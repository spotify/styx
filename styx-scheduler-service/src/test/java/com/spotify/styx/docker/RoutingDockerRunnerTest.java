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

import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Maps;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.testdata.TestData;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RoutingDockerRunnerTest {

  private static final WorkflowInstance WORKFLOW_INSTANCE = WorkflowInstance.create(
      TestData.WORKFLOW_ID, "param");
  private static final String MOCK_EXEC_ID = "mock-run-id-0";
  private static final DockerRunner.RunSpec RUN_SPEC =
      DockerRunner.RunSpec.simple(MOCK_EXEC_ID, "busybox");

  private int createCounter = 0;
  private Map<String, DockerRunner> createdRunners = Maps.newHashMap();
  @Mock private Supplier<String> dockerId;

  private DockerRunner dockerRunner;
  @Mock private RunState runState;

  @Before
  public void setUp() {
    dockerRunner = new RoutingDockerRunner(this::create, dockerId);
  }

  @Test
  public void testUsesCreatesRunnerOnStart() throws Exception {
    when(dockerId.get()).thenReturn("default");
    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);

    assertThat(createdRunners, hasKey("default"));
    verify(createdRunners.get("default")).start(WORKFLOW_INSTANCE, RUN_SPEC);
  }

  @Test
  public void testUsesCreatesRunnerOnPoll() {
    when(dockerId.get()).thenReturn("default");
    dockerRunner.poll(runState);

    assertThat(createdRunners, hasKey("default"));
    verify(createdRunners.get("default")).poll(runState);
  }

  @Test
  public void testUsesDefaultRunnerOnCleanup() throws Exception {
    when(dockerId.get()).thenReturn("default");
    dockerRunner.cleanup();

    assertThat(createdRunners, hasKey("default"));
    verify(createdRunners.get("default")).cleanup();
  }

  @Test
  public void testCreatesOnlyOneRunnerPerDockerId() throws Exception {
    when(dockerId.get()).thenReturn("default");
    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);
    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);

    assertThat(createCounter, is(1));
    assertThat(createdRunners.keySet(), hasSize(1));
    assertThat(createdRunners, hasKey("default"));
  }

  @Test
  public void testSwitchesDockerRunner() throws Exception {
    Mockito.<Supplier>reset(dockerId);
    when(dockerId.get()).thenReturn("id-1", "id-2");

    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);
    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);

    assertThat(createdRunners, hasKey("id-1"));
    assertThat(createdRunners, hasKey("id-2"));
  }

  @Test
  public void testCreatedRunnersAreClosed() throws Exception {
    Mockito.<Supplier>reset(dockerId);
    when(dockerId.get()).thenReturn("id-1", "id-2");

    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);
    dockerRunner.start(WORKFLOW_INSTANCE, RUN_SPEC);
    dockerRunner.close();

    assertThat(createCounter, is(2));
    assertThat(createdRunners.keySet(), hasSize(2));
    for (DockerRunner runner : createdRunners.values()) {
      verify(runner).close();
    }
  }

  private DockerRunner create(String id) {
    DockerRunner mock = mock(DockerRunner.class);
    createCounter++;
    createdRunners.put(id, mock);
    return mock;
  }
}
