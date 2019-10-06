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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.spotify.styx.ServiceAccountKeyManager;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.StateManager;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DockerRunnerTest {

  @Rule public ExpectedException expectedException = ExpectedException.none();

  @Mock(answer = Answers.RETURNS_DEEP_STUBS) private Fabric8KubernetesClient client;

  @Mock private StateManager stateManager;

  @Mock private ServiceAccountKeyManager serviceAccountKeyManager;

  @Before
  public void setUp() {
    when(client.listPods().getItems()).thenReturn(List.of());
  }

  @Test
  public void shouldCreateKubernetesDockerRunner() throws IOException {
    var dockerRunner = DockerRunner.kubernetes(client, stateManager, Stats.NOOP, serviceAccountKeyManager,
        () -> true, "test", Set.of(), Duration.ZERO);
    assertThat(dockerRunner, notNullValue());
    dockerRunner.close();
  }

  @Test
  public void shouldFailToInit() {
    var exception = new RuntimeException();
    when(client.watchPods(any())).thenThrow(exception);
    expectedException.expect(RuntimeException.class);
    expectedException.expectCause(is(exception));
    DockerRunner.kubernetes(client, stateManager, Stats.NOOP, serviceAccountKeyManager, () -> true, "test", Set.of(),
        Duration.ZERO);
  }
}
