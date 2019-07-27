/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

package com.spotify.styx.monitoring;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.docker.InvalidExecutionException;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MeteredDockerRunnerProxyTest {

  @Rule
  public ExpectedException expect = ExpectedException.none();

  @Mock private WorkflowInstance workflowInstance;
  @Mock private RunSpec runSpec;
  @Mock private DockerRunner dockerRunner;
  @Mock private Stats stats;

  private Instant now = Instant.now();
  private Instant later = now.plusMillis(123);
  private List<Instant> times = Arrays.asList(now, later);
  private int pos = 0;
  private Time time = () -> times.get(pos++);

  private DockerRunner proxy;

  @Before
  public void setUp() {
    proxy = MeteredDockerRunnerProxy.instrument(dockerRunner, stats, time);
  }

  @Test
  public void instrumentDockerMethod() throws IOException {
    proxy.start(workflowInstance, runSpec);

    verify(dockerRunner).start(workflowInstance, runSpec);
    verify(stats).recordDockerOperation("start", 123, "success");
  }

  @Test
  public void surfaceExceptions() throws IOException {
    doThrow(new RuntimeException("with message")).when(dockerRunner)
        .start(any(WorkflowInstance.class), any(RunSpec.class));

    expect.expect(RuntimeException.class);
    expect.expectMessage("with message");

    proxy.start(workflowInstance, runSpec);
  }

  @Test
  public void reportInvalidExecutionException() throws Exception {
    doThrow(new InvalidExecutionException("Maximum number of keys on service account reached"))
        .when(dockerRunner).start(any(), any());

    try {
      proxy.start(workflowInstance, runSpec);
      fail("Expected exception");
    } catch (Exception ignored) {
    }

    verify(stats).recordDockerOperationError("start", "invalid-execution");
  }

  @Test
  public void reportUnknownError() throws Exception {
    doThrow(new RuntimeException()).when(dockerRunner).start(any(), any());

    try {
      proxy.start(workflowInstance, runSpec);
      fail("Expected exception");
    } catch (Exception ignored) {
    }

    verify(stats).recordDockerOperationError("start", "unknown");
  }
}
