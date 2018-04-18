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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.docker.InvalidExecutionException;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.util.Time;
import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

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
  public void instrumentDockerMethod() {
    proxy.cleanup(workflowInstance, "barbaz");

    verify(dockerRunner).cleanup(workflowInstance, "barbaz");
    verify(stats).recordDockerOperation("cleanup", 123, "success");
  }

  @Test
  public void surfaceExceptions() {
    doThrow(new RuntimeException("with message")).when(dockerRunner)
        .cleanup(any(WorkflowInstance.class), anyString());

    expect.expect(RuntimeException.class);
    expect.expectMessage("with message");

    proxy.cleanup(workflowInstance, "foo");
  }

  @Test
  public void reportKubernetesClientException() throws Exception {
    doThrow(new KubernetesClientException("enhance your calm", 429, new Status()))
        .when(dockerRunner).start(any(), any());

    try {
      proxy.start(workflowInstance, runSpec);
      fail("Expected exception");
    } catch (Exception ignored) {
    }

    verify(stats).recordDockerOperationError("start", "kubernetes-client", 429, 123);
  }

  @Test
  public void reportKubernetesClientTimeoutException() throws Exception {
    doThrow(new KubernetesClientTimeoutException(ImmutableList.of(), 10L, TimeUnit.SECONDS))
        .when(dockerRunner).start(any(), any());

    try {
      proxy.start(workflowInstance, runSpec);
      fail("Expected exception");
    } catch (Exception ignored) {
    }

    verify(stats).recordDockerOperationError("start", "kubernetes-client-timeout", 0, 123);
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

    verify(stats).recordDockerOperationError("start", "invalid-execution", 0, 123);
  }

  @Test
  public void reportUnknownError() throws Exception {
    doThrow(new RuntimeException()).when(dockerRunner).start(any(), any());

    try {
      proxy.start(workflowInstance, runSpec);
      fail("Expected exception");
    } catch (Exception ignored) {
    }

    verify(stats).recordDockerOperationError("start", "unknown", 0, 123);
  }
}
