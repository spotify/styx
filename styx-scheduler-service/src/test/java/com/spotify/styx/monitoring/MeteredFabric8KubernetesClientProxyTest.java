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
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import com.spotify.styx.docker.Fabric8KubernetesClient;
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
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MeteredFabric8KubernetesClientProxyTest {

  @Rule
  public ExpectedException expect = ExpectedException.none();

  @Mock private Fabric8KubernetesClient fabric8KubernetesClient;
  @Mock private Stats stats;

  private Instant now = Instant.now();
  private Instant later = now.plusMillis(123);
  private List<Instant> times = Arrays.asList(now, later);
  private int pos = 0;
  private Time time = () -> times.get(pos++);

  private Fabric8KubernetesClient proxy;

  @Before
  public void setUp() {
    proxy = MeteredFabric8KubernetesClientProxy.instrument(fabric8KubernetesClient, stats, time);
  }

  @Test
  public void instrumentClientMethod() {
    proxy.listPods();

    verify(fabric8KubernetesClient).listPods();
    verify(stats).recordKubernetesOperation("listPods", 123, "success");
  }

  @Test
  public void surfaceExceptions() {
    doThrow(new RuntimeException("with message")).when(fabric8KubernetesClient).listPods();

    expect.expect(RuntimeException.class);
    expect.expectMessage("with message");

    proxy.listPods();
  }

  @Test
  public void reportKubernetesClientException() {
    doThrow(new KubernetesClientException("enhance your calm", 429, new Status()))
        .when(fabric8KubernetesClient).listPods();

    try {
      proxy.listPods();
      fail("Expected exception");
    } catch (Exception ignored) {
    }

    verify(stats).recordKubernetesOperationError("listPods", "kubernetes-client", 429);
  }

  @Test
  public void reportKubernetesClientTimeoutException() {
    doThrow(new KubernetesClientTimeoutException(List.of(), 10L, TimeUnit.SECONDS))
        .when(fabric8KubernetesClient).listPods();

    try {
      proxy.listPods();
      fail("Expected exception");
    } catch (Exception ignored) {
    }

    verify(stats).recordKubernetesOperationError("listPods", "kubernetes-client-timeout", 0);
  }

  @Test
  public void reportUnknownError() {
    doThrow(new RuntimeException()).when(fabric8KubernetesClient).listPods();

    try {
      proxy.listPods();
      fail("Expected exception");
    } catch (Exception ignored) {
    }

    verify(stats).recordKubernetesOperationError("listPods", "unknown", 0);
  }
}
