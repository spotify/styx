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

import static com.spotify.styx.util.ExceptionUtil.findCause;

import com.spotify.styx.docker.Fabric8KubernetesClient;
import com.spotify.styx.util.Time;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.KubernetesClientTimeoutException;
import java.lang.reflect.Proxy;
import javaslang.control.Try;

/**
 * A proxy for instrumenting an instance of {@link Fabric8KubernetesClient} using {@link Proxy#newProxyInstance}.
 */
public class MeteredFabric8KubernetesClientProxy extends MeteredProxy<Fabric8KubernetesClient> {

  private MeteredFabric8KubernetesClientProxy(Fabric8KubernetesClient delegate, Stats stats, Time time) {
    super(delegate, stats, time);
  }

  public static Fabric8KubernetesClient instrument(Fabric8KubernetesClient kubernetesClient, Stats stats, Time time) {
    return MeteredProxy.instrument(Fabric8KubernetesClient.class,
        new MeteredFabric8KubernetesClientProxy(kubernetesClient, stats, time));
  }

  @Override
  protected void checkResult(String operation, long durationMillis, Try<?> result, Stats stats) {
    result.onFailure(error -> reportOperationError(operation, error, stats));
    final String status = (result.isSuccess()) ? "success" : "failure";
    stats.recordKubernetesOperation(operation, durationMillis, status);
  }

  private void reportOperationError(String operation, Throwable e, Stats stats) {
    var kubernetesClientException = findCause(e, KubernetesClientException.class);
    if (kubernetesClientException != null) {
      final String type = (kubernetesClientException instanceof KubernetesClientTimeoutException)
                          ? "kubernetes-client-timeout"
                          : "kubernetes-client";
      stats.recordKubernetesOperationError(operation, type, kubernetesClientException.getCode());
    } else {
      stats.recordKubernetesOperationError(operation, "unknown", 0);
    }
  }
}
