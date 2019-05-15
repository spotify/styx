/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A thin wrapper around the fabric8 {@link KubernetesClient} that exposes a smaller non-fluent interface that is
 * easier to mock and monitor using proxying.
 * @see com.spotify.styx.monitoring.MeteredFabric8KubernetesClientProxy
 */
public interface Fabric8KubernetesClient {

  Optional<Pod> getPod(String executionId);

  PodList listPods();

  Pod createPod(Pod pod);

  boolean deletePod(String name);

  Watch watchPods(Watcher<Pod> watcher);

  Optional<Secret> getSecret(String name);

  SecretList listSecrets();

  Secret createSecret(Secret secret);

  boolean deleteSecret(String name);

  static Fabric8KubernetesClient of(KubernetesClient kubernetesClient) {
    return new Impl(kubernetesClient);
  }

  class Impl implements Fabric8KubernetesClient {

    private static final Logger log = LoggerFactory.getLogger(Fabric8KubernetesClient.class);

    private final KubernetesClient client;

    public Impl(KubernetesClient client) {
      this.client = Objects.requireNonNull(client);
    }

    @Override
    public Optional<Pod> getPod(String name) {
      return Optional.ofNullable(client.pods().withName(name).get());
    }

    @Override
    public Pod createPod(Pod pod) {
      return client.pods().create(pod);
    }

    @Override
    public Optional<Secret> getSecret(String name) {
      return Optional.ofNullable(client.secrets().withName(name).get());
    }

    @Override
    public SecretList listSecrets() {
      return client.secrets().list();
    }

    @Override
    public Secret createSecret(Secret secret) {
      return client.secrets().create(secret);
    }

    @Override
    public boolean deleteSecret(String name) {
      return Optional.ofNullable(client.secrets().withName(name).delete()).orElse(false);
    }

    @Override
    public Watch watchPods(Watcher<Pod> watcher) {
      return client.pods().watch(watcher);
    }

    @Override
    public PodList listPods() {
      return client.pods().list();
    }

    @Override
    public boolean deletePod(String name) {
      return Optional.ofNullable(client.pods().withName(name).delete()).orElse(false);
    }
  }
}
