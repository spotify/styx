/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2017 Spotify AB
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

package com.spotify.styx;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.container.v1beta1.Container;
import com.google.api.services.container.v1beta1.model.Cluster;
import com.google.api.services.container.v1beta1.model.MasterAuth;
import com.google.common.collect.ImmutableMap;
import com.spotify.styx.StyxScheduler.KubernetesClientFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StyxSchedulerTest {

  @Captor ArgumentCaptor<io.fabric8.kubernetes.client.Config> k8sClientConfigCaptor;

  @Mock KubernetesClientFactory kubernetesClientFactory;
  @Mock NamespacedKubernetesClient kubernetesClient;
  @Mock(answer = RETURNS_DEEP_STUBS) Container gkeClient;
  @Mock Container.Projects.Locations.Clusters.Get gkeClusterGet;

  @Before
  public void setUp() throws Exception {
    when(kubernetesClientFactory.apply(any())).thenReturn(kubernetesClient);
    when(gkeClient.projects().locations().clusters().get(any())).thenReturn(gkeClusterGet);
  }

  @Test
  public void testGetKubernetesClient() throws Exception {

    final String project = "test-project";
    final String zone = "test-zone";
    final String cluster = "test-cluster";
    final String namespace = "test-namespace";

    final Config config = ConfigFactory.parseMap(ImmutableMap.of(
        "styx.gke.foo.project-id", project,
        "styx.gke.foo.cluster-zone", zone,
        "styx.gke.foo.cluster-id", cluster,
        "styx.gke.foo.namespace", namespace));

    final String endpoint = "k8s.example.com:4711";
    final String clusterCaCertificate = "cluster-ca-cert";
    final String clientCertificate = "client-cert";
    final String clientKey = "client-key";

    final Cluster gkeCluster = new Cluster();
    gkeCluster.setEndpoint(endpoint);

    final MasterAuth masterAuth = new MasterAuth();
    masterAuth.setClusterCaCertificate(clusterCaCertificate);
    masterAuth.setClientCertificate(clientCertificate);
    masterAuth.setClientKey(clientKey);
    gkeCluster.setMasterAuth(masterAuth);

    when(gkeClusterGet.execute()).thenReturn(gkeCluster);

    final NamespacedKubernetesClient client =
        StyxScheduler.getKubernetesClient(config, "foo", gkeClient, kubernetesClientFactory);
    assertThat(client, is(theInstance(kubernetesClient)));

    verify(kubernetesClientFactory).apply(k8sClientConfigCaptor.capture());

    final io.fabric8.kubernetes.client.Config k8sConfig = k8sClientConfigCaptor.getValue();

    assertThat(k8sConfig.getMasterUrl(), is("https://" + endpoint + "/"));
    assertThat(k8sConfig.getCaCertData(), is(clusterCaCertificate));
    assertThat(k8sConfig.getClientCertData(), is(clientCertificate));
    assertThat(k8sConfig.getClientKeyData(), is(clientKey));
    assertThat(k8sConfig.getNamespace(), is(namespace));
  }
}
