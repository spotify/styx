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

import static com.spotify.styx.testdata.TestData.WORKFLOW_WITH_RESOURCES;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.container.v1beta1.Container;
import com.google.api.services.container.v1beta1.model.Cluster;
import com.google.api.services.container.v1beta1.model.MasterAuth;
import com.google.common.collect.ImmutableMap;
import com.spotify.styx.StyxScheduler.KubernetesClientFactory;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import com.spotify.styx.storage.TransactionFunction;
import com.spotify.styx.util.Shard;
import com.spotify.styx.workflow.WorkflowInitializer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StyxSchedulerTest {

  @Captor private ArgumentCaptor<io.fabric8.kubernetes.client.Config> k8sClientConfigCaptor;
  @Captor private ArgumentCaptor<Shard> shardArgumentCaptor;

  @Mock private KubernetesClientFactory kubernetesClientFactory;
  @Mock private NamespacedKubernetesClient kubernetesClient;
  @Mock(answer = RETURNS_DEEP_STUBS) private Container gkeClient;
  @Mock private Container.Projects.Locations.Clusters.Get gkeClusterGet;
  @Mock private Storage storage;
  @Mock private StorageTransaction transaction;
  @Mock private WorkflowInitializer workflowInitializer;
  @Mock private Stats stats;
  @Mock private StateManager stateManager;
  @Mock private BiConsumer<Optional<Workflow>, Optional<Workflow>> workflowConsumer;

  private StyxScheduler styxScheduler;

  @Before
  public void setUp() throws Exception {
    when(kubernetesClientFactory.apply(any())).thenReturn(kubernetesClient);
    when(gkeClient.projects().locations().clusters().get(any())).thenReturn(gkeClusterGet);

    styxScheduler = StyxScheduler.newBuilder().build();
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

  @Test
  public void shouldUpdateShardsAccordingToUsedResources() throws IOException {
    when(storage.runInTransaction(any())).thenAnswer(
        a -> a.getArgumentAt(0, TransactionFunction.class).apply(transaction));
    final Map<String, Long> resourcesUsageMap = ImmutableMap.of("res1", 257L);

    styxScheduler.updateShards(storage, resourcesUsageMap);

    verify(transaction, times(128)).store(shardArgumentCaptor.capture());
    shardsWithValue(shardArgumentCaptor, 3L, 1L);
    shardsWithValue(shardArgumentCaptor, 2L, 127L);
  }

  @Test
  public void shouldFailToUpdateShardsAccordingToUsedResources() throws IOException {
    final IOException exception = new IOException();
    when(storage.runInTransaction(any())).thenThrow(exception);
    final Map<String, Long> resourcesUsageMap = ImmutableMap.of("res1", 257L);

    try {
      styxScheduler.updateShards(storage, resourcesUsageMap);
      fail();
    } catch (Exception e) {
      assertThat(e.getCause(), is(exception));
    }
  }

  @Test
  public void shouldResetShardsOfResource() throws IOException {
    when(storage.runInTransaction(any())).thenAnswer(
        a -> a.getArgumentAt(0, TransactionFunction.class).apply(transaction));

    styxScheduler.resetShards(storage, Resource.create("res1", 300));

    verify(transaction, times(128)).store(shardArgumentCaptor.capture());
    shardsWithValue(shardArgumentCaptor, 0L, 128);
  }

  @Test
  public void shouldFailToResetShardsOfResource() throws IOException {
    final IOException exception = new IOException();
    when(storage.runInTransaction(any())).thenThrow(exception);

    try {
      styxScheduler.resetShards(storage, Resource.create("res1", 300));
      fail();
    } catch (Exception e) {
      assertThat(e.getCause(), is(exception));
    }
  }

  @Test
  public void shouldHandleModifyWorkflow() {
    when(workflowInitializer.inspectChange(WORKFLOW_WITH_RESOURCES)).thenReturn(Optional.of(WORKFLOW_WITH_RESOURCES));
    final Consumer<Workflow> consumer = StyxScheduler.workflowChanged(workflowInitializer, stats,
        stateManager, workflowConsumer);
    consumer.accept(WORKFLOW_WITH_RESOURCES);
    verify(workflowInitializer).inspectChange(WORKFLOW_WITH_RESOURCES);
    verify(stats).registerActiveStatesMetric(eq(WORKFLOW_WITH_RESOURCES.id()), any());
    verify(workflowConsumer).accept(Optional.of(WORKFLOW_WITH_RESOURCES),
        Optional.of(WORKFLOW_WITH_RESOURCES));
  }

  @Test
  public void shouldHandleWorkflowCreate() {
    when(workflowInitializer.inspectChange(WORKFLOW_WITH_RESOURCES)).thenReturn(Optional.empty());
    final Consumer<Workflow> consumer = StyxScheduler.workflowChanged(workflowInitializer, stats,
        stateManager, workflowConsumer);
    consumer.accept(WORKFLOW_WITH_RESOURCES);
    verify(workflowInitializer).inspectChange(WORKFLOW_WITH_RESOURCES);
    verify(stats).registerActiveStatesMetric(eq(WORKFLOW_WITH_RESOURCES.id()), any());
    verify(workflowConsumer).accept(Optional.empty(), Workflow.of(WORKFLOW_WITH_RESOURCES));
  }

  @Test
  public void shouldHandleWorkflowRemove() {
    final Consumer<Workflow> consumer = StyxScheduler.workflowRemoved(storage, workflowConsumer);
    consumer.accept(WORKFLOW_WITH_RESOURCES);
    verify(workflowConsumer).accept(Optional.of(WORKFLOW_WITH_RESOURCES), Optional.empty());
  }

  @Test
  public void shouldFailToDeleteWorkflow() throws IOException {
    final IOException exception = new IOException();
    doThrow(exception).when(storage).delete(WORKFLOW_WITH_RESOURCES.id());
    final Consumer<Workflow> consumer = StyxScheduler.workflowRemoved(storage, workflowConsumer);
    try {
      consumer.accept(WORKFLOW_WITH_RESOURCES);
      fail();
    } catch (Exception e) {
      assertThat(e.getCause(), is(exception));
    }
    verify(workflowConsumer, never()).accept(Optional.of(WORKFLOW_WITH_RESOURCES), Optional.empty());
  }

  private void shardsWithValue(ArgumentCaptor<Shard> shardArgumentCaptor, long value, long times) {
    assertThat(shardArgumentCaptor.getAllValues()
            .stream()
            .filter(shard -> shard.value() == value)
            .count(),
        is(times));
  }
}
