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

import static com.spotify.styx.model.Schedule.DAYS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Gauge;
import com.google.api.services.container.v1beta1.Container;
import com.google.api.services.container.v1beta1.model.Cluster;
import com.google.api.services.container.v1beta1.model.MasterAuth;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.RateLimiter;
import com.spotify.styx.StyxScheduler.KubernetesClientFactory;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.QueuedStateManager;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import com.spotify.styx.storage.TransactionFunction;
import com.spotify.styx.util.Shard;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerUtil;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StyxSchedulerTest {

  @Captor private ArgumentCaptor<io.fabric8.kubernetes.client.Config> k8sClientConfigCaptor;
  @Captor private ArgumentCaptor<Shard> shardArgumentCaptor;
  @Captor private ArgumentCaptor<Gauge<Long>> longGaugeCaptor;
  @Captor private ArgumentCaptor<Gauge<Double>> doubleGaugeCaptor;

  @Mock private KubernetesClientFactory kubernetesClientFactory;
  @Mock private NamespacedKubernetesClient kubernetesClient;
  @Mock(answer = RETURNS_DEEP_STUBS) private Container gkeClient;
  @Mock private Container.Projects.Locations.Clusters.Get gkeClusterGet;
  @Mock private Storage storage;
  @Mock private StorageTransaction transaction;
  @Mock private QueuedStateManager stateManager;
  @Mock private Supplier<Map<WorkflowId, Workflow>> workflowCache;
  @Mock private RateLimiter submissionRateLimiter;
  @Mock private Stats stats;
  @Mock private Time time;

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
  public void shouldUpdateShardsAccordingToUsedResources() throws Exception {
    when(storage.runInTransaction(any())).thenAnswer(
        a -> a.<TransactionFunction>getArgument(0).apply(transaction));
    final Map<String, Long> resourcesUsageMap = ImmutableMap.of("res1", 257L);

    styxScheduler.updateShards(storage, resourcesUsageMap);

    verify(transaction, times(128)).store(shardArgumentCaptor.capture());
    shardsWithValue(shardArgumentCaptor, 3L, 1L);
    shardsWithValue(shardArgumentCaptor, 2L, 127L);
  }

  @Test
  public void shouldFailToUpdateShardsAccordingToUsedResources() throws Exception {
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
  public void shouldResetShardsOfResource() throws Exception {
    when(storage.runInTransaction(any())).thenAnswer(
        a -> a.<TransactionFunction>getArgument(0).apply(transaction));

    styxScheduler.resetShards(storage, Resource.create("res1", 300));

    verify(transaction, times(128)).store(shardArgumentCaptor.capture());
    shardsWithValue(shardArgumentCaptor, 0L, 128);
  }

  @Test
  public void shouldFailToResetShardsOfResource() throws Exception {
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
  public void testSetupMetrics() throws IOException {
    final WorkflowId wfid1 = WorkflowId.create("foo1", "bar1");
    final WorkflowId wfid2 = WorkflowId.create("foo2", "bar2");
    final WorkflowConfiguration wfc1 = WorkflowConfiguration.builder()
        .id(wfid1.id())
        .schedule(DAYS)
        .dockerImage("foo1:bar1")
        .build();
    final WorkflowConfiguration wfc2 = WorkflowConfiguration.builder()
        .id(wfid2.id())
        .schedule(DAYS)
        .dockerImage("foo2:bar2")
        .build();
    final Workflow wf1 = Workflow.create(wfid1.componentId(), wfc1);
    final Workflow wf2 = Workflow.create(wfid2.componentId(), wfc2);
    final WorkflowInstance wfi1 = WorkflowInstance.create(wfid1, "2018-01-02");
    final WorkflowInstance wfi2 = WorkflowInstance.create(wfid2, "2018-01-02");
    final RunState rs1 = RunState.create(wfi1, State.QUEUED, StateData.newBuilder()
        .trigger(Trigger.natural())
        .build());
    final RunState rs2 = RunState.create(wfi2, State.RUNNING, StateData.newBuilder()
        .trigger(Trigger.adhoc("foobar"))
        .build());

    when(time.get()).thenReturn(Instant.now());
    when(stateManager.getActiveStates()).thenReturn(ImmutableMap.of(wfi1, rs1, wfi2, rs2));
    when(workflowCache.get()).thenReturn(ImmutableMap.of(wfid1, wf1, wfid2, wf2));
    when(storage.enabled()).thenReturn(ImmutableSet.of(wfid1));

    StyxScheduler.setupMetrics(stateManager, workflowCache, storage, submissionRateLimiter, stats, time);

    verify(stats).registerQueuedEventsMetric(longGaugeCaptor.capture());
    verify(stats).registerWorkflowCountMetric(eq("all"), longGaugeCaptor.capture());
    verify(stats).registerWorkflowCountMetric(eq("configured"), longGaugeCaptor.capture());
    verify(stats).registerWorkflowCountMetric(eq("enabled"), longGaugeCaptor.capture());
    verify(stats).registerWorkflowCountMetric(eq("docker_termination_logging_enabled"), longGaugeCaptor.capture());

    for (State state : State.values()) {
      for (String triggerType : TriggerUtil.triggerTypesList()) {
        verify(stats).registerActiveStatesMetric(eq(state), eq(triggerType), longGaugeCaptor.capture());
      }
      verify(stats).registerActiveStatesMetric(eq(state), eq("none"), longGaugeCaptor.capture());
    }

    verify(stats).registerSubmissionRateLimitMetric(doubleGaugeCaptor.capture());

    longGaugeCaptor.getAllValues().forEach(Gauge::getValue);
    doubleGaugeCaptor.getAllValues().forEach(Gauge::getValue);

    verify(stateManager).queuedEvents();

    // Verify that expensive methods were cached
    verify(stateManager, times(1)).getActiveStates();
    verify(storage, times(1)).enabled();

    verifyNoMoreInteractions(storage);
    verifyNoMoreInteractions(stateManager);
  }

  @Test
  public void shouldReflectDevMode() {
    final Config devConfig = ConfigFactory.parseMap(ImmutableMap.of("styx.mode", "development"));
    final Config prodConfig = ConfigFactory.parseMap(ImmutableMap.of("styx.mode", "production"));
    assertThat(StyxScheduler.isDevMode(devConfig), is(true));
    assertThat(StyxScheduler.isDevMode(prodConfig), is(false));
  }

  private void shardsWithValue(ArgumentCaptor<Shard> shardArgumentCaptor, long value, long times) {
    assertThat(shardArgumentCaptor.getAllValues()
            .stream()
            .filter(shard -> shard.value() == value)
            .count(),
        is(times));
  }
}
