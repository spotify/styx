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
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.theInstance;
import static org.junit.Assert.assertThrows;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.codahale.metrics.Gauge;
import com.google.api.services.container.Container;
import com.google.api.services.container.model.Cluster;
import com.google.api.services.container.model.MasterAuth;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.google.common.util.concurrent.RateLimiter;
import com.spotify.styx.StyxScheduler.KubernetesClientFactory;
import com.spotify.styx.flyte.FlyteAdminClientInterceptors;
import com.spotify.styx.flyte.FlyteRunner;
import com.spotify.styx.flyte.NoopFlyteRunner;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.PersistentStateManager;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerUtil;
import com.typesafe.config.ConfigFactory;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.function.Supplier;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class StyxSchedulerTest {

  private static final int DEFAULT_KUBERNETES_REQUEST_TIMEOUT_MILLIS = 60_000;
  private static final String ENDPOINT = "k8s.example.com:4711";
  private static final String TEST_NAMESPACE = "test-namespace";

  @Rule public ExpectedException exception = ExpectedException.none();

  @Captor private ArgumentCaptor<io.fabric8.kubernetes.client.Config> k8sClientConfigCaptor;
  @Captor private ArgumentCaptor<OkHttpClient> httpClientCaptor;
  @Captor private ArgumentCaptor<Gauge<Long>> longGaugeCaptor;
  @Captor private ArgumentCaptor<Gauge<Double>> doubleGaugeCaptor;

  @Mock private KubernetesClientFactory kubernetesClientFactory;
  @Mock private NamespacedKubernetesClient kubernetesClient;
  @Mock(answer = RETURNS_DEEP_STUBS) private Container gkeClient;
  @Mock private Container.Projects.Locations.Clusters.Get gkeClusterGet;
  @Mock private Storage storage;
  @Mock private PersistentStateManager stateManager;
  @Mock private Supplier<Map<WorkflowId, Workflow>> workflowCache;
  @Mock private RateLimiter submissionRateLimiter;
  @Mock private Stats stats;
  @Mock private Time time;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(kubernetesClientFactory.apply(any(), any())).thenReturn(kubernetesClient);
    when(gkeClient.projects().locations().clusters().get(any())).thenReturn(gkeClusterGet);
  }

  @Test
  public void testBuildStyxScheduler() {
    assertThat(StyxScheduler.newBuilder().build(), notNullValue());
  }

  @Test
  @Parameters({ "4711", "" })
  public void testGetKubernetesClient(String k8sRequestTimeoutConfig) throws Exception {
    var clusterCaCertificate = Resources.toString(Resources.getResource("ca.crt"), UTF_8);
    var clientCertificate = Resources.toString(Resources.getResource("client.crt"), UTF_8);
    var clientKey = Resources.toString(Resources.getResource("client.key"), UTF_8);

    final int expectedK8sRequestTimeout;
    if (!k8sRequestTimeoutConfig.isEmpty()) {
      expectedK8sRequestTimeout = Integer.parseInt(k8sRequestTimeoutConfig);
    } else {
      expectedK8sRequestTimeout = DEFAULT_KUBERNETES_REQUEST_TIMEOUT_MILLIS;
    }

    var client = getKubernetesClient(
        k8sRequestTimeoutConfig,
        clusterCaCertificate,
        clientCertificate, clientKey);

    assertThat(client, is(theInstance(kubernetesClient)));

    verify(kubernetesClientFactory).apply(httpClientCaptor.capture(), k8sClientConfigCaptor.capture());

    var k8sConfig = k8sClientConfigCaptor.getValue();
    var httpClient = httpClientCaptor.getValue();

    assertThat(k8sConfig.getMasterUrl(), is("https://" + ENDPOINT + "/"));
    assertThat(k8sConfig.getCaCertData(), is(clusterCaCertificate));
    assertThat(k8sConfig.getClientCertData(), is(clientCertificate));
    assertThat(k8sConfig.getClientKeyData(), is(clientKey));
    assertThat(k8sConfig.getNamespace(), is(TEST_NAMESPACE));
    assertThat(k8sConfig.getRequestTimeout(), is(expectedK8sRequestTimeout));

    assertThat(httpClient.protocols(), contains(Protocol.HTTP_1_1));
  }

  @Test
  public void testGetKubernetesClientNullClusterCaCertificate() throws Exception {
    var clientCertificate = Resources.toString(Resources.getResource("client.crt"), UTF_8);
    var clientKey = Resources.toString(Resources.getResource("client.key"), UTF_8);

    exception.expect(NullPointerException.class);
    getKubernetesClient("", null, clientCertificate, clientKey);
  }

  @Test
  public void testGetKubernetesClientNullClientCertificate() throws Exception {
    var clusterCaCertificate = Resources.toString(Resources.getResource("ca.crt"), UTF_8);
    var clientKey = Resources.toString(Resources.getResource("client.key"), UTF_8);

    exception.expect(NullPointerException.class);
    getKubernetesClient("", clusterCaCertificate, null, clientKey);
  }

  @Test
  public void testGetKubernetesClientNullClientKey() throws Exception {
    var clusterCaCertificate = Resources.toString(Resources.getResource("ca.crt"), UTF_8);
    var clientCertificate = Resources.toString(Resources.getResource("client.crt"), UTF_8);

    exception.expect(NullPointerException.class);
    getKubernetesClient("", clusterCaCertificate, clientCertificate, null);
  }

  private NamespacedKubernetesClient getKubernetesClient(String k8sRequestTimeoutConfig,
                                                         String clusterCaCertificate,
                                                         String clientCertificate,
                                                         String clientKey) throws Exception {
    var project = "test-project";
    var zone = "test-zone";
    var cluster = "test-cluster";

    var configMap = ImmutableMap.<String, String>builder()
        .put("styx.gke.foo.project-id", project)
        .put("styx.gke.foo.cluster-zone", zone)
        .put("styx.gke.foo.cluster-id", cluster)
        .put("styx.gke.foo.namespace", TEST_NAMESPACE);

    if (!k8sRequestTimeoutConfig.isEmpty()) {
      configMap.put("styx.k8s.request-timeout", k8sRequestTimeoutConfig);
    }

    var config = ConfigFactory.parseMap(configMap.build());

    var gkeCluster = new Cluster();
    gkeCluster.setEndpoint(ENDPOINT);

    var masterAuth = new MasterAuth();
    masterAuth.setClusterCaCertificate(clusterCaCertificate);
    masterAuth.setClientCertificate(clientCertificate);
    masterAuth.setClientKey(clientKey);
    gkeCluster.setMasterAuth(masterAuth);

    when(gkeClusterGet.execute()).thenReturn(gkeCluster);

    return StyxScheduler.getKubernetesClient(config, "foo", gkeClient, kubernetesClientFactory);
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
    when(stateManager.getActiveStates()).thenReturn(Map.of(wfi1, rs1, wfi2, rs2));
    when(workflowCache.get()).thenReturn(Map.of(wfid1, wf1, wfid2, wf2));
    when(storage.enabled()).thenReturn(ImmutableSet.of(wfid1));

    StyxScheduler.setupMetrics(stateManager, workflowCache, storage, submissionRateLimiter, stats, time);

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

    // Verify that expensive methods were cached
    verify(stateManager, times(1)).getActiveStates();
    verify(storage, times(1)).enabled();

    verifyNoMoreInteractions(storage);
    verifyNoMoreInteractions(stateManager);
  }

  @Test
  public void testCreateNoopFlyteRunner() {
    var configMap = ImmutableMap.<String, String>builder()
        .put("styx.flyte.enabled", "false");
    var config = ConfigFactory.parseMap(configMap.build());
    final FlyteRunner flyteRunner = StyxScheduler.createFlyteRunner("runnerId", config, stateManager, FlyteAdminClientInterceptors.NOOP, stats);

    assertThat(flyteRunner, instanceOf(NoopFlyteRunner.class));
  }

  @Test
  public void testCreateFlyteRunner() {
    var configMap = ImmutableMap.<String, String>builder()
        .put("styx.flyte.enabled", "true")
        .put("styx.flyte.admin.production.host", "localhost")
        .put("styx.flyte.admin.production.port", "81")
        .put("styx.flyte.admin.production.insecure", "true")
        .put("styx.flyte.admin.production.grpc.deadline-seconds", "5")
            .put("styx.flyte.admin.production.grpc.retry-initial-wait-duration", "5")
            .put("styx.flyte.admin.production.grpc.retry-wait-duration-backoff-multiplier", "2")
        .put("styx.flyte.admin.production.grpc.max-retry-attempts", "3");

    var config = ConfigFactory.parseMap(configMap.build());
    final FlyteRunner flyteRunner = StyxScheduler.createFlyteRunner("production", config, stateManager,
        FlyteAdminClientInterceptors.NOOP, stats);

    assertThat(flyteRunner.isEnabled(), is(true));
  }

  @Test
  public void testCreateFlyteRunnerForMissingRunnerIdConfig() {
    var configMap = ImmutableMap.<String, String>builder()
        .put("styx.flyte.enabled", "true")
        .put("styx.flyte.admin.production.host", "localhost")
        .put("styx.flyte.admin.production.port", "81")
        .put("styx.flyte.admin.production.insecure", "true");
    var config = ConfigFactory.parseMap(configMap.build());

    assertThrows(
        IllegalArgumentException.class,
        () -> StyxScheduler.createFlyteRunner("staging", config, stateManager, FlyteAdminClientInterceptors.NOOP, stats)
    );
  }
}
