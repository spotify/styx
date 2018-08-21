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

import static com.spotify.styx.docker.KubernetesDockerRunner.KEEPALIVE_CONTAINER_NAME;
import static com.spotify.styx.docker.KubernetesDockerRunner.MAIN_CONTAINER_NAME;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.podStatusNoContainer;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.setRunning;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.setTerminated;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.setWaiting;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.terminated;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.terminatedContainerState;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.styx.QuietDeterministicScheduler;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.docker.KubernetesDockerRunner.KubernetesSecretSpec;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.Debug;
import com.spotify.styx.util.Time;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.DoneableSecret;
import io.fabric8.kubernetes.api.model.ListMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.Watcher.Action;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
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
public class KubernetesDockerRunnerTest {

  private static final String EXECUTION_ID = "badf00d";
  private static final String POD_NAME = EXECUTION_ID;
  private static final String SERVICE_ACCOUNT = "sa@example.com";
  private static final String SERVICE_ACCOUNT_SECRET = "sa-secret";
  private static final WorkflowInstance WORKFLOW_INSTANCE = WorkflowInstance.create(TestData.WORKFLOW_ID, "foo");
  private static final RunSpec RUN_SPEC = RunSpec.simple("eid0", "busybox");
  private static final RunSpec RUN_SPEC_WITH_SECRET = RunSpec.builder()
      .executionId("eid1")
      .imageName("busybox")
      .secret(WorkflowConfiguration.Secret.create("secret1", "/etc/secret"))
      .build();
  private static final RunSpec RUN_SPEC_WITH_SA = RunSpec.builder()
      .executionId("eid3")
      .imageName("busybox")
      .serviceAccount(SERVICE_ACCOUNT)
      .build();

  private static final KubernetesSecretSpec SECRET_SPEC_WITH_SA = KubernetesSecretSpec.builder()
      .serviceAccountSecret(SERVICE_ACCOUNT_SECRET)
      .build();

  private static final KubernetesSecretSpec SECRET_SPEC_WITH_CUSTOM_SECRET = KubernetesSecretSpec.builder()
      .customSecret(RUN_SPEC_WITH_SECRET.secret())
      .build();

  private static final KubernetesSecretSpec EMPTY_SECRET_SPEC = KubernetesSecretSpec.builder()
      .build();

  private static final RunSpec RUN_SPEC_WITH_SECRET_AND_SA = RunSpec.builder()
      .executionId("eid")
      .imageName("busybox")
      .secret(WorkflowConfiguration.Secret.create("secret1", KubernetesDockerRunner.STYX_WORKFLOW_SA_SECRET_MOUNT_PATH))
      .serviceAccount(SERVICE_ACCOUNT)
      .build();

  private static final int POLL_INTERVAL_SECONDS = 60;
  private static final int POD_DELETION_DELAY_SECONDS = 120;
  private static final Instant FIXED_INSTANT = Instant.parse("2017-09-01T01:00:00Z");

  private static final String STYX_ENVIRONMENT = "testing";

  @Mock private NamespacedKubernetesClient k8sClient;
  @Mock private KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager;
  @Mock private MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> pods;
  @Mock private MixedOperation<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>> secrets;
  @Mock private Resource<Secret, DoneableSecret> namedResource;
  @Mock private PodResource<Pod, DoneablePod> namedPod;
  @Mock private PodList podList;
  @Mock private PodStatus podStatus;
  @Mock private ContainerStatus containerStatus;
  @Mock private ContainerState containerState;
  @Mock private ContainerStateTerminated containerStateTerminated;
  @Mock private ListMeta listMeta;
  @Mock private Watch watch;
  @Mock private Debug debug;
  @Mock private Time time;
  @Mock private StateManager stateManager;

  @Captor private ArgumentCaptor<Watcher<Pod>> watchCaptor;
  @Captor private ArgumentCaptor<Pod> podCaptor;

  @Rule public ExpectedException exception = ExpectedException.none();

  private Pod createdPod = createPod(WORKFLOW_INSTANCE, RUN_SPEC, EMPTY_SECRET_SPEC);
  private Stats stats = mock(Stats.class);

  private KubernetesDockerRunner kdr;
  private Watcher<Pod> podWatcher;
  private QuietDeterministicScheduler executor = new QuietDeterministicScheduler();
  private ContainerStatus keepaliveContainerStatus = new ContainerStatusBuilder()
      .withName(KEEPALIVE_CONTAINER_NAME)
      .withNewState().withNewRunning().endRunning().endState()
      .build();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    when(debug.get()).thenReturn(false);

    when(k8sClient.inNamespace(any(String.class))).thenReturn(k8sClient);
    when(k8sClient.pods()).thenReturn(pods);

    when(pods.list()).thenReturn(podList);
    when(podList.getItems()).thenReturn(ImmutableList.of(createdPod));
    when(podList.getMetadata()).thenReturn(listMeta);
    when(listMeta.getResourceVersion()).thenReturn("1000");

    when(pods.watch(watchCaptor.capture())).thenReturn(watch);

    when(serviceAccountSecretManager.ensureServiceAccountKeySecret(
        WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT))
        .thenReturn(SERVICE_ACCOUNT_SECRET);

    when(time.get()).thenReturn(FIXED_INSTANT);

    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountSecretManager,
        debug, STYX_ENVIRONMENT, POLL_INTERVAL_SECONDS, POD_DELETION_DELAY_SECONDS, time, executor);
    kdr.init();

    podWatcher = watchCaptor.getValue();

    Map<String, String> annotations = new HashMap<>();
    annotations.put(KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION, WORKFLOW_INSTANCE.toKey());
    createdPod.getMetadata().setAnnotations(annotations);
    createdPod.getMetadata().setName(POD_NAME);
    createdPod.getMetadata().setResourceVersion("1001");

    StateData stateData = StateData.newBuilder().executionId(POD_NAME).build();
    RunState runState = RunState.create(WORKFLOW_INSTANCE, State.SUBMITTED, stateData);

    when(stateManager.getActiveStates()).thenReturn(ImmutableMap.of(WORKFLOW_INSTANCE, runState));
    when(stateManager.getActiveState(WORKFLOW_INSTANCE)).thenReturn(Optional.of(runState));
  }

  @After
  public void tearDown() throws Exception {
    kdr.close();
  }

  @Test
  public void shouldUseExecutionIdForPodName() throws IOException {
    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC);
    verify(pods).create(podCaptor.capture());
    Pod submittedPod = podCaptor.getValue();
    assertThat(submittedPod.getMetadata().getName(), is(RUN_SPEC.executionId()));
  }

  @Test
  public void shouldCreateMainContainerAndKeepaliveContainer() throws IOException {
    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC);
    verify(pods).create(podCaptor.capture());
    Pod submittedPod = podCaptor.getValue();
    assertThat(submittedPod.getSpec().getContainers().size(), is(2));
    final Container mainContainer = submittedPod.getSpec().getContainers().get(0);
    final Container keepaliveContainer = submittedPod.getSpec().getContainers().get(1);
    assertThat(mainContainer.getName(), is(MAIN_CONTAINER_NAME));
    assertThat(keepaliveContainer.getName(), is(KEEPALIVE_CONTAINER_NAME));
    assertThat(keepaliveContainer.getVolumeMounts(), is(empty()));
  }

  @Test
  public void shouldNotDeletePodIfDebugEnabled() {
    when(debug.get()).thenReturn(true);
    when(pods.withName(anyString())).thenReturn(namedPod);

    kdr.deletePod(WORKFLOW_INSTANCE, createdPod, "Foobar!");
    verifyPodNeverDeleted(namedPod);
  }

  @Parameters({"TERMINATED", "FAILED", "ERROR", "DONE"})
  @Test
  public void shouldCleanupPodAfterNonDeletePeriodIfRunStateNotRunning(String stateName) {
    final State state = State.valueOf(stateName);
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(keepaliveContainerStatus, containerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(5)).toString());

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, createdPod, RunState.create(WORKFLOW_INSTANCE, state));
    verify(namedPod).delete();
  }

  @Parameters({"NEW", "QUEUED", "PREPARE", "SUBMITTING", "SUBMITTED", "RUNNING"})
  @Test
  public void shouldNotDeletePodAfterNonDeletePeriodIfRunStateStillRunning(String stateName) {
    final State state = State.valueOf(stateName);
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(5)).toString());

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, createdPod, RunState.create(WORKFLOW_INSTANCE, state));
    verifyPodNeverDeleted(namedPod);
  }

  @Test
  public void shouldCleanupPodWhenMissingFinishedAt() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, createdPod, RunState.create(WORKFLOW_INSTANCE, State.TERMINATED));
    verify(namedPod).delete();
  }

  @Test
  public void shouldNotCleanupPodWhenMissingContainerStatus() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, createdPod, RunState.create(WORKFLOW_INSTANCE, State.TERMINATED));

    // It is normal for a pod to not have any container status for a while after creation
    verifyPodNeverDeleted(namedPod);
  }

  @Test
  public void shouldCleanupPodWhenPullImageError() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    setWaiting(createdPod, "Pending", "ErrImagePull");

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, createdPod, RunState.create(WORKFLOW_INSTANCE, State.TERMINATED));
    verify(namedPod).delete();
  }

  @Test
  public void shouldNotCleanupPodBeforeNonDeletePeriod() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(1)).toString());

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, createdPod, RunState.create(WORKFLOW_INSTANCE, State.TERMINATED));
    verifyPodNeverDeleted(namedPod);
  }

  @Test
  public void shouldNotCleanupPodIfNotTerminated() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, createdPod, RunState.create(WORKFLOW_INSTANCE, State.TERMINATED));
    verifyPodNeverDeleted(namedPod);
  }

  @Test
  public void shouldNotCleanupNonStyxPod() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    createdPod.getMetadata().setAnnotations(Collections.emptyMap());

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus, keepaliveContainerStatus));

    when(stateManager.getActiveStates()).thenReturn(Collections.emptyMap());

    kdr.tryPollPods();

    verifyPodNeverDeleted(namedPod);
  }

  @Test
  public void shouldNotCleanupNonStyxPodWithoutRunState() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    createdPod.getMetadata().setAnnotations(Collections.emptyMap());

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus, keepaliveContainerStatus));

    when(stateManager.getActiveStates()).thenReturn(Collections.emptyMap());

    kdr.tryPollPods();

    verifyPodNeverDeleted(namedPod);
  }

  @Test
  public void shouldCleanupPodWithoutRunStateIfNotTerminated() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);

    kdr.cleanupWithoutRunState(WORKFLOW_INSTANCE, createdPod);
    verify(namedPod).delete();
  }

  @Test
  public void shouldNotCleanupTerminatedPodWithoutRunStateBeforeNonDeletePeriod() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(1)).toString());

    kdr.cleanupWithoutRunState(WORKFLOW_INSTANCE, createdPod);
    verifyPodNeverDeleted(namedPod);
  }

  @Test
  public void shouldNotCleanupRunningPodWithoutRunStateIfTerminatedAfterRefresh() {
    final String name = createdPod.getMetadata().getName();

    final ContainerStatus runningMainContainer = new ContainerStatusBuilder()
        .withName(MAIN_CONTAINER_NAME)
        .withNewState().withNewRunning().endRunning().endState()
        .build();

    final ContainerStatus terminatedMainContainer = new ContainerStatusBuilder()
        .withName(MAIN_CONTAINER_NAME)
        .withNewState()
        .withNewTerminated()
        .withFinishedAt(FIXED_INSTANT.minus(Duration.ofDays(1)).toString())
        .endTerminated()
        .endState()
        .build();

    createdPod.setStatus(new PodStatusBuilder()
        .withContainerStatuses(runningMainContainer, keepaliveContainerStatus)
        .build());

    final Pod refreshedPod = new PodBuilder(createdPod)
        .withStatus(new PodStatusBuilder()
            .withContainerStatuses(terminatedMainContainer, keepaliveContainerStatus)
            .build())
        .build();

    // Return terminated container when refreshing by name
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(refreshedPod);

    kdr.cleanupWithoutRunState(WORKFLOW_INSTANCE, createdPod);

    verify(pods).withName(EXECUTION_ID);
    verify(namedPod).get();

    // Leave the pod to expire and be deleted by a later poll tick
    verifyPodNeverDeleted(namedPod);
  }

  @Test
  public void shouldCleanupPodWithoutRunStateAfterNonDeletePeriod() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(5)).toString());

    kdr.cleanupWithoutRunState(WORKFLOW_INSTANCE, createdPod);
    verify(namedPod).delete();
  }

  @Test(expected = InvalidExecutionException.class)
  public void shouldThrowIfSecretNotExist() throws IOException {
    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(null);
    when(k8sClient.secrets()).thenReturn(secrets);

    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SECRET);
  }

  @Test(expected = InvalidExecutionException.class)
  public void shouldThrowIfMountToReservedPath() throws IOException {
    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(null);
    when(k8sClient.secrets()).thenReturn(secrets);

    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SECRET_AND_SA);
  }

  @Test
  public void shouldMountSecret() {
    final Pod pod = createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SECRET,
        SECRET_SPEC_WITH_CUSTOM_SECRET);
    assertThat(pod.getSpec().getVolumes().size(), is(1));
    assertThat(pod.getSpec().getVolumes().get(0).getName(),
               is(RUN_SPEC_WITH_SECRET.secret().get().name()));
    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(),
               is(RUN_SPEC_WITH_SECRET.secret().get().mountPath()));
    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(),
               is(RUN_SPEC_WITH_SECRET.secret().get().name()));
  }

  @Test
  public void shouldMountServiceAccount() {
    final Pod pod = createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA, SECRET_SPEC_WITH_SA);
    assertThat(pod.getSpec().getVolumes().size(), is(1));
    assertThat(pod.getSpec().getVolumes().get(0).getName(),
               is(KubernetesDockerRunner.STYX_WORKFLOW_SA_SECRET_NAME));
    assertThat(pod.getSpec().getContainers().size(), is(2));
    final Container mainContainer = pod.getSpec().getContainers().get(0);
    assertThat(mainContainer.getEnv().stream()
            .anyMatch(e -> e.getName().equals(KubernetesDockerRunner.STYX_WORKFLOW_SA_ENV_VARIABLE)),
        is(true));
  }

  @Test
  public void shouldConfigureResourceRequirements() {
    final String memRequest = "17Mi";
    final String memLimit = "4711Mi";
    final Pod pod = createPod(WORKFLOW_INSTANCE, RunSpec.builder()
        .executionId("eid1")
        .imageName("busybox")
        .memRequest(memRequest)
        .memLimit(memLimit)
        .build(),
        EMPTY_SECRET_SPEC);

    final ResourceRequirements resourceReqs = pod.getSpec().getContainers().get(0).getResources();
    assertThat(resourceReqs.getRequests().get("memory"), is(new Quantity(memRequest)));
    assertThat(resourceReqs.getLimits().get("memory"), is(new Quantity(memLimit)));
  }

  @Test
  public void shouldRunIfSecretExists() throws IOException {
    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(new SecretBuilder().build());
    when(k8sClient.secrets()).thenReturn(secrets);

    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SECRET);

    verify(pods).create(podCaptor.capture());
  }

  @Test
  public void shouldCleanupServiceAccountSecrets() throws Exception {
    kdr.cleanup();
    verify(serviceAccountSecretManager).cleanup();
  }

  @Test
  public void shouldEnsureAndMountServiceAccountSecret() throws IOException {
    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(null);
    when(k8sClient.secrets()).thenReturn(secrets);

    when(serviceAccountSecretManager.ensureServiceAccountKeySecret(
        WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT)).thenReturn(SERVICE_ACCOUNT_SECRET);

    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA);

    verify(serviceAccountSecretManager).ensureServiceAccountKeySecret(
        WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT);

    verify(pods).create(podCaptor.capture());

    final Pod pod = podCaptor.getValue();

    final Optional<SecretVolumeSource> serviceAccountSecretVolume = pod.getSpec().getVolumes().stream()
        .map(Volume::getSecret)
        .filter(Objects::nonNull)
        .filter(v -> SERVICE_ACCOUNT_SECRET.equals(v.getSecretName()))
        .findAny();

    assertThat(serviceAccountSecretVolume.isPresent(), is(true));
  }

  @Test
  public void shouldNotRunIfServiceAccountSecretEnsureFails() throws IOException {
    final InvalidExecutionException error = new InvalidExecutionException("SA not found");
    when(serviceAccountSecretManager.ensureServiceAccountKeySecret(
        WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT)).thenThrow(error);

    exception.expect(is(error));

    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA);
  }

  @Test
  public void shouldNotRunIfSecretHasManagedServiceAccountKeySecretNamePrefix() throws
      IOException {
    final String secret = "styx-wf-sa-keys-foo";

    exception.expect(InvalidExecutionException.class);
    exception.expectMessage("Referenced secret '" + secret + "' has the managed service account key secret name prefix");
    kdr.start(WORKFLOW_INSTANCE, RunSpec.builder()
        .executionId("eid")
        .imageName("busybox")
        .secret(WorkflowConfiguration.Secret.create(secret, "/foo/bar"))
        .build());

    verify(pods, never()).create(any(Pod.class));
  }

  @Parameters({
      "Running,   20, true",
      "Running,   1,  true",
      "Running,   0,  true",
      "Succeeded, 20, true",
      "Succeeded, 1,  true",
      "Succeeded, 0,  true",
      "Failed,    20, true",
      "Failed,    1,  true",
      "Running,   20, false",
      "Running,   1,  false",
      "Running,   0,  false",
      "Succeeded, 20, false",
      "Succeeded, 1,  false",
      "Succeeded, 0,  false",
      "Failed,    20, false",
      "Failed,    1,  false"
  })
  @Test
  public void shouldCompleteWithStatusCodeOnMainContainerTerminated(String phase, int code,
      boolean withKeepaliveContainer) throws Exception {
    final String executionId = createdPod.getMetadata().getName();

    final PodStatus podStatus = podStatusNoContainer(phase);

    podStatus.getContainerStatuses()
        .add(new ContainerStatusBuilder()
            .withState(terminatedContainerState(code, ""))
            .withName(MAIN_CONTAINER_NAME)
            .build());

    // Verify that old pods without a keepalive container are also correctly handled
    if (withKeepaliveContainer) {
      podStatus.getContainerStatuses()
          .add(new ContainerStatusBuilder()
              .withName(KEEPALIVE_CONTAINER_NAME)
              .withNewState().withNewRunning().endRunning().endState()
              .build());
    }

    createdPod.setStatus(podStatus);
    receiveAndProcessEvent(Watcher.Action.MODIFIED, createdPod);

    verify(stateManager).receive(Event.started(WORKFLOW_INSTANCE), -1);
    verify(stateManager).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(code)), 0);
  }

  @Test
  public void shouldFailOnErrImagePull() throws Exception {
    setWaiting(createdPod, "Pending", "ErrImagePull");
    receiveAndProcessEvent(Watcher.Action.MODIFIED, createdPod);

    verify(stateManager).receive(
        Event.runError(WORKFLOW_INSTANCE, "One or more containers failed to pull their image"),
        -1);
  }

  @Test
  public void shouldSendStatsOnErrImagePull() throws Exception {
    setWaiting(createdPod, "Pending", "ErrImagePull");
    receiveAndProcessEvent(Watcher.Action.MODIFIED, createdPod);

    verify(stats, times(1)).recordPullImageError();
    verify(stateManager).receive(
        Event.runError(WORKFLOW_INSTANCE, "One or more containers failed to pull their image"),
        -1);
  }

  @Test
  public void shouldNotSendStatsOnOtherError() {
    createdPod.setStatus(podStatusNoContainer("Succeeded"));
    receiveAndProcessEvent(Watcher.Action.MODIFIED, createdPod);

    verifyNoMoreInteractions(stats);
  }

  @Test
  public void shouldFailOnUnknownPhaseEntered() throws Exception {
    createdPod.setStatus(podStatusNoContainer("Unknown"));
    receiveAndProcessEvent(Watcher.Action.MODIFIED, createdPod);
    verify(stateManager).receive(Event.runError(WORKFLOW_INSTANCE, "Pod entered Unknown phase"),
        -1);
  }

  @Test
  public void shouldIgnoreDeletedEvents() throws Exception {
    createdPod.setStatus(podStatusNoContainer("Succeeded"));
    receiveAndProcessEvent(Watcher.Action.DELETED, createdPod);

    verify(stateManager, never()).receive(any(), anyLong());
  }

  @Test
  public void shouldFailOnMissingContainer() throws Exception {
    createdPod.setStatus(podStatusNoContainer("Succeeded"));
    receiveAndProcessEvent(Watcher.Action.MODIFIED, createdPod);

    verify(stateManager).receive(
        Event.runError(WORKFLOW_INSTANCE, "Could not find our container in pod"),
        -1);
  }

  @Test
  public void shouldFailOnUnexpectedTerminatedStatus() throws Exception {
    setWaiting(createdPod, "Failed", "");
    receiveAndProcessEvent(Watcher.Action.MODIFIED, createdPod);

    verify(stateManager).receive(
        Event.runError(WORKFLOW_INSTANCE, "Unexpected null terminated status"),
        -1);
  }

  @Test
  public void shouldGenerateStartedAndRecordSubmitToRunningTimeWhenContainerIsReady() throws Exception {
    when(time.nanoTime()).thenReturn(TimeUnit.SECONDS.toNanos(17));
    kdr.start(WORKFLOW_INSTANCE, RunSpec.simple(POD_NAME, "busybox"));
    verify(stats).recordSubmission(POD_NAME);

    when(time.nanoTime()).thenReturn(TimeUnit.SECONDS.toNanos(18));

    when(time.nanoTime()).thenReturn(TimeUnit.SECONDS.toNanos(19));
    setRunning(createdPod, /* ready= */ false);
    receiveAndProcessEvent(Watcher.Action.MODIFIED, createdPod);
    verify(stateManager, never()).receive(Event.started(WORKFLOW_INSTANCE), -1);

    when(time.nanoTime()).thenReturn(TimeUnit.SECONDS.toNanos(4711));
    setRunning(createdPod, /* ready= */ true);
    receiveAndProcessEvent(Watcher.Action.MODIFIED, createdPod);
    verify(stateManager).receive(Event.started(WORKFLOW_INSTANCE), -1);

    verify(stats).recordRunning(POD_NAME);
  }

  @Test
  public void shouldDiscardChangesForOldExecutions() throws Exception {
    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC);

    // simulate event from different pod, but still with the same workflow instance annotation
    createdPod.getMetadata().setName(POD_NAME + "-other");
    setTerminated(createdPod, "Succeeded", 20, null);

    receiveAndProcessEvent(Watcher.Action.MODIFIED, createdPod);

    verify(stateManager, never()).receive(any(), anyLong());
  }

  @Test
  public void shouldPollPodStatusAndEmitEventsOnRestore() throws Exception {
    when(k8sClient.pods().withName(createdPod.getMetadata().getName())).thenReturn(namedPod);

    // Stop the runner and change the pod status to terminated while styx is "down"
    kdr.close();
    setTerminated(createdPod, "Succeeded", 20, null);

    // Start a new runner
    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountSecretManager,
        debug, STYX_ENVIRONMENT, POLL_INTERVAL_SECONDS, 0, time, executor);
    kdr.init();

    // Make the runner poll states for all pods
    kdr.restore();

    // Verify that the runner polled and found out that the pods is terminated
    verify(stateManager).receive(Event.started(WORKFLOW_INSTANCE), -1);
    verify(stateManager).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(20)), 0);
  }

  @Test
  public void shouldRegularlyPollPodStatusAndEmitEvents() throws Exception {
    when(k8sClient.pods().withName(createdPod.getMetadata().getName())).thenReturn(namedPod);

    setRunning(createdPod, /* ready= */ true);

    // Change the pod status to terminated without notifying the runner through the pod watcher
    final Pod terminatedPod = new PodBuilder(createdPod)
        .withStatus(terminated("Succeeded", 20, null))
        .build();
    when(podList.getItems()).thenReturn(ImmutableList.of(terminatedPod));

    // Make time pass so the runner polls
    executor.tick(POLL_INTERVAL_SECONDS, TimeUnit.SECONDS);

    // Verify that the runner eventually polls and finds out that the pod is terminated
    verify(stateManager, timeout(30_000)).receive(
        Event.started(WORKFLOW_INSTANCE),
        -1);
    verify(stateManager, timeout(30_000)).receive(
        Event.terminate(WORKFLOW_INSTANCE, Optional.of(20)),
        0);
  }

  @Test
  public void shouldRecognizeMainContainer() {
    assertThat(KubernetesDockerRunner.isMainContainer(MAIN_CONTAINER_NAME, createdPod), is(true));
    assertThat(KubernetesDockerRunner.isMainContainer(KEEPALIVE_CONTAINER_NAME, createdPod), is(false));
    assertThat(KubernetesDockerRunner.isMainContainer("foobar", createdPod), is(false));
  }

  private void verifyPodNeverDeleted(PodResource<Pod, DoneablePod> pod) {
    verify(k8sClient.pods(), never()).delete(any(Pod.class));
    verify(k8sClient.pods(), never()).delete(any(Pod[].class));
    verify(k8sClient.pods(), never()).delete(anyListOf(Pod.class));
    verify(k8sClient.pods(), never()).delete();
    verify(pod, never()).delete();
  }

  /**
   * Helper to deal with asynchronous pod even handling
   */
  private void receiveAndProcessEvent(Action action, Pod pod) {
    @SuppressWarnings("unchecked") PodResource<Pod, DoneablePod> namedPod = mock(PodResource.class);
    when(namedPod.get()).thenReturn(pod);
    when(pods.withName(pod.getMetadata().getName())).thenReturn(namedPod);
    podWatcher.eventReceived(action, pod);
    executor.tick(10, TimeUnit.SECONDS);
  }

  private static Pod createPod(WorkflowInstance workflowInstance,
                               DockerRunner.RunSpec runSpec,
                               KubernetesSecretSpec secretSpec) {
    return KubernetesDockerRunner
        .createPod(workflowInstance, runSpec, secretSpec, STYX_ENVIRONMENT);
  }
}
