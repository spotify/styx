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

import static com.github.npathai.hamcrestopt.OptionalMatchers.hasValue;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.podStatusNoContainer;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.running;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.terminated;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.waiting;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.docker.KubernetesDockerRunner.KubernetesSecretSpec;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.Debug;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.Time;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.DoneableSecret;
import io.fabric8.kubernetes.api.model.ListMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatus;
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
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesDockerRunnerTest {

  private static final String POD_NAME = "test-pod-1";
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

  private static final int NO_POLL = Integer.MAX_VALUE;
  private static final int POD_DELETION_DELAY_SECONDS = 120;
  private static final Instant FIXED_INSTANT = Instant.parse("2017-09-01T01:00:00Z");

  @Mock NamespacedKubernetesClient k8sClient;
  @Mock KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager;
  @Mock MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> pods;
  @Mock MixedOperation<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>> secrets;
  @Mock Resource<Secret, DoneableSecret> namedResource;
  @Mock PodResource<Pod, DoneablePod> namedPod;
  @Mock PodList podList;
  @Mock PodStatus podStatus;
  @Mock ContainerStatus containerStatus;
  @Mock ContainerState containerState;
  @Mock ContainerStateTerminated containerStateTerminated;
  @Mock ListMeta listMeta;
  @Mock Watch watch;
  @Mock Debug debug;
  @Mock Time time;

  @Captor ArgumentCaptor<Watcher<Pod>> watchCaptor;
  @Captor ArgumentCaptor<Pod> podCaptor;

  @Rule public ExpectedException exception = ExpectedException.none();

  Pod createdPod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC, EMPTY_SECRET_SPEC);
  StateManager stateManager = Mockito.spy(new SyncStateManager());
  Stats stats = Mockito.mock(Stats.class);

  KubernetesDockerRunner kdr;
  Watcher<Pod> podWatcher;

  @Before
  public void setUp() throws Exception {
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

    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountSecretManager, debug, NO_POLL,
                                     POD_DELETION_DELAY_SECONDS, time);
    kdr.init();

    podWatcher = watchCaptor.getValue();

    Map<String, String> annotations = new HashMap<>();
    annotations.put(KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION, WORKFLOW_INSTANCE.toKey());
    createdPod.getMetadata().setAnnotations(annotations);
    createdPod.getMetadata().setName(POD_NAME);
    createdPod.getMetadata().setResourceVersion("1001");

    StateData stateData = StateData.newBuilder().executionId(POD_NAME).build();
    stateManager.trigger(RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTED, stateData), trigger);
  }

  @After
  public void tearDown() throws Exception {
    kdr.close();
  }

  @Test
  public void shouldUseExecutionIdForPodName() throws IOException, IsClosedException {
    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC);
    verify(pods).create(podCaptor.capture());
    Pod submittedPod = podCaptor.getValue();
    assertThat(submittedPod.getMetadata().getName(), is(RUN_SPEC.executionId()));
  }

  @Test
  public void shouldNotDeletePodIfDebugEnabled() throws Exception {
    when(debug.get()).thenReturn(true);
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(KubernetesDockerRunner.STYX_RUN);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(5)).toString());

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, name);
    verify(k8sClient.pods(), never()).delete(any(Pod.class));
    verify(k8sClient.pods(), never()).delete(any(Pod[].class));
    verify(k8sClient.pods(), never()).delete(anyListOf(Pod.class));
    verify(k8sClient.pods(), never()).delete();
    verify(namedPod, never()).delete();
  }

  @Test
  public void shouldCleanupPodAfterNonDeletePeriod() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(KubernetesDockerRunner.STYX_RUN);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(5)).toString());

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, name);
    verify(namedPod).delete();
  }

  @Test
  public void shouldCleanupPodWhenMissingFinishedAt() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(KubernetesDockerRunner.STYX_RUN);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, name);
    verify(namedPod).delete();
  }

  @Test
  public void shouldCleanupPodWhenMissingContainerStatus() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, name);
    verify(namedPod).delete();
  }

  @Test
  public void shouldCleanupPodWhenPullImageError() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(waiting("Pending", "ErrImagePull"));

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, name);
    verify(namedPod).delete();
  }

  @Test
  public void shouldNotCleanupPodBeforeNonDeletePeriod() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(KubernetesDockerRunner.STYX_RUN);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(1)).toString());

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, name);
    verify(namedPod, never()).delete();
  }

  @Test
  public void shouldNotCleanupPodIfNotTerminated() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(KubernetesDockerRunner.STYX_RUN);
    when(containerStatus.getState()).thenReturn(containerState);

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, name);
    verify(namedPod, never()).delete();
  }

  @Test
  public void shouldNotCleanupPodIfNotStyxContainer() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn("foo");

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, name);
    verify(namedPod, never()).delete();
  }

  @Test
  public void shouldNotCleanupPodWithoutRunStateIfNotStyxContainer() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn("foo");

    kdr.cleanupWithoutRunState(WORKFLOW_INSTANCE, name);
    verify(namedPod, never()).delete();
  }

  @Test
  public void shouldCleanupPodWithoutRunStateIfNotTerminated() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(KubernetesDockerRunner.STYX_RUN);
    when(containerStatus.getState()).thenReturn(containerState);

    kdr.cleanupWithoutRunState(WORKFLOW_INSTANCE, name);
    verify(namedPod).delete();
  }

  @Test
  public void shouldNotCleanupPodWithoutRunStateBeforeNonDeletePeriod() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(KubernetesDockerRunner.STYX_RUN);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(1)).toString());

    kdr.cleanupWithoutRunState(WORKFLOW_INSTANCE, name);
    verify(namedPod, never()).delete();
  }

  @Test
  public void shouldCleanupPodWithoutRunStateAfterNonDeletePeriod() {
    final String name = createdPod.getMetadata().getName();
    when(k8sClient.pods().withName(name)).thenReturn(namedPod);
    when(namedPod.get()).thenReturn(createdPod);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(KubernetesDockerRunner.STYX_RUN);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(5)).toString());

    kdr.cleanupWithoutRunState(WORKFLOW_INSTANCE, name);
    verify(namedPod).delete();
  }

  @Test(expected = InvalidExecutionException.class)
  public void shouldThrowIfSecretNotExist() throws IOException, IsClosedException {
    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(null);
    when(k8sClient.secrets()).thenReturn(secrets);

    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SECRET);
  }

  @Test(expected = InvalidExecutionException.class)
  public void shouldThrowIfMountToReservedPath() throws IOException, IsClosedException {
    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(null);
    when(k8sClient.secrets()).thenReturn(secrets);

    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SECRET_AND_SA);
  }

  @Test
  public void shouldMountSecret() throws IOException, IsClosedException {
    final Pod pod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SECRET,
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
  public void shouldMountServiceAccount() throws IOException, IsClosedException {
    final Pod pod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA, SECRET_SPEC_WITH_SA);
    assertThat(pod.getSpec().getVolumes().size(), is(1));
    assertThat(pod.getSpec().getVolumes().get(0).getName(),
               is(KubernetesDockerRunner.STYX_WORKFLOW_SA_SECRET_NAME));
    assertThat(pod.getSpec().getContainers().size(), is(1));
    assertThat(
        pod.getSpec().getContainers().get(0).getEnv().stream()
            .anyMatch(e -> e.getName()
                .equals(KubernetesDockerRunner.STYX_WORKFLOW_SA_ENV_VARIABLE)),
        is(true));
  }

  @Test
  public void shouldConfigureResourceRequirements() throws IOException, IsClosedException {
    final String memRequest = "17Mi";
    final String memLimit = "4711Mi";
    final Pod pod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RunSpec.builder()
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
  public void shouldRunIfSecretExists() throws IOException, IsClosedException {
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
  public void shouldEnsureAndMountServiceAccountSecret() throws IsClosedException, IOException {
    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(null);
    when(k8sClient.secrets()).thenReturn(secrets);

    when(serviceAccountSecretManager.ensureServiceAccountKeySecret(
        WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT)).thenReturn(SERVICE_ACCOUNT_SECRET);

    StateData stateData = StateData.newBuilder().executionId(POD_NAME).build();
    stateManager.trigger(RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTED, stateData), trigger);

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
  public void shouldNotRunIfServiceAccountSecretEnsureFails() throws IsClosedException, IOException {
    final InvalidExecutionException error = new InvalidExecutionException("SA not found");
    when(serviceAccountSecretManager.ensureServiceAccountKeySecret(
        WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT)).thenThrow(error);

    exception.expect(is(error));

    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA);
  }

  @Test
  public void shouldNotRunIfSecretHasManagedServiceAccountKeySecretNamePrefix() throws
                                                                                IsClosedException, IOException {
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

  @Test
  public void shouldCompleteWithStatusCodeOnSucceeded() throws Exception {
    createdPod.setStatus(terminated("Succeeded", 20, null));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    assertThat(stateManager.get(WORKFLOW_INSTANCE).data().lastExit(), hasValue(20));
  }

  @Test
  public void shouldFailOnErrImagePull() throws Exception {
    createdPod.setStatus(waiting("Pending", "ErrImagePull"));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    assertThat(stateManager.get(WORKFLOW_INSTANCE).state(), is(RunState.State.FAILED));
  }

  @Test
  public void shouldSendStatsOnErrImagePull() throws Exception {
    createdPod.setStatus(waiting("Pending", "ErrImagePull"));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    verify(stats, times(1)).recordPullImageError();
    assertThat(stateManager.get(WORKFLOW_INSTANCE).state(), is(RunState.State.FAILED));
  }

  @Test
  public void shouldNotSendStatsOnOtherError() throws Exception {
    createdPod.setStatus(podStatusNoContainer("Succeeded"));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    verifyNoMoreInteractions(stats);
    assertThat(stateManager.get(WORKFLOW_INSTANCE).state(), is(RunState.State.FAILED));
  }

  @Test
  public void shouldFailOnUnknownPhaseEntered() throws Exception {
    createdPod.setStatus(podStatusNoContainer("Unknown"));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    assertThat(stateManager.get(WORKFLOW_INSTANCE).state(), is(RunState.State.FAILED));
  }

  @Test
  public void shouldIgnoreDeletedEvents() throws Exception {
    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC);
    stateManager.receive(Event.started(WORKFLOW_INSTANCE));

    podWatcher.eventReceived(Watcher.Action.DELETED, createdPod);

    assertThat(stateManager.get(WORKFLOW_INSTANCE).state(), is(RunState.State.RUNNING));
  }

  @Test
  public void shouldFailOnMissingContainer() throws Exception {
    createdPod.setStatus(podStatusNoContainer("Succeeded"));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    assertThat(stateManager.get(WORKFLOW_INSTANCE).state(), is(RunState.State.FAILED));
  }

  @Test
  public void shouldFailOnUnexpectedTerminatedStatus() throws Exception {
    createdPod.setStatus(waiting("Failed", ""));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    assertThat(stateManager.get(WORKFLOW_INSTANCE).state(), is(RunState.State.FAILED));
  }

  @Test
  public void shouldGenerateStartedAndRecordSubmitToRunningTimeWhenContainerIsReady() throws Exception {
    when(time.nanoTime()).thenReturn(TimeUnit.SECONDS.toNanos(17));
    kdr.start(WORKFLOW_INSTANCE, RunSpec.simple(POD_NAME, "busybox"));
    verify(stats).recordSubmission(POD_NAME);

    when(time.nanoTime()).thenReturn(TimeUnit.SECONDS.toNanos(18));
    StateData stateData = StateData.newBuilder().executionId(POD_NAME).build();
    stateManager.trigger(RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTED, stateData), trigger);

    when(time.nanoTime()).thenReturn(TimeUnit.SECONDS.toNanos(19));
    createdPod.setStatus(running(/* ready= */ false));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);
    assertThat(stateManager.get(WORKFLOW_INSTANCE).state(), is(RunState.State.SUBMITTED));

    when(time.nanoTime()).thenReturn(TimeUnit.SECONDS.toNanos(4711));
    createdPod.setStatus(running(/* ready= */ true));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);
    assertThat(stateManager.get(WORKFLOW_INSTANCE).state(), is(RunState.State.RUNNING));

    verify(stats).recordRunning(POD_NAME);
  }

  @Test
  public void shouldDiscardChangesForOldExecutions() throws Exception {
    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC);
    stateManager.receive(Event.started(WORKFLOW_INSTANCE));

    // simulate event from different pod, but still with the same workflow instance annotation
    createdPod.getMetadata().setName(POD_NAME + "-other");
    createdPod.setStatus(terminated("Succeeded", 20, null));

    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    assertThat(stateManager.get(WORKFLOW_INSTANCE).state(), is(RunState.State.RUNNING));
  }

  @Test
  public void shouldPollPodStatusAndEmitEventsOnRestore() throws Exception {
    when(k8sClient.pods().withName(createdPod.getMetadata().getName())).thenReturn(namedPod);

    // Stop the runner and change the pod status to terminated while styx is "down"
    kdr.close();
    createdPod.setStatus(terminated("Succeeded", 20, null));

    // Start a new runner
    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountSecretManager,
        debug, NO_POLL, 0, time);
    kdr.init();

    // Make the runner poll states for all pods
    kdr.restore();

    // Verify that the runner polled and found out that the pods is terminated
    verify(stateManager).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(20)));
    assertThat(stateManager.get(WORKFLOW_INSTANCE).data().lastExit(), hasValue(20));
  }

  @Test
  public void shouldRegularlyPollPodStatusAndEmitEvents() throws Exception {
    when(k8sClient.pods().withName(createdPod.getMetadata().getName())).thenReturn(namedPod);

    createdPod.setStatus(running(/* ready= */ true));

    // Set up a runner with short poll interval to avoid this test having to wait a long time for the poll
    kdr.close();
    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountSecretManager,
        debug, 1, 0, time);
    kdr.init();
    kdr.restore();

    // Change the pod status to terminated without notifying the runner through the pod watcher
    final Pod terminatedPod = new PodBuilder(createdPod)
        .withStatus(terminated("Succeeded", 20, null))
        .build();
    when(podList.getItems()).thenReturn(ImmutableList.of(terminatedPod));

    // Verify that the runner eventually polls and finds out that the pod is terminated
    verify(stateManager, timeout(30_000)).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(20)));
    await().timeout(30, SECONDS).until(() ->
        assertThat(stateManager.get(WORKFLOW_INSTANCE).data().lastExit(), hasValue(20)));
  }
}
