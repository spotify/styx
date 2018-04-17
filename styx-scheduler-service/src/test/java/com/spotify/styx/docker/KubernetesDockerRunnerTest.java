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

import static com.spotify.styx.docker.KubernetesDockerRunnerTestUtil.getJobName;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.podStatusNoContainer;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.running;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.terminated;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.waiting;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.DoneableJob;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.DoneableSecret;
import io.fabric8.kubernetes.api.model.Job;
import io.fabric8.kubernetes.api.model.JobList;
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
import io.fabric8.kubernetes.client.dsl.ExtensionsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
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
  private static final String CONTAINER_NAME = "test-container-1";
  private static final String SERVICE_ACCOUNT = "sa@example.com";
  private static final String SERVICE_ACCOUNT_SECRET = "sa-secret";
  private static final WorkflowInstance WORKFLOW_INSTANCE = WorkflowInstance.create(TestData.WORKFLOW_ID, "foo");
  private static final String EXECUTION_ID = "eid0";
  private static final RunSpec RUN_SPEC = RunSpec.simple(EXECUTION_ID, "busybox");
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

  @Mock private NamespacedKubernetesClient k8sClient;
  @Mock private KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager;
  @Mock private MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> pods;
  @Mock private MixedOperation<Job, JobList, DoneableJob, ScalableResource<Job, DoneableJob>> jobs;
  @Mock private MixedOperation<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>> secrets;
  @Mock private Resource<Secret, DoneableSecret> namedResource;
  @Mock private PodResource<Pod, DoneablePod> namedPod;
  @Mock private ScalableResource<Job, DoneableJob> namedJob;
  @Mock private ExtensionsAPIGroupDSL extensionsAPIGroupDSL;
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
  @Captor private ArgumentCaptor<Job> jobCaptor;

  @Rule public ExpectedException exception = ExpectedException.none();

  private Pod createdPod = KubernetesDockerRunnerTestUtil
      .createPod(WORKFLOW_INSTANCE, RUN_SPEC, EMPTY_SECRET_SPEC);
  private Stats stats = Mockito.mock(Stats.class);

  private KubernetesDockerRunner kdr;
  private Watcher<Pod> podWatcher;

  @Before
  public void setUp() {
    when(debug.get()).thenReturn(false);

    when(k8sClient.pods()).thenReturn(pods);

    when(k8sClient.extensions()).thenReturn(extensionsAPIGroupDSL);
    when(extensionsAPIGroupDSL.jobs()).thenReturn(jobs);

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

    final String podName = createdPod.getMetadata().getName();
    when(pods.withName(podName)).thenReturn(namedPod);

    final String jobName = getJobName(createdPod);
    when(jobs.withName(jobName)).thenReturn(namedJob);

    StateData stateData = StateData.newBuilder().executionId(EXECUTION_ID).build();
    RunState runState = RunState.create(WORKFLOW_INSTANCE, State.SUBMITTED, stateData);

    when(stateManager.getActiveStates()).thenReturn(ImmutableMap.of(WORKFLOW_INSTANCE, runState));
    when(stateManager.getActiveState(WORKFLOW_INSTANCE)).thenReturn(Optional.of(runState));
  }

  @After
  public void tearDown() {
    kdr.close();
  }

  @Test
  public void shouldUseExecutionIdForJobName() throws IOException {
    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC);
    verify(jobs).create(jobCaptor.capture());
    Job submittedJob = jobCaptor.getValue();
    assertThat(submittedJob.getMetadata().getName(), is(RUN_SPEC.executionId()));
  }

  @Test
  public void shouldCreateSingleContainerNamedByExecutionId() throws IOException {
    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC);
    verify(jobs).create(jobCaptor.capture());
    Job submittedJob = jobCaptor.getValue();
    assertThat(submittedJob.getSpec().getTemplate().getSpec().getContainers().size(), is(1));
    assertThat(submittedJob.getSpec().getTemplate().getSpec().getContainers().get(0).getName(),
        is(RUN_SPEC.executionId()));
  }

  @Test
  public void shouldNotDeleteJobfDebugEnabled() {
    when(debug.get()).thenReturn(true);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(5)).toString());

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, getJobName(createdPod), createdPod);

    verify(jobs, never()).delete(any(Job.class));
    verify(jobs, never()).delete(any(Job[].class));
    verify(jobs, never()).delete(anyListOf(Job.class));
    verify(jobs, never()).delete();
    verify(namedJob, never()).delete();

    verify(pods, never()).delete(any(Pod.class));
    verify(pods, never()).delete(any(Pod[].class));
    verify(pods, never()).delete(anyListOf(Pod.class));
    verify(pods, never()).delete();
    verify(namedPod, never()).delete();
  }

  @Test
  public void shouldCleanupJobAfterNonDeletePeriod() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(5)).toString());

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, getJobName(createdPod), createdPod);
    verify(namedJob).delete();
    verify(namedPod).delete();
  }

  @Test
  public void shouldCleanupJobWhenMissingFinishedAt() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, getJobName(createdPod), createdPod);

    verify(namedPod).delete();
    verify(namedJob).delete();
  }

  @Test
  public void shouldCleanupJobWhenMissingContainerStatus() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, getJobName(createdPod), createdPod);

    verify(namedJob).delete();
    verify(namedPod).delete();
  }

  @Test
  public void shouldCleanupJobWhenPullImageError() {
    // inject mock status in real instance
    createdPod.setStatus(waiting("Pending", "ErrImagePull"));

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, getJobName(createdPod), createdPod);

    verify(namedJob).delete();
    verify(namedPod).delete();
  }

  @Test
  public void shouldNotCleanupJobBeforeNonDeletePeriod() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(1)).toString());

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, getJobName(createdPod), createdPod);

    verify(namedJob, never()).delete();
    verify(namedPod, never()).delete();
  }

  @Test
  public void shouldNotCleanupJobIfNotTerminated() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, getJobName(createdPod), createdPod);

    verify(namedJob, never()).delete();
    verify(namedPod, never()).delete();
  }

  @Test
  public void shouldNotCleanupNonStyxJob() {
    createdPod.getMetadata().setAnnotations(Collections.emptyMap());

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));

    kdr.cleanupWithRunState(WORKFLOW_INSTANCE, getJobName(createdPod), createdPod);

    verify(namedJob, never()).delete();
    verify(namedPod, never()).delete();
  }

  @Test
  public void shouldNotCleanupNonStyxJobWithoutRunState() {
    createdPod.getMetadata().setAnnotations(Collections.emptyMap());

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));

    kdr.cleanupWithoutRunState(WORKFLOW_INSTANCE, getJobName(createdPod), createdPod);

    verify(namedJob, never()).delete();
    verify(namedPod, never()).delete();
  }

  @Test
  public void shouldCleanupJobWithoutRunStateIfNotTerminated() {
    final String name = getJobName(createdPod);
    when(jobs.withName(name)).thenReturn(namedJob);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);

    kdr.cleanupWithoutRunState(WORKFLOW_INSTANCE, getJobName(createdPod), createdPod);

    verify(namedJob).delete();
    verify(namedPod).delete();
  }

  @Test
  public void shouldNotCleanupJobWithoutRunStateBeforeNonDeletePeriod() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(1)).toString());

    kdr.cleanupWithoutRunState(WORKFLOW_INSTANCE, getJobName(createdPod), createdPod);

    verify(namedJob, never()).delete();
    verify(namedPod, never()).delete();
  }

  @Test
  public void shouldCleanupJobWithoutRunStateAfterNonDeletePeriod() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(5)).toString());

    kdr.cleanupWithoutRunState(WORKFLOW_INSTANCE, getJobName(createdPod), createdPod);

    verify(namedJob).delete();
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
    final Pod pod = KubernetesDockerRunnerTestUtil.createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SECRET,
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
    final Pod pod = KubernetesDockerRunnerTestUtil
        .createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA, SECRET_SPEC_WITH_SA);
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
  public void shouldConfigureResourceRequirements() {
    final String memRequest = "17Mi";
    final String memLimit = "4711Mi";
    final Pod pod = KubernetesDockerRunnerTestUtil.createPod(WORKFLOW_INSTANCE, RunSpec.builder()
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

    verify(jobs).create(jobCaptor.capture());
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

    verify(jobs).create(jobCaptor.capture());

    final Job job = jobCaptor.getValue();

    final Optional<SecretVolumeSource> serviceAccountSecretVolume = job.getSpec()
        .getTemplate().getSpec().getVolumes().stream()
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
  public void shouldNotRunIfSecretHasManagedServiceAccountKeySecretNamePrefix()
      throws IOException {
    final String secret = "styx-wf-sa-keys-foo";

    exception.expect(InvalidExecutionException.class);
    exception.expectMessage("Referenced secret '" + secret + "' has the managed service account key secret name prefix");
    kdr.start(WORKFLOW_INSTANCE, RunSpec.builder()
        .executionId("eid")
        .imageName("busybox")
        .secret(WorkflowConfiguration.Secret.create(secret, "/foo/bar"))
        .build());

    verify(jobs, never()).create(any(Job.class));
  }

  @Test
  public void shouldCompleteWithStatusCodeOnSucceeded() throws Exception {
    createdPod.setStatus(terminated("Succeeded", 20, null));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    verify(stateManager).receive(Event.started(WORKFLOW_INSTANCE), -1);
    verify(stateManager).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(20)), 0);
  }

  @Test
  public void shouldFailOnErrImagePull() throws Exception {
    createdPod.setStatus(waiting("Pending", "ErrImagePull"));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    verify(stateManager).receive(
        Event.runError(WORKFLOW_INSTANCE, "One or more containers failed to pull their image"),
        -1);
  }

  @Test
  public void shouldSendStatsOnErrImagePull() throws Exception {
    createdPod.setStatus(waiting("Pending", "ErrImagePull"));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    verify(stats, times(1)).recordPullImageError();
    verify(stateManager).receive(
        Event.runError(WORKFLOW_INSTANCE, "One or more containers failed to pull their image"),
        -1);
  }

  @Test
  public void shouldNotSendStatsOnOtherError() {
    createdPod.setStatus(podStatusNoContainer("Succeeded"));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    verifyNoMoreInteractions(stats);
  }

  @Test
  public void shouldFailOnUnknownPhaseEntered() throws Exception {
    createdPod.setStatus(podStatusNoContainer("Unknown"));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    verify(stateManager).receive(Event.runError(WORKFLOW_INSTANCE, "Pod entered Unknown phase"),
        -1);
  }

  @Test
  public void shouldIgnoreDeletedEvents() throws Exception {
    createdPod.setStatus(podStatusNoContainer("Succeeded"));
    podWatcher.eventReceived(Watcher.Action.DELETED, createdPod);

    verify(stateManager, never()).receive(any(), anyLong());
  }

  @Test
  public void shouldFailOnMissingContainer() throws Exception {
    createdPod.setStatus(podStatusNoContainer("Succeeded"));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    verify(stateManager).receive(
        Event.runError(WORKFLOW_INSTANCE, "Could not find our container in pod"),
        -1);
  }

  @Test
  public void shouldFailOnUnexpectedTerminatedStatus() throws Exception {
    createdPod.setStatus(waiting("Failed", ""));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    verify(stateManager).receive(
        Event.runError(WORKFLOW_INSTANCE, "Unexpected null terminated status"),
        -1);
  }

  @Test
  public void shouldGenerateStartedAndRecordSubmitToRunningTimeWhenContainerIsReady() throws Exception {
    when(time.nanoTime()).thenReturn(TimeUnit.SECONDS.toNanos(17));
    kdr.start(WORKFLOW_INSTANCE, RunSpec.simple(EXECUTION_ID, "busybox"));
    verify(stats).recordSubmission(EXECUTION_ID);

    when(time.nanoTime()).thenReturn(TimeUnit.SECONDS.toNanos(18));

    when(time.nanoTime()).thenReturn(TimeUnit.SECONDS.toNanos(19));
    createdPod.setStatus(running(/* ready= */ false));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);
    verify(stateManager, never()).receive(Event.started(WORKFLOW_INSTANCE), -1);

    when(time.nanoTime()).thenReturn(TimeUnit.SECONDS.toNanos(4711));
    createdPod.setStatus(running(/* ready= */ true));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);
    verify(stateManager).receive(Event.started(WORKFLOW_INSTANCE), -1);

    verify(stats).recordRunning(EXECUTION_ID);
  }

  @Test
  public void shouldDiscardChangesForOldExecutions() throws Exception {
    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC);

    // simulate event from different pod, but still with the same workflow instance annotation
    createdPod.getMetadata().getLabels().put("job-name", "another-job");
    createdPod.setStatus(terminated("Succeeded", 20, null));

    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    verify(stateManager, never()).receive(any(), anyLong());
  }

  @Test
  public void shouldPollPodStatusAndEmitEventsOnRestore() throws Exception {
    when(jobs.withName(getJobName(createdPod))).thenReturn(namedJob);

    // Stop the runner and change the pod status to terminated while styx is "down"
    kdr.close();
    createdPod.setStatus(terminated("Succeeded", 20, null,
        FIXED_INSTANT.minus(Duration.ofMinutes(1)).toString()));

    // Start a new runner
    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountSecretManager,
        debug, NO_POLL, 0, time);
    kdr.init();

    // Make the runner poll states for all pods
    kdr.restore();

    // Verify that the runner polled and found out that the pods is terminated
    verify(stateManager).receive(Event.started(WORKFLOW_INSTANCE), -1);
    verify(stateManager).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(20)), 0);
  }

  @Test
  public void shouldRegularlyPollPodStatusAndEmitEvents() throws Exception {
    when(jobs.withName(getJobName(createdPod))).thenReturn(namedJob);

    createdPod.setStatus(running(/* ready= */ true));

    // Set up a runner with short poll interval to avoid this test having to wait a long time for the poll
    kdr.close();
    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountSecretManager,
        debug, 1, 0, time);
    kdr.init();
    kdr.restore();

    // Change the pod status to terminated without notifying the runner through the pod watcher
    final Pod terminatedPod = new PodBuilder(createdPod)
        .withStatus(terminated("Succeeded", 20, null,
            FIXED_INSTANT.minus(Duration.ofMinutes(1)).toString()))
        .build();
    when(podList.getItems()).thenReturn(ImmutableList.of(terminatedPod));

    // Verify that the runner eventually polls and finds out that the pod is terminated
    verify(stateManager, timeout(30_000)).receive(
        Event.started(WORKFLOW_INSTANCE),
        -1);
    verify(stateManager, timeout(30_000)).receive(
        Event.terminate(WORKFLOW_INSTANCE, Optional.of(20)),
        0);
  }

}
