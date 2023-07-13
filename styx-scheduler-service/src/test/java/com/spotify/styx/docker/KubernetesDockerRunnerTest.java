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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.spotify.styx.QuietDeterministicScheduler;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.docker.KubernetesDockerRunner.KubernetesSecretSpec;
import com.spotify.styx.docker.KubernetesDockerRunner.PodDeletionDecision;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.StateTransitionConflictException;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.CounterCapacityException;
import com.spotify.styx.util.Debug;
import com.spotify.styx.util.Time;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.ListMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirements;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.StatusBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.Watcher.Action;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javaslang.control.Try;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class KubernetesDockerRunnerTest {

  private static final String EXECUTION_ID = "badf00d";
  private static final String RUNNER_ID = "test";
  private static final String POD_NAME = EXECUTION_ID;
  private static final String SERVICE_ACCOUNT = "sa@example.com";
  private static final String SERVICE_ACCOUNT_SECRET = "sa-secret";
  private static final WorkflowInstance WORKFLOW_INSTANCE = WorkflowInstance.create(TestData.WORKFLOW_ID, "foo");
  private static final RunState RUN_STATE = RunState.create(WORKFLOW_INSTANCE, State.SUBMITTING, StateData.zero());
  private static final RunSpec RUN_SPEC = RunSpec.simple("eid0", "busybox");
  private static final RunSpec RUN_SPEC_WITH_SECRET = RunSpec.builder()
      .executionId("eid1")
      .imageName("busybox")
      .build();
  private static final RunSpec RUN_SPEC_WITH_NON_WHITELISTED_SECRET = RunSpec.builder()
      .executionId("eid1")
      .imageName("busybox")
      .build();
  private static final RunSpec RUN_SPEC_WITH_SA = RunSpec.builder()
      .executionId("eid3")
      .imageName("busybox")
      .serviceAccount(SERVICE_ACCOUNT)
      .build();

  private static final KubernetesSecretSpec SECRET_SPEC_WITH_SA = KubernetesSecretSpec.builder()
      .serviceAccountSecret(SERVICE_ACCOUNT_SECRET)
      .build();

  private static final KubernetesSecretSpec EMPTY_SECRET_SPEC = KubernetesSecretSpec.builder()
      .build();

  private static final RunSpec RUN_SPEC_WITH_SECRET_AND_SA = RunSpec.builder()
      .executionId("eid")
      .imageName("busybox")
      .serviceAccount(SERVICE_ACCOUNT)
      .build();

  private static final int POD_CLEANUP_INTERVAL_SECONDS = 60;
  private static final int POD_DELETION_DELAY_SECONDS = 120;
  private static final Instant FIXED_INSTANT = Instant.parse("2017-09-01T01:00:00Z");

  private static final String STYX_ENVIRONMENT = "testing";

  @Mock private Fabric8KubernetesClient k8sClient;
  @Mock private KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager;
  @Mock private PodList podList;
  @Mock private PodStatus podStatus;
  @Mock private ContainerStatus containerStatus;
  @Mock private ContainerState containerState;
  @Mock private ContainerStateTerminated containerStateTerminated;
  @Mock private ListMeta listMeta;
  @Mock private Watch podWatch;
  @Mock private Debug debug;
  @Mock private Time time;
  @Mock private StateManager stateManager;

  @Captor private ArgumentCaptor<Watcher<Pod>> watcherCaptor;
  @Captor private ArgumentCaptor<Pod> podCaptor;

  private final Pod createdPod = createPod(WORKFLOW_INSTANCE, RUN_SPEC, EMPTY_SECRET_SPEC);
  private final Stats stats = mock(Stats.class);

  private KubernetesDockerRunner kdr;
  private Watcher<Pod> podWatcher;
  private final QuietDeterministicScheduler executor = new QuietDeterministicScheduler();
  private final ContainerStatus keepaliveContainerStatus = new ContainerStatusBuilder()
      .withName(KEEPALIVE_CONTAINER_NAME)
      .withNewState().withNewRunning().endRunning().endState()
      .build();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    when(debug.get()).thenReturn(false);

    when(k8sClient.getPod(createdPod.getMetadata().getName())).thenReturn(Optional.of(createdPod));
    when(k8sClient.listPods()).thenReturn(podList);
    when(podList.getItems()).thenReturn(List.of(createdPod));
    when(podList.getMetadata()).thenReturn(listMeta);
    when(listMeta.getResourceVersion()).thenReturn("1000");
    when(k8sClient.getSecret(any())).thenReturn(Optional.empty());

    when(k8sClient.watchPods(watcherCaptor.capture())).thenReturn(podWatch);

    when(serviceAccountSecretManager.ensureServiceAccountKeySecret(
        WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT))
        .thenReturn(SERVICE_ACCOUNT_SECRET);

    when(time.get()).thenReturn(FIXED_INSTANT);

    kdr = new KubernetesDockerRunner(RUNNER_ID, k8sClient, stateManager, stats, serviceAccountSecretManager,
        debug, STYX_ENVIRONMENT, PodMutator.NOOP, POD_CLEANUP_INTERVAL_SECONDS,
        POD_DELETION_DELAY_SECONDS, time, executor, Collections.emptyMap());
    kdr.init();

    verify(k8sClient).watchPods(any());

    podWatcher = watcherCaptor.getValue();

    Map<String, String> annotations = new HashMap<>();
    annotations.put(KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION, WORKFLOW_INSTANCE.toKey());
    createdPod.getMetadata().setAnnotations(annotations);
    createdPod.getMetadata().setName(POD_NAME);
    createdPod.getMetadata().setResourceVersion("1001");

    StateData stateData = StateData.newBuilder().executionId(POD_NAME).build();
    RunState runState = RunState.create(WORKFLOW_INSTANCE, State.SUBMITTED, stateData);

    when(stateManager.getActiveStates()).thenReturn(Map.of(WORKFLOW_INSTANCE, runState));
    when(stateManager.getActiveState(WORKFLOW_INSTANCE)).thenReturn(Optional.of(runState));
  }

  @After
  public void tearDown() throws Exception {
    kdr.close();
    verify(podWatch, atLeastOnce()).close();
  }

  @Test
  public void shouldReconnectUponWatcherClose() {
    Mockito.reset(k8sClient);
    when(k8sClient.watchPods(any())).thenThrow(new KubernetesClientException("Forced failure")).thenReturn(podWatch);

    podWatcher.onClose(new KubernetesClientException("Forced failure"));

    executor.tick(2, TimeUnit.SECONDS);
    verify(podWatch, times(2)).close();
    verify(k8sClient, times(2)).watchPods(any());
  }

  @Test
  public void shouldFailToInitialize() throws IOException {
    var spiedExecutor = spy(executor);
    when(k8sClient.watchPods(any())).thenThrow(new KubernetesClientException("Forced failure"));
    var kdr = new KubernetesDockerRunner(RUNNER_ID, k8sClient, stateManager, stats, serviceAccountSecretManager,
        debug, STYX_ENVIRONMENT, PodMutator.NOOP, POD_CLEANUP_INTERVAL_SECONDS,
        POD_DELETION_DELAY_SECONDS, time, spiedExecutor, Collections.emptyMap());
    kdr.init();
    verify(spiedExecutor).schedule(any(Runnable.class), anyLong(), any());
    kdr.close();
    verify(podWatch, never()).close();
  }

  @Test
  public void shouldToleratePodAlreadyCreated() throws IOException {
    Mockito.reset(k8sClient);
    when(k8sClient.createPod(any(Pod.class))).thenThrow(
        new KubernetesClientException("Already created", 409,
            new StatusBuilder().withReason("AlreadyExists").build()));
    assertThat(kdr.start(RUN_STATE, RUN_SPEC), is(RUNNER_ID));
    verifyNoInteractions(stateManager);
  }

  @Test
  public void shouldThrowIOException() {
    Mockito.reset(k8sClient);
    when(k8sClient.createPod(any(Pod.class))).thenThrow(
        new KubernetesClientException("foobar", 500,
            new StatusBuilder().withReason("foobar").build()));
    assertThrows("Failed to create Kubernetes pod", IOException.class,
        () -> kdr.start(RUN_STATE, RUN_SPEC));
  }

  @Test
  public void shouldCheckIfPodExistsBeforeCreating() throws IOException {
    when(k8sClient.getPod(any())).thenReturn(Optional.of(createdPod));
    kdr.start(RUN_STATE, RUN_SPEC);
    verify(k8sClient).getPod(RUN_SPEC.executionId());
    verifyNoMoreInteractions(k8sClient);
    verifyNoInteractions(stateManager);
  }

  @Test
  public void shouldUseExecutionIdForPodName() throws IOException {
    when(k8sClient.getPod(RUN_SPEC.executionId())).thenReturn(Optional.empty());
    kdr.start(RUN_STATE, RUN_SPEC);
    verify(k8sClient).getPod(RUN_SPEC.executionId());
    verify(k8sClient).createPod(podCaptor.capture());
    var submittedPod = podCaptor.getValue();
    assertThat(submittedPod.getMetadata().getName(), is(RUN_SPEC.executionId()));
  }

  @Test
  public void shouldCreateMainContainerAndKeepaliveContainer() throws IOException {
    when(k8sClient.getPod(RUN_SPEC.executionId())).thenReturn(Optional.empty());
    kdr.start(RUN_STATE, RUN_SPEC);
    verify(k8sClient).createPod(podCaptor.capture());
    var submittedPod = podCaptor.getValue();
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
    when(k8sClient.getPod(anyString())).thenReturn(Optional.of(createdPod));
    var shouldDelete = kdr.shouldDeletePod(WORKFLOW_INSTANCE, createdPod, "Foobar!");
    assertThat(shouldDelete, is(false));
  }

  @Parameters({"TERMINATED", "FAILED", "ERROR", "DONE"})
  @Test
  public void shouldCleanupPodAfterNonDeletePeriodIfRunStateNotRunning(String stateName) {
    final State state = State.valueOf(stateName);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(List.of(keepaliveContainerStatus, containerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(5)).toString());

    var runState = RunState.create(WORKFLOW_INSTANCE, state);
    var shouldDelete = kdr.shouldDeletePodWithRunState(WORKFLOW_INSTANCE, createdPod, runState);
    assertThat(shouldDelete, is(PodDeletionDecision.DELETE));
  }

  @Parameters({"TERMINATED", "FAILED", "ERROR", "DONE"})
  @Test
  public void shouldForceCleanupPodAfterPodForceDeletionTolerationPassed(String stateName) {
    final State state = State.valueOf(stateName);

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(List.of(keepaliveContainerStatus, containerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(35)).toString());

    var runState = RunState.create(WORKFLOW_INSTANCE, state);
    var shouldDelete = kdr.shouldDeletePodWithRunState(WORKFLOW_INSTANCE, createdPod, runState);
    assertThat(shouldDelete, is(PodDeletionDecision.FORCE_DELETE));
  }

  @Parameters({"NEW", "QUEUED", "PREPARE", "SUBMITTING", "SUBMITTED", "RUNNING"})
  @Test
  public void shouldNotDeletePodAfterNonDeletePeriodIfRunStateStillRunning(String stateName) {
    final State state = State.valueOf(stateName);

    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(List.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(5)).toString());

    var runState = RunState.create(WORKFLOW_INSTANCE, state);
    var shouldDelete = kdr.shouldDeletePodWithRunState(WORKFLOW_INSTANCE, createdPod, runState);
    assertThat(shouldDelete, is(PodDeletionDecision.DO_NOT_DELETE));
  }

  @Test
  public void shouldForceDeletePodWhenMissingFinishedAt() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(List.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);

    var runState = RunState.create(WORKFLOW_INSTANCE, State.TERMINATED);
    var shouldDelete = kdr.shouldDeletePodWithRunState(WORKFLOW_INSTANCE, createdPod, runState);
    assertThat(shouldDelete, is(PodDeletionDecision.FORCE_DELETE));
  }

  @Test
  public void shouldForceDeletePodWhenFailedToParseFinishedAt() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(List.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerStateTerminated.getFinishedAt()).thenReturn("foo");
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);

    var runState = RunState.create(WORKFLOW_INSTANCE, State.TERMINATED);
    var shouldDelete = kdr.shouldDeletePodWithRunState(WORKFLOW_INSTANCE, createdPod, runState);
    assertThat(shouldDelete, is(PodDeletionDecision.FORCE_DELETE));
  }

  @Test
  public void shouldNotCleanupPodWhenMissingContainerStatus() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);

    var runState = RunState.create(WORKFLOW_INSTANCE, State.TERMINATED);
    var shouldDelete = kdr.shouldDeletePodWithRunState(WORKFLOW_INSTANCE, createdPod, runState);

    // It is normal for a pod to not have any container status for a while after creation
    assertThat(shouldDelete, is(PodDeletionDecision.DO_NOT_DELETE));
  }

  @Test
  public void shouldCleanupPodWhenPullImageError() {
    // inject mock status in real instance
    setWaiting(createdPod, "Pending", "ErrImagePull");

    var runState = RunState.create(WORKFLOW_INSTANCE, State.TERMINATED);
    var shouldDelete = kdr.shouldDeletePodWithRunState(WORKFLOW_INSTANCE, createdPod, runState);
    assertThat(shouldDelete, is(PodDeletionDecision.DELETE));
  }

  @Test
  public void shouldNotCleanupPodBeforeNonDeletePeriod() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(List.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(1)).toString());

    var runState = RunState.create(WORKFLOW_INSTANCE, State.TERMINATED);
    var shouldDelete = kdr.shouldDeletePodWithRunState(WORKFLOW_INSTANCE, createdPod, runState);
    assertThat(shouldDelete, is(PodDeletionDecision.DO_NOT_DELETE));
  }

  @Test
  public void shouldNotCleanupPodIfNotTerminated() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(List.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);

    var runState = RunState.create(WORKFLOW_INSTANCE, State.TERMINATED);
    var shouldDelete = kdr.shouldDeletePodWithRunState(WORKFLOW_INSTANCE, createdPod, runState);
    assertThat(shouldDelete, is(PodDeletionDecision.DO_NOT_DELETE));
  }

  @Test
  public void shouldNotCleanupNonStyxPod() {
    createdPod.getMetadata().setAnnotations(Collections.emptyMap());

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(List.of(containerStatus, keepaliveContainerStatus));

    when(stateManager.getActiveStates()).thenReturn(Collections.emptyMap());

    kdr.tryCleanupPods();

    verify(k8sClient, never()).deletePod(any(), anyBoolean());
  }

  @Test
  public void shouldNotCleanupNonStyxPodWithoutRunState() {
    createdPod.getMetadata().setAnnotations(Collections.emptyMap());

    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(List.of(containerStatus, keepaliveContainerStatus));

    when(stateManager.getActiveStates()).thenReturn(Collections.emptyMap());

    kdr.tryCleanupPods();

    verify(k8sClient, never()).deletePod(any(), anyBoolean());
  }

  @Test
  public void shouldCleanupPodWithoutRunStateIfNotTerminated() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(List.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(k8sClient.getPod(POD_NAME)).thenReturn(Optional.of(createdPod));

    var shouldDelete = kdr.shouldDeletePodWithoutRunState(WORKFLOW_INSTANCE, createdPod);
    assertThat(shouldDelete, is(PodDeletionDecision.DELETE));
  }

  @Test
  public void shouldNotCleanupTerminatedPodWithoutRunStateBeforeNonDeletePeriod() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(List.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(1)).toString());

    var shouldDelete = kdr.shouldDeletePodWithoutRunState(WORKFLOW_INSTANCE, createdPod);
    assertThat(shouldDelete, is(PodDeletionDecision.DO_NOT_DELETE));
  }

  @Test
  public void shouldNotCleanupRunningPodWithoutRunStateIfTerminatedAfterRefresh() {
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
    when(k8sClient.getPod(POD_NAME)).thenReturn(Optional.of(refreshedPod));

    var shouldDelete = kdr.shouldDeletePodWithoutRunState(WORKFLOW_INSTANCE, createdPod);

    // Leave the pod to expire and be deleted by a later poll tick
    assertThat(shouldDelete, is(PodDeletionDecision.DO_NOT_DELETE));
  }

  @Test
  public void shouldCleanupPodWithoutRunStateAfterNonDeletePeriod() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(List.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
    when(containerStateTerminated.getFinishedAt())
        .thenReturn(FIXED_INSTANT.minus(Duration.ofMinutes(5)).toString());

    var shouldDelete = kdr.shouldDeletePodWithoutRunState(WORKFLOW_INSTANCE, createdPod);
    assertThat(shouldDelete, is(PodDeletionDecision.DELETE));
  }

  @Test
  public void shouldCleanupNonExistPod() {
    // inject mock status in real instance
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(List.of(containerStatus, keepaliveContainerStatus));
    when(containerStatus.getName()).thenReturn(MAIN_CONTAINER_NAME);
    when(containerStatus.getState()).thenReturn(containerState);
    when(k8sClient.getPod(POD_NAME)).thenReturn(Optional.empty());

    var shouldDelete = kdr.shouldDeletePodWithoutRunState(WORKFLOW_INSTANCE, createdPod);
    assertThat(shouldDelete, is(PodDeletionDecision.DO_NOT_DELETE));
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
    final String cpuRequest = "1";
    final String memLimit = "4711Mi";
    final String cpuLimit = "2";
    final Pod pod = createPod(WORKFLOW_INSTANCE, RunSpec.builder()
        .executionId("eid1")
        .imageName("busybox")
        .memRequest(memRequest)
        .memLimit(memLimit)
        .cpuRequest(cpuRequest)
        .cpuLimit(cpuLimit)
        .build(),
        EMPTY_SECRET_SPEC);

    final ResourceRequirements resourceReqs = pod.getSpec().getContainers().get(0).getResources();
    assertThat(resourceReqs.getRequests().get("memory"), is(new Quantity(memRequest)));
    assertThat(resourceReqs.getRequests().get("cpu"), is(new Quantity(cpuRequest)));
    assertThat(resourceReqs.getLimits().get("memory"), is(new Quantity(memLimit)));
    assertThat(resourceReqs.getLimits().get("cpu"), is(new Quantity(cpuLimit)));
  }

  @Test
  public void shouldRunIfSecretExists() throws IOException {
    when(k8sClient.getSecret(any())).thenReturn(Optional.of(new SecretBuilder().build()));
    assertThat(kdr.start(RUN_STATE, RUN_SPEC_WITH_SECRET), is(RUNNER_ID));
    verify(k8sClient).createPod(any());
  }

  @Test
  public void shouldCleanupServiceAccountSecrets() throws Exception {
    kdr.cleanup();
    verify(serviceAccountSecretManager).cleanup();
  }

  @Test
  public void shouldEnsureAndMountServiceAccountSecret() throws IOException {
    when(serviceAccountSecretManager.ensureServiceAccountKeySecret(
        WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT)).thenReturn(SERVICE_ACCOUNT_SECRET);

    kdr.start(RUN_STATE, RUN_SPEC_WITH_SA);

    verify(serviceAccountSecretManager).ensureServiceAccountKeySecret(
        WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT);

    verify(k8sClient).createPod(podCaptor.capture());

    final Pod pod = podCaptor.getValue();

    final Optional<SecretVolumeSource> serviceAccountSecretVolume = pod.getSpec().getVolumes().stream()
        .map(Volume::getSecret)
        .filter(Objects::nonNull)
        .filter(v -> SERVICE_ACCOUNT_SECRET.equals(v.getSecretName()))
        .findAny();

    assertThat(serviceAccountSecretVolume.isPresent(), is(true));
  }

  @Test
  public void shouldNotRunIfServiceAccountSecretEnsureFails() {
    final InvalidExecutionException error = new InvalidExecutionException("SA not found");
    when(serviceAccountSecretManager.ensureServiceAccountKeySecret(
        WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT)).thenThrow(error);

    var exception = assertThrows(InvalidExecutionException.class, () -> kdr.start(RUN_STATE, RUN_SPEC_WITH_SA));
    assertThat(exception, is(error));
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
    setWaiting(createdPod, "Pending", "ErrImagePull", "foobar");
    receiveAndProcessEvent(Watcher.Action.MODIFIED, createdPod);

    verify(stateManager).receive(
        Event.runError(WORKFLOW_INSTANCE,
            "Failed to pull image busybox:latest of container styx-run, reason: ErrImagePull, message: foobar"),
        -1);
  }

  @Test
  public void shouldSendStatsOnErrImagePull() throws Exception {
    setWaiting(createdPod, "Pending", "ErrImagePull", "foobar");
    receiveAndProcessEvent(Watcher.Action.MODIFIED, createdPod);

    verify(stats).recordPullImageError();
    verify(stateManager).receive(
        Event.runError(WORKFLOW_INSTANCE,
            "Failed to pull image busybox:latest of container styx-run, reason: ErrImagePull, message: foobar"),
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
    kdr.start(RUN_STATE, RunSpec.simple(POD_NAME, "busybox"));
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

    verify(stats).recordRunning(POD_NAME, createdPod.getSpec().getNodeName());
  }

  @Test
  public void shouldDiscardChangesForOldExecutions() throws Exception {
    kdr.start(RUN_STATE, RUN_SPEC);

    // simulate event from different pod, but still with the same workflow instance annotation
    createdPod.getMetadata().setName(POD_NAME + "-other");
    setTerminated(createdPod, "Succeeded", 20, null);

    receiveAndProcessEvent(Watcher.Action.MODIFIED, createdPod);

    verify(stateManager, never()).receive(any(), anyLong());
  }

  @Test
  public void shouldPollPodStatusAndEmitEvents() throws Exception {

    // Change the pod status to terminated without notifying the runner through the pod watcher
    final Pod terminatedPod = new PodBuilder(createdPod)
        .withStatus(terminated("Succeeded", 20, null))
        .build();
    when(k8sClient.getPod(POD_NAME)).thenReturn(Optional.of(terminatedPod));

    // Poll for execution status
    var stateData = StateData.newBuilder().executionId(POD_NAME).build();
    var runState = RunState.create(WORKFLOW_INSTANCE, State.SUBMITTED, stateData);
    kdr.poll(runState);

    // Verify that the runner found out that the pod is terminated and emits events
    verify(stateManager, atLeastOnce()).receive(
        Event.started(WORKFLOW_INSTANCE),
        -1);
    verify(stateManager, atLeastOnce()).receive(
        Event.terminate(WORKFLOW_INSTANCE, Optional.of(20)),
        0);
  }

  @Test
  public void shouldTolerateTransitionConflictWhenEmittingEvents() throws Exception {
    // Change the pod status to terminated without notifying the runner through the pod watcher
    final Pod terminatedPod = new PodBuilder(createdPod)
        .withStatus(terminated("Succeeded", 20, null))
        .build();
    when(k8sClient.getPod(POD_NAME)).thenReturn(Optional.of(terminatedPod));

    doThrow(new StateTransitionConflictException("foo!"))
        .when(stateManager).receive(any(), anyLong());

    verifyNoInteractions(stateManager);

    // Poll for execution status
    var stateData = StateData.newBuilder().executionId(POD_NAME).build();
    var runState = RunState.create(WORKFLOW_INSTANCE, State.SUBMITTED, stateData);
    kdr.poll(runState);

    verify(stateManager).receive(any(), anyLong());
  }

  @Test
  public void shouldTolerateCounterCapacityExceptionWhenEmittingEvents() throws Exception {
    // Change the pod status to terminated without notifying the runner through the pod watcher
    var terminatedPod = new PodBuilder(createdPod)
        .withStatus(terminated("Succeeded", 20, null))
        .build();
    when(k8sClient.getPod(POD_NAME)).thenReturn(Optional.of(terminatedPod));

    doThrow(new CounterCapacityException("foo!"))
        .when(stateManager).receive(any(), anyLong());

    verifyNoInteractions(stateManager);

    // Poll for execution status
    var stateData = StateData.newBuilder().executionId(POD_NAME).build();
    var runState = RunState.create(WORKFLOW_INSTANCE, State.SUBMITTED, stateData);
    kdr.poll(runState);

    verify(stateManager).receive(any(), anyLong());
  }

  @Test
  public void shouldHandlePodWithoutAnnotationsWhenPolling()  {
    // Create a pod instance with null `annotations` field - not possible through builder
    final Pod pod = Json.OBJECT_MAPPER.convertValue(
        Map.of("metadata",
            Map.of("name", "foobar")), Pod.class);

    assertThat(pod.getMetadata().getAnnotations(), is(nullValue()));

    when(k8sClient.getPod("foobar")).thenReturn(Optional.of(pod));
    when(podList.getItems()).thenReturn(List.of(pod));

    assertThat(Try.run(() -> kdr.tryCleanupPods()).isSuccess(), is(true));
  }

  @Test
  public void shouldHandlePodWithoutAnnotationsWhenWatching()  {
    // Create a pod instance with null `annotations` field - not possible through builder
    final Pod pod = Json.OBJECT_MAPPER.convertValue(
        Map.of("metadata",
            Map.of("name", "foobar")), Pod.class);

    assertThat(Try.run(() -> podWatcher.eventReceived(Action.MODIFIED, pod)).isSuccess(), is(true));
  }

  @Test
  public void shouldRecognizeMainContainer() {
    assertThat(KubernetesDockerRunner.isMainContainer(MAIN_CONTAINER_NAME), is(true));
    assertThat(KubernetesDockerRunner.isMainContainer(KEEPALIVE_CONTAINER_NAME), is(false));
    assertThat(KubernetesDockerRunner.isMainContainer("foobar"), is(false));
  }

  /**
   * Helper to deal with asynchronous pod even handling
   */
  private void receiveAndProcessEvent(Action action, Pod pod) {
    when(k8sClient.getPod(pod.getMetadata().getName())).thenReturn(Optional.of(pod));
    podWatcher.eventReceived(action, pod);
    executor.tick(10, TimeUnit.SECONDS);
  }

  private static Pod createPod(WorkflowInstance workflowInstance,
                               DockerRunner.RunSpec runSpec,
                               KubernetesSecretSpec secretSpec) {
    return KubernetesDockerRunner
        .createPod(workflowInstance, runSpec, secretSpec, STYX_ENVIRONMENT, PodMutator.NOOP,
            Collections.emptyMap(), StateData.zero().tries());
  }
}
