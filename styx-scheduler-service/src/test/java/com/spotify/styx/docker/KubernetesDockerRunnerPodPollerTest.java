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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.docker.KubernetesDockerRunner.KubernetesSecretSpec;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.Debug;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.DoneableJob;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.Job;
import io.fabric8.kubernetes.api.model.JobList;
import io.fabric8.kubernetes.api.model.ListMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.dsl.ExtensionsAPIGroupDSL;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.ScalableResource;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesDockerRunnerPodPollerTest {

  private static final String EXECUTION_ID_1 = "eid1";
  private static final String EXECUTION_ID_2 = "eid2";

  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "foo");
  private static final WorkflowInstance WORKFLOW_INSTANCE_2 =
      WorkflowInstance.create(TestData.WORKFLOW_ID_2, "bar");
  private static final DockerRunner.RunSpec RUN_SPEC =
      DockerRunner.RunSpec.simple(EXECUTION_ID_1, "busybox");
  private static final DockerRunner.RunSpec RUN_SPEC_2 =
      DockerRunner.RunSpec.simple(EXECUTION_ID_2, "busybox");
  private final static KubernetesSecretSpec SECRET_SPEC = KubernetesSecretSpec.builder().build();

  @Mock private NamespacedKubernetesClient k8sClient;
  @Mock private MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> pods;
  @Mock private MixedOperation<Job, JobList, DoneableJob, ScalableResource<Job, DoneableJob>> jobs;
  @Mock private ExtensionsAPIGroupDSL extensionsAPIGroupDSL;
  private PodList podList;
  @Mock private PodResource<Pod, DoneablePod> namedPod1;
  @Mock private PodResource<Pod, DoneablePod> namedPod2;
  @Mock private ScalableResource<Job, DoneableJob> namedJob1;
  @Mock private ScalableResource<Job, DoneableJob> namedJob2;
  @Mock private PodStatus podStatus;
  @Mock private ContainerStatus containerStatus;
  @Mock private ContainerState containerState;
  @Mock private ContainerStateTerminated containerStateTerminated;
  @Mock private StateManager stateManager;
  @Mock private Stats stats;

  @Mock private KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager;
  @Mock private Debug debug;

  private KubernetesDockerRunner kdr;

  @Before
  public void setUp() {
    when(debug.get()).thenReturn(false);

    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountSecretManager, debug);
    podList = new PodList();
    podList.setMetadata(new ListMeta());
    podList.getMetadata().setResourceVersion("4711");

    when(k8sClient.pods()).thenReturn(pods);
    when(pods.list()).thenReturn(podList);

    when(k8sClient.extensions()).thenReturn(extensionsAPIGroupDSL);
    when(extensionsAPIGroupDSL.jobs()).thenReturn(jobs);
  }

  @Test
  @SuppressWarnings("MockitoCast")
  public void shouldSendRunErrorWhenPodForRunningWFIDoesntExist() {
    Pod createdPod = KubernetesDockerRunnerTestUtil
        .createPod(WORKFLOW_INSTANCE, RUN_SPEC, SECRET_SPEC);
    podList.setItems(Collections.singletonList(createdPod));
    setupActiveInstances(RunState.State.RUNNING, EXECUTION_ID_1, EXECUTION_ID_2);

    kdr.pollPods();

    verify(stateManager, times(1)).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE_2, "No pod associated with this instance"));
  }

  @Test
  public void shouldNotSendRunErrorWhenPodForRunningWFIExists() {
    Pod createdPod = KubernetesDockerRunnerTestUtil
        .createPod(WORKFLOW_INSTANCE, RUN_SPEC, SECRET_SPEC);
    Pod createdPod2 = KubernetesDockerRunnerTestUtil
        .createPod(WORKFLOW_INSTANCE_2, RUN_SPEC, SECRET_SPEC);
    podList.setItems(Arrays.asList(createdPod, createdPod2));
    setupActiveInstances(RunState.State.RUNNING, EXECUTION_ID_1, EXECUTION_ID_2);

    kdr.pollPods();

    verify(stateManager, never()).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE, "No pod associated with this instance"));
    verify(stateManager, never()).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE_2, "No pod associated with this instance"));
  }

  @Test
  public void shouldHandleEmptyPodList() {
    when(jobs.withName(EXECUTION_ID_1)).thenReturn(namedJob1);
    when(jobs.withName(EXECUTION_ID_2)).thenReturn(namedJob2);

    when(k8sClient.pods().list()).thenReturn(podList);
    setupActiveInstances(RunState.State.RUNNING, EXECUTION_ID_1, EXECUTION_ID_2);

    kdr.pollPods();

    verify(stateManager).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE, "No pod associated with this instance"));
    verify(namedJob1).delete();
    verify(stateManager).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE_2, "No pod associated with this instance"));
    verify(namedJob2).delete();
  }

  @Test
  public void shouldNotSendErrorEventForInstancesNotInRunningState() {
    when(k8sClient.pods().list()).thenReturn(podList);
    setupActiveInstances(RunState.State.SUBMITTED, EXECUTION_ID_1, EXECUTION_ID_2);

    kdr.pollPods();

    verify(stateManager, never()).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE, "No pod associated with this instance"));
    verify(stateManager, never()).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE_2, "No pod associated with this instance"));
  }

  @Test
  public void shouldDeleteJobsNoRunStates() {
    final Pod createdPod1 = KubernetesDockerRunnerTestUtil
        .createPod(WORKFLOW_INSTANCE, RUN_SPEC, SECRET_SPEC);
    final Pod createdPod2 = KubernetesDockerRunnerTestUtil
        .createPod(WORKFLOW_INSTANCE_2, RUN_SPEC_2, SECRET_SPEC);

    when(pods.withName(createdPod1.getMetadata().getName())).thenReturn(namedPod1);
    when(pods.withName(createdPod2.getMetadata().getName())).thenReturn(namedPod2);
    when(jobs.withName(getJobName(createdPod1))).thenReturn(namedJob1);
    when(jobs.withName(getJobName(createdPod2))).thenReturn(namedJob2);

    podList.setItems(Arrays.asList(createdPod1, createdPod2));
    when(stateManager.getActiveState(any())).thenReturn(Optional.empty());

    setStatusAndState(createdPod1, RUN_SPEC.executionId());
    setStatusAndState(createdPod2, RUN_SPEC_2.executionId());

    kdr.pollPods();

    verify(namedJob1).delete();
    verify(namedJob2).delete();
    verify(namedPod1).delete();
    verify(namedPod2).delete();
  }

  @Test
  public void shouldNotDeleteUnwantedStyxJobsIfDebugEnabled() {
    when(debug.get()).thenReturn(true);

    final Pod createdPod1 = KubernetesDockerRunnerTestUtil
        .createPod(WORKFLOW_INSTANCE, RUN_SPEC, SECRET_SPEC);
    final Pod createdPod2 = KubernetesDockerRunnerTestUtil
        .createPod(WORKFLOW_INSTANCE_2, RUN_SPEC_2, SECRET_SPEC);

    when(pods.withName(createdPod1.getMetadata().getName())).thenReturn(namedPod1);
    when(pods.withName(createdPod2.getMetadata().getName())).thenReturn(namedPod2);
    when(jobs.withName(getJobName(createdPod1))).thenReturn(namedJob1);
    when(jobs.withName(getJobName(createdPod2))).thenReturn(namedJob2);

    podList.setItems(Arrays.asList(createdPod1, createdPod2));
    when(stateManager.getActiveState(any())).thenReturn(Optional.empty());

    setStatusAndState(createdPod1, RUN_SPEC.executionId());
    setStatusAndState(createdPod2, RUN_SPEC_2.executionId());

    kdr.pollPods();

    verify(k8sClient.extensions().jobs(), never()).delete(any(Job.class));
    verify(k8sClient.extensions().jobs(), never()).delete(any(Job[].class));
    verify(k8sClient.extensions().jobs(), never()).delete(anyListOf(Job.class));
    verify(k8sClient.extensions().jobs(), never()).delete();
    verify(namedJob1, never()).delete();
    verify(namedJob2, never()).delete();

    verify(k8sClient.pods(), never()).delete(any(Pod.class));
    verify(k8sClient.pods(), never()).delete(any(Pod[].class));
    verify(k8sClient.pods(), never()).delete(anyListOf(Pod.class));
    verify(k8sClient.pods(), never()).delete();
    verify(namedPod1, never()).delete();
    verify(namedPod2, never()).delete();
  }

  @Test
  public void shouldNotDeleteUnwantedNonStyxPods() {
    final Pod createdPod1 = KubernetesDockerRunnerTestUtil
        .createPod(WORKFLOW_INSTANCE, RUN_SPEC, SECRET_SPEC);
    final Pod createdPod2 = KubernetesDockerRunnerTestUtil
        .createPod(WORKFLOW_INSTANCE_2, RUN_SPEC_2, SECRET_SPEC);

    createdPod1.getMetadata().getAnnotations().remove("styx-workflow-instance");
    createdPod2.getMetadata().getAnnotations().remove("styx-workflow-instance");

    when(pods.withName(createdPod1.getMetadata().getName())).thenReturn(namedPod1);
    when(pods.withName(createdPod2.getMetadata().getName())).thenReturn(namedPod2);
    when(jobs.withName(getJobName(createdPod1))).thenReturn(namedJob1);
    when(jobs.withName(getJobName(createdPod2))).thenReturn(namedJob2);

    podList.setItems(Arrays.asList(createdPod1, createdPod2));

    kdr.pollPods();

    verify(k8sClient.extensions().jobs(), never()).delete(any(Job.class));
    verify(k8sClient.extensions().jobs(), never()).delete(any(Job[].class));
    verify(k8sClient.extensions().jobs(), never()).delete(anyListOf(Job.class));
    verify(k8sClient.extensions().jobs(), never()).delete();
    verify(namedJob1, never()).delete();
    verify(namedJob2, never()).delete();

    verify(k8sClient.pods(), never()).delete(any(Pod.class));
    verify(k8sClient.pods(), never()).delete(any(Pod[].class));
    verify(k8sClient.pods(), never()).delete(anyListOf(Pod.class));
    verify(k8sClient.pods(), never()).delete();
    verify(namedPod1, never()).delete();
    verify(namedPod2, never()).delete();
  }

  @Test
  public void shouldNotDeleteWantedStyxJobs() {
    final Pod createdPod1 = KubernetesDockerRunnerTestUtil
        .createPod(WORKFLOW_INSTANCE, RUN_SPEC, SECRET_SPEC);
    final Pod createdPod2 = KubernetesDockerRunnerTestUtil
        .createPod(WORKFLOW_INSTANCE_2, RUN_SPEC, SECRET_SPEC);

    when(pods.withName(createdPod1.getMetadata().getName())).thenReturn(namedPod1);
    when(pods.withName(createdPod2.getMetadata().getName())).thenReturn(namedPod2);
    when(jobs.withName(getJobName(createdPod1))).thenReturn(namedJob1);
    when(jobs.withName(getJobName(createdPod2))).thenReturn(namedJob2);

    podList.setItems(Arrays.asList(createdPod1, createdPod2));

    setupActiveInstances(RunState.State.RUNNING, RUN_SPEC.executionId(), RUN_SPEC_2.executionId());

    kdr.pollPods();

    verify(k8sClient.extensions().jobs(), never()).delete(any(Job.class));
    verify(k8sClient.extensions().jobs(), never()).delete(any(Job[].class));
    verify(k8sClient.extensions().jobs(), never()).delete(anyListOf(Job.class));
    verify(k8sClient.extensions().jobs(), never()).delete();
    verify(namedJob1, never()).delete();
    verify(namedJob2, never()).delete();

    verify(k8sClient.pods(), never()).delete(any(Pod.class));
    verify(k8sClient.pods(), never()).delete(any(Pod[].class));
    verify(k8sClient.pods(), never()).delete(anyListOf(Pod.class));
    verify(k8sClient.pods(), never()).delete();
    verify(namedPod1, never()).delete();
    verify(namedPod2, never()).delete();
  }

  private void setupActiveInstances(RunState.State state, String executionId1, String executionId2) {
    StateData stateData = StateData.newBuilder().executionId(executionId1).build();
    StateData stateData2 = StateData.newBuilder().executionId(executionId2).build();
    Map<WorkflowInstance, RunState> map = new HashMap<>();
    RunState runState = RunState.create(WORKFLOW_INSTANCE, state, stateData);
    RunState runState2 = RunState.create(WORKFLOW_INSTANCE_2, state, stateData2);
    map.put(WORKFLOW_INSTANCE, runState);
    map.put(WORKFLOW_INSTANCE_2, runState2);
    when(stateManager.getActiveState(WORKFLOW_INSTANCE)).thenReturn(Optional.of(runState));
    when(stateManager.getActiveState(WORKFLOW_INSTANCE_2)).thenReturn(Optional.of(runState2));
    when(stateManager.getActiveStates()).thenReturn(map);
  }

  private void setStatusAndState(Pod createdPod, String containerName) {
    createdPod.setStatus(podStatus);
    when(podStatus.getContainerStatuses()).thenReturn(ImmutableList.of(containerStatus));
    when(containerStatus.getName()).thenReturn(containerName);
    when(containerStatus.getState()).thenReturn(containerState);
    when(containerState.getTerminated()).thenReturn(containerStateTerminated);
  }
}
