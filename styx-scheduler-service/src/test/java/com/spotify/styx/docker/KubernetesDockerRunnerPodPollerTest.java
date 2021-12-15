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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.spotify.styx.docker.KubernetesDockerRunner.KubernetesSecretSpec;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.Debug;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ListMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.PodStatusBuilder;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesDockerRunnerPodPollerTest {

  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "foo");
  private static final WorkflowInstance WORKFLOW_INSTANCE_2 =
      WorkflowInstance.create(TestData.WORKFLOW_ID_2, "bar");
  private static final DockerRunner.RunSpec RUN_SPEC =
      DockerRunner.RunSpec.simple("eid1", "busybox");
  private static final DockerRunner.RunSpec RUN_SPEC_2 =
      DockerRunner.RunSpec.simple("eid2", "busybox");
  private final static KubernetesSecretSpec SECRET_SPEC = KubernetesSecretSpec.builder().build();

  private static final String POD_NAME = RUN_SPEC.executionId();
  private static final String POD_NAME_2 = RUN_SPEC_2.executionId();

  private static final String STYX_ENVIRONMENT = "testing";

  @Mock Fabric8KubernetesClient k8sClient;
  @Mock PodStatus podStatus1;
  @Mock PodStatus podStatus2;
  @Mock ContainerStatus containerStatus1;
  @Mock ContainerStatus containerStatus2;
  @Mock StateManager stateManager;
  @Mock Stats stats;

  @Mock KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager;
  @Mock Debug debug;

  private PodList podList;

  private KubernetesDockerRunner kdr;

  @Before
  public void setUp() {
    when(debug.get()).thenReturn(false);

    podList = new PodList();
    podList.setMetadata(new ListMeta());
    podList.getMetadata().setResourceVersion("4711");

    when(k8sClient.getPod(any())).thenReturn(Optional.empty());
    when(k8sClient.listPods()).thenReturn(podList);

    kdr = new KubernetesDockerRunner("test", k8sClient, stateManager, stats, serviceAccountSecretManager,
        debug, STYX_ENVIRONMENT, PodMutator.NOOP, Collections.emptyMap());
  }

  @Test
  public void shouldSendRunErrorWhenPodForRunningWFIDoesntExist() {
    var states = setupActiveInstances(RunState.State.RUNNING, POD_NAME, POD_NAME_2);
    var runState = states.get(WORKFLOW_INSTANCE);

    kdr.poll(runState);

    verify(stateManager, times(1)).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE, "No pod associated with this instance"), -1);
  }

  @Test
  public void shouldNotSendRunErrorWhenPodForRunningWFIExists() {
    var createdPod1 = createPod(WORKFLOW_INSTANCE, RUN_SPEC, SECRET_SPEC);
    var createdPod2 = createPod(WORKFLOW_INSTANCE_2, RUN_SPEC_2, SECRET_SPEC);
    when(podStatus1.getPhase()).thenReturn("Pending");
    when(podStatus2.getPhase()).thenReturn("Pending");
    createdPod1.setStatus(podStatus1);
    createdPod2.setStatus(podStatus2);

    when(k8sClient.getPod(POD_NAME)).thenReturn(Optional.of(createdPod1));
    when(k8sClient.getPod(POD_NAME_2)).thenReturn(Optional.of(createdPod2));
    var states = setupActiveInstances(RunState.State.RUNNING, POD_NAME, POD_NAME_2);
    var runState1 = states.get(WORKFLOW_INSTANCE);
    var runState2 = states.get(WORKFLOW_INSTANCE_2);

    kdr.poll(runState1);
    kdr.poll(runState2);

    verifyNoMoreInteractions(stateManager);
  }

  @Test
  public void shouldNotSendErrorEventForInstancesNotInRunningState() {
    setupActiveInstances(RunState.State.SUBMITTED, POD_NAME, POD_NAME_2);

    kdr.tryCleanupPods();

    verifyNoInteractions(stateManager);
  }

  @Test
  public void shouldDeleteUnwantedStyxPods() {
    final Pod createdPod1 = createPod(WORKFLOW_INSTANCE, RUN_SPEC, SECRET_SPEC);
    final Pod createdPod2 = createPod(WORKFLOW_INSTANCE_2, RUN_SPEC_2, SECRET_SPEC);

    podList.setItems(Arrays.asList(createdPod1, createdPod2));
    when(k8sClient.getPod(RUN_SPEC.executionId())).thenReturn(Optional.of(createdPod1));
    when(k8sClient.getPod(RUN_SPEC_2.executionId())).thenReturn(Optional.of(createdPod2));

    createdPod1.setStatus(podStatus1);
    when(podStatus1.getContainerStatuses()).thenReturn(List.of(containerStatus1));
    when(containerStatus1.getName()).thenReturn(RUN_SPEC.executionId());

    createdPod2.setStatus(podStatus2);
    when(podStatus2.getContainerStatuses()).thenReturn(List.of(containerStatus2));
    when(containerStatus2.getName()).thenReturn(RUN_SPEC_2.executionId());

    kdr.tryCleanupPods();

    verify(k8sClient).deletePod(createdPod1.getMetadata().getName(), false);
    verify(k8sClient).deletePod(createdPod2.getMetadata().getName(), false);
  }

  @Test
  public void shouldNotDeleteUnwantedStyxPodsIfDebugEnabled() {
    when(debug.get()).thenReturn(true);

    final Pod createdPod1 = createPod(WORKFLOW_INSTANCE, RUN_SPEC, SECRET_SPEC);
    final Pod createdPod2 = createPod(WORKFLOW_INSTANCE_2, RUN_SPEC_2, SECRET_SPEC);

    podList.setItems(Arrays.asList(createdPod1, createdPod2));
    when(k8sClient.getPod(RUN_SPEC.executionId())).thenReturn(Optional.of(createdPod1));
    when(k8sClient.getPod(RUN_SPEC_2.executionId())).thenReturn(Optional.of(createdPod2));
    createdPod1.setStatus(new PodStatusBuilder().withContainerStatuses().build());
    createdPod2.setStatus(new PodStatusBuilder().withContainerStatuses().build());

    kdr.tryCleanupPods();

    verify(k8sClient, never()).deletePod(any(), anyBoolean());
  }

  @Test
  public void shouldNotDeleteUnwantedNonStyxPods() {
    final Pod createdPod1 = createPod(WORKFLOW_INSTANCE, RUN_SPEC, SECRET_SPEC);
    final Pod createdPod2 = createPod(WORKFLOW_INSTANCE_2, RUN_SPEC_2, SECRET_SPEC);

    createdPod1.getMetadata().getAnnotations().remove("styx-workflow-instance");
    createdPod2.getMetadata().getAnnotations().remove("styx-workflow-instance");

    podList.setItems(Arrays.asList(createdPod1, createdPod2));

    kdr.tryCleanupPods();

    verify(k8sClient, never()).deletePod(any(), anyBoolean());
  }

  @Test
  public void shouldNotDeleteWantedStyxPods() {
    final Pod createdPod1 = createPod(WORKFLOW_INSTANCE, RUN_SPEC, SECRET_SPEC);
    final Pod createdPod2 = createPod(WORKFLOW_INSTANCE_2, RUN_SPEC_2, SECRET_SPEC);

    createdPod1.setStatus(podStatus1);
    createdPod2.setStatus(podStatus2);
    podList.setItems(Arrays.asList(createdPod1, createdPod2));

    setupActiveInstances(RunState.State.RUNNING, RUN_SPEC.executionId(), RUN_SPEC_2.executionId());

    kdr.tryCleanupPods();

    verify(k8sClient, never()).deletePod(any(), anyBoolean());
  }

  private Map<WorkflowInstance, RunState> setupActiveInstances(RunState.State state, String podName1, String podName2) {
    StateData stateData = StateData.newBuilder().executionId(podName1).build();
    StateData stateData2 = StateData.newBuilder().executionId(podName2).build();
    Map<WorkflowInstance, RunState> map = new HashMap<>();
    RunState runState = RunState.create(WORKFLOW_INSTANCE, state, stateData);
    RunState runState2 = RunState.create(WORKFLOW_INSTANCE_2, state, stateData2);
    map.put(WORKFLOW_INSTANCE, runState);
    map.put(WORKFLOW_INSTANCE_2, runState2);
    when(stateManager.getActiveState(any())).thenAnswer(a ->
        Optional.ofNullable(map.get(a.<WorkflowInstance>getArgument(0))));
    return map;
  }

  private static Pod createPod(WorkflowInstance workflowInstance,
                               DockerRunner.RunSpec runSpec,
                               KubernetesSecretSpec secretSpec) {
    return KubernetesDockerRunner
        .createPod(workflowInstance, runSpec, secretSpec, STYX_ENVIRONMENT, PodMutator.NOOP,
            Collections.emptyMap());
  }
}
