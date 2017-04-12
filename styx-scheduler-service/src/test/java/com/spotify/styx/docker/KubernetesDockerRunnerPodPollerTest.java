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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.testdata.TestData;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.ClientMixedOperation;
import io.fabric8.kubernetes.client.dsl.ClientPodResource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KubernetesDockerRunnerPodPollerTest {

  private static final String POD_NAME = "test-pod-1";
  private static final String POD_NAME_2 = "test-pod-2";
  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "foo");
  private static final WorkflowInstance WORKFLOW_INSTANCE_2 =
      WorkflowInstance.create(TestData.WORKFLOW_ID_2, "bar");
  private static final DockerRunner.RunSpec RUN_SPEC =
      DockerRunner.RunSpec.simple("busybox");

  @Mock
  KubernetesClient k8sClient;
  @Mock
  ClientMixedOperation<Pod, PodList, DoneablePod, ClientPodResource<Pod, DoneablePod>> pods;
  PodList podList;

  @Mock
  StateManager stateManager;
  @Mock
  Stats stats;

  KubernetesDockerRunner kdr;

  @Before
  public void setUp() throws Exception {
    when(k8sClient.inNamespace(any(String.class))).thenReturn(k8sClient);
    when(k8sClient.pods()).thenReturn(pods);

    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats);
    podList = new PodList();
  }

  @Test
  public void shouldSendRunErrorWhenPodForRunningWFIDoesntExist() throws Exception {
    Pod createdPod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC);
    podList.setItems(Arrays.asList(createdPod));
    when(k8sClient.pods().list()).thenReturn(podList);
    setupActiveInstances(RunState.State.RUNNING);

    kdr.pollPods();

    verify(stateManager, times(1)).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE_2, "No pod associated with this instance"));
  }

  @Test
  public void shouldNotSendRunErrorWhenPodForRunningWFIExists() throws Exception {
    Pod createdPod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC);
    Pod createdPod2 = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE_2, RUN_SPEC);
    podList.setItems(Arrays.asList(createdPod, createdPod2));
    when(k8sClient.pods().list()).thenReturn(podList);
    setupActiveInstances(RunState.State.RUNNING);

    kdr.pollPods();

    verify(stateManager, never()).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE, "No pod associated with this instance"));
    verify(stateManager, never()).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE_2, "No pod associated with this instance"));
  }

  @Test
  public void shouldHandleEmptyPodList() throws Exception {
    when(k8sClient.pods().list()).thenReturn(podList);
    setupActiveInstances(RunState.State.RUNNING);

    kdr.pollPods();

    verify(stateManager, times(1)).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE, "No pod associated with this instance"));
    verify(stateManager, times(1)).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE_2, "No pod associated with this instance"));
  }

  @Test
  public void shouldNotSendErrorEventForInstancesNotInRunningState() throws Exception {
    when(k8sClient.pods().list()).thenReturn(podList);
    setupActiveInstances(RunState.State.SUBMITTED);

    kdr.pollPods();

    verify(stateManager, never()).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE, "No pod associated with this instance"));
    verify(stateManager, never()).receiveIgnoreClosed(
        Event.runError(WORKFLOW_INSTANCE_2, "No pod associated with this instance"));
  }

  private void setupActiveInstances(RunState.State state) {
    StateData stateData = StateData.newBuilder().executionId(POD_NAME).build();
    StateData stateData2 = StateData.newBuilder().executionId(POD_NAME_2).build();
    Map<WorkflowInstance, RunState> map = new HashMap<>();
    map.put(WORKFLOW_INSTANCE,
        RunState.create(WORKFLOW_INSTANCE, state, stateData));
    map.put(WORKFLOW_INSTANCE_2,
        RunState.create(WORKFLOW_INSTANCE_2, state, stateData2));

    when(stateManager.activeStates()).thenReturn(map);
  }
}
