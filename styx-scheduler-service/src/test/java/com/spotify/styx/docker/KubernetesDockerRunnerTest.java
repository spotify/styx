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

import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.podStatusNoContainer;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.running;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.terminated;
import static com.spotify.styx.docker.KubernetesPodEventTranslatorTest.waiting;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.testdata.TestData;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.ListMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.ClientMixedOperation;
import io.fabric8.kubernetes.client.dsl.ClientPodResource;
import io.fabric8.kubernetes.client.dsl.Watchable;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
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
  private static final WorkflowInstance WORKFLOW_INSTANCE = WorkflowInstance.create(TestData.WORKFLOW_ID, "foo");
  private static final RunSpec RUN_SPEC = RunSpec.create("busybox", ImmutableList.of(), Optional.empty());

  @Mock KubernetesClient k8sClient;

  @Mock ClientMixedOperation<Pod, PodList, DoneablePod, ClientPodResource<Pod, DoneablePod>> pods;

  @Mock PodList podList;
  @Mock ListMeta listMeta;
  @Mock Watchable<Watch, Watcher<Pod>> podWatchable;
  @Mock Watch watch;
  @Captor ArgumentCaptor<Watcher<Pod>> watchCaptor;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  Pod createdPod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC);
  StateManager stateManager = new SyncStateManager();
  Stats stats = Mockito.mock(Stats.class);

  KubernetesDockerRunner kdr;
  Watcher<Pod> podWatcher;

  @Before
  public void setUp() throws Exception {
    when(k8sClient.inNamespace(any(String.class))).thenReturn(k8sClient);
    when(k8sClient.pods()).thenReturn(pods);

    // pods().list().getMetadata().getResourceVersion()
    when(pods.list()).thenReturn(podList);
    when(podList.getMetadata()).thenReturn(listMeta);
    when(listMeta.getResourceVersion()).thenReturn("1000");

    when(pods.withResourceVersion("1000")).thenReturn(podWatchable);
    when(podWatchable.watch(watchCaptor.capture())).thenReturn(watch);

    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats);
    kdr.init();

    podWatcher = watchCaptor.getValue();

    Map<String, String> annotations = new HashMap<>();
    annotations.put(KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION, WORKFLOW_INSTANCE.toKey());
    createdPod.getMetadata().setAnnotations(annotations);
    createdPod.getMetadata().setName(POD_NAME);
    createdPod.getMetadata().setResourceVersion("1001");

    stateManager.initialize(RunState.create(WORKFLOW_INSTANCE, POD_NAME, RunState.State.SUBMITTED));

    when(pods.create(any(Pod.class))).thenReturn(createdPod);

    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC);
    stateManager.receive(Event.started(WORKFLOW_INSTANCE));
  }

  @After
  public void tearDown() throws Exception {
    kdr.close();
  }

  @Test
  public void shouldCompleteWithStatusCodeOnSucceeded() throws Exception {
    createdPod.setStatus(terminated("Succeeded", 20));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    assertThat(stateManager.get(WORKFLOW_INSTANCE).lastExit(), is(20));
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

    verify(stats, times(1)).pullImageError();
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
  public void shouldGenerateStartedWhenContainerIsReady() throws Exception {
    stateManager.initialize(RunState.create(WORKFLOW_INSTANCE, POD_NAME, RunState.State.SUBMITTED));
    createdPod.setStatus(running(/* ready= */ false));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);
    assertThat(stateManager.get(WORKFLOW_INSTANCE).state(), is(RunState.State.SUBMITTED));

    createdPod.setStatus(running(/* ready= */ true));
    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);
    assertThat(stateManager.get(WORKFLOW_INSTANCE).state(), is(RunState.State.RUNNING));
  }

  @Test
  public void shouldDiscardChangesForOldExecutions() throws Exception {
    // simulate event from different pod, but still with the same workflow instance annotation
    createdPod.getMetadata().setName(POD_NAME + "-other");
    createdPod.setStatus(terminated("Succeeded", 20));

    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    assertThat(stateManager.get(WORKFLOW_INSTANCE).state(), is(RunState.State.RUNNING));
  }
}
