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
import static java.util.Optional.empty;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.api.services.iam.v1.model.ServiceAccountKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.styx.ServiceAccountKeyManager;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.StateManager.IsClosed;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.testdata.TestData;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.DoneableSecret;
import io.fabric8.kubernetes.api.model.ListMeta;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.dsl.Watchable;
import java.io.IOException;
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
  private static final String SERVICE_ACCOUNT = "sa@example.com";
  private static final WorkflowInstance WORKFLOW_INSTANCE = WorkflowInstance.create(TestData.WORKFLOW_ID, "foo");
  private static final RunSpec RUN_SPEC = RunSpec.simple("busybox");
  private static final RunSpec RUN_SPEC_WITH_SECRET = RunSpec.create("busybox",
                                                                     ImmutableList.copyOf(new String[0]),
                                                                     false,
                                                                     Optional.of(WorkflowConfiguration.Secret.create("secret1", "/etc/secret")),
                                                                     empty(),
                                                                     empty());
  private static final RunSpec RUN_SPEC_WITH_SA = RunSpec.create("busybox",
                                                                 ImmutableList.copyOf(new String[0]),
                                                                 false,
                                                                 empty(),
                                                                 Optional.of(SERVICE_ACCOUNT),
                                                                 empty());

  @Mock NamespacedKubernetesClient k8sClient;

  @Mock ServiceAccountKeyManager serviceAccountKeyManager;

  @Mock MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> pods;
  @Mock MixedOperation<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>> secrets;
  @Mock Resource<Secret, DoneableSecret> namedResource;

  @Mock PodList podList;
  @Mock ListMeta listMeta;
  @Mock Watchable<Watch, Watcher<Pod>> podWatchable;
  @Mock Watch watch;
  @Captor ArgumentCaptor<Watcher<Pod>> watchCaptor;
  @Captor ArgumentCaptor<Secret> secretCaptor;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  ServiceAccountKey serviceAccountJsonKey = new ServiceAccountKey();
  ServiceAccountKey serviceAccountP12Key = new ServiceAccountKey();

  Pod createdPod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC);
  StateManager stateManager = Mockito.spy(new SyncStateManager());
  Stats stats = Mockito.mock(Stats.class);

  KubernetesDockerRunner kdr;
  Watcher<Pod> podWatcher;

  @Before
  public void setUp() throws Exception {
    when(k8sClient.inNamespace(any(String.class))).thenReturn(k8sClient);
    when(k8sClient.pods()).thenReturn(pods);

    // pods().list().getMetadata().getResourceVersion()
    when(pods.list()).thenReturn(podList);
    when(podList.getItems()).thenReturn(ImmutableList.of(createdPod));
    when(podList.getMetadata()).thenReturn(listMeta);
    when(listMeta.getResourceVersion()).thenReturn("1000");

    when(pods.withResourceVersion("1000")).thenReturn(podWatchable);
    when(podWatchable.watch(watchCaptor.capture())).thenReturn(watch);

    when(serviceAccountKeyManager.createJsonKey(anyString())).thenReturn(serviceAccountJsonKey);
    when(serviceAccountKeyManager.createJsonKey(anyString())).thenReturn(serviceAccountP12Key);

    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountKeyManager);
    kdr.init();
    kdr.restore();

    podWatcher = watchCaptor.getValue();

    Map<String, String> annotations = new HashMap<>();
    annotations.put(KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION, WORKFLOW_INSTANCE.toKey());
    createdPod.getMetadata().setAnnotations(annotations);
    createdPod.getMetadata().setName(POD_NAME);
    createdPod.getMetadata().setResourceVersion("1001");

    StateData stateData = StateData.newBuilder().executionId(POD_NAME).build();
    stateManager.initialize(RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTED, stateData));

    when(pods.create(any(Pod.class))).thenReturn(createdPod);

    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC);
    stateManager.receive(Event.started(WORKFLOW_INSTANCE));
  }

  @After
  public void tearDown() throws Exception {
    kdr.close();
  }

  @Test(expected = InvalidExecutionException.class)
  public void shouldThrowIfSecretNotExist() throws IOException, StateManager.IsClosed {
    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(null);
    when(k8sClient.secrets()).thenReturn(secrets);
    Pod createdPod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SECRET);
    when(pods.create(any(Pod.class))).thenReturn(createdPod);

    stateManager.receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(0)));
    stateManager.receive(Event.success(WORKFLOW_INSTANCE));
    kdr.close();

    // Start a new runner
    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountKeyManager);
    kdr.init();
    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SECRET);
  }

  @Test
  public void shouldMountSecret() throws IOException, StateManager.IsClosed {
    final Pod pod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SECRET);
    assertThat(pod.getSpec().getVolumes().size(), is(1));
    assertThat(pod.getSpec().getVolumes().get(0).getName(),
               is(RUN_SPEC_WITH_SECRET.secret().get().name()));
    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getMountPath(),
               is(RUN_SPEC_WITH_SECRET.secret().get().mountPath()));
    assertThat(pod.getSpec().getContainers().get(0).getVolumeMounts().get(0).getName(),
               is(RUN_SPEC_WITH_SECRET.secret().get().name()));
  }

  @Test
  public void shouldMountServiceAccount() throws IOException, StateManager.IsClosed {
    final Pod pod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA);
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
  public void shouldRunIfSecretExists() throws IOException, StateManager.IsClosed {
    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(new SecretBuilder().build());
    when(k8sClient.secrets()).thenReturn(secrets);
    Pod createdPod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SECRET);
    when(pods.create(any(Pod.class))).thenReturn(createdPod);

    stateManager.receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(0)));
    stateManager.receive(Event.success(WORKFLOW_INSTANCE));
    kdr.close();

    // Start a new runner
    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountKeyManager);
    StateData stateData = StateData.newBuilder().executionId(POD_NAME).build();
    stateManager.initialize(RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTED, stateData));
    kdr.init();
    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SECRET);
    stateManager.receive(Event.started(WORKFLOW_INSTANCE));
  }

  @Test
  public void shouldCreateSASecret() throws StateManager.IsClosed, IOException {
    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(null);
    when(k8sClient.secrets()).thenReturn(secrets);

    when(serviceAccountKeyManager.serviceAccountExists(SERVICE_ACCOUNT)).thenReturn(true);

    ServiceAccountKey jsonKey = new ServiceAccountKey();
    jsonKey.setName("key.json");
    jsonKey.setPrivateKeyData("json-private-key-data");
    ServiceAccountKey p12Key = new ServiceAccountKey();
    p12Key.setName("key.p12");
    p12Key.setPrivateKeyData("p12-private-key-data");
    when(serviceAccountKeyManager.createJsonKey(any(String.class))).thenReturn(jsonKey);
    when(serviceAccountKeyManager.createP12Key(any(String.class))).thenReturn(p12Key);

    Pod createdPod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA);
    when(pods.create(any(Pod.class))).thenReturn(createdPod);

    stateManager.receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(0)));
    stateManager.receive(Event.success(WORKFLOW_INSTANCE));
    kdr.close();

    // Start a new runner
    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountKeyManager);
    StateData stateData = StateData.newBuilder().executionId(POD_NAME).build();
    stateManager.initialize(RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTED, stateData));
    kdr.init();
    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA);
    verify(secrets).create(secretCaptor.capture());

    final Secret createdSecret = secretCaptor.getValue();
    assertThat(createdSecret.getMetadata().getAnnotations(), hasEntry("styx-wf-sa", SERVICE_ACCOUNT));
    assertThat(createdSecret.getData(), hasEntry("styx-wf-sa.json", jsonKey.getPrivateKeyData()));
    assertThat(createdSecret.getData(), hasEntry("styx-wf-sa.p12", p12Key.getPrivateKeyData()));
  }

  @Test
  public void shouldRunIfSASecretExists() throws StateManager.IsClosed, IOException {
    stateManager.receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(0)));
    stateManager.receive(Event.success(WORKFLOW_INSTANCE));
    kdr.close();

    final String jsonKeyId = "json-key";
    final String p12KeyId = "p12-key";

    final String jsonKeyName = keyName(SERVICE_ACCOUNT, jsonKeyId);
    final String p12KeyName = keyName(SERVICE_ACCOUNT, p12KeyId);

    final String jsonKeyData = "json-private-key-data";
    final String p12KeyData = "p12-private-key-data";

    final ObjectMeta metadata = new ObjectMeta();
    metadata.setAnnotations(ImmutableMap.of(
        "styx-wf-sa", SERVICE_ACCOUNT,
        "styx-wf-sa-json-key-name", jsonKeyName,
        "styx-wf-sa-p12-key-name", p12KeyName));

    final Secret secret = new SecretBuilder()
        .withMetadata(metadata)
        .withData(ImmutableMap.of(
            "styx-wf-sa.json", jsonKeyData,
            "styx-wf-sa.p12", p12KeyData))
        .build();

    when(serviceAccountKeyManager.serviceAccountExists(SERVICE_ACCOUNT)).thenReturn(true);
    when(serviceAccountKeyManager.keyExists(jsonKeyName)).thenReturn(true);
    when(serviceAccountKeyManager.keyExists(p12KeyName)).thenReturn(true);

    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(secret);

    when(k8sClient.secrets()).thenReturn(secrets);
    Pod createdPod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA);
    when(pods.create(any(Pod.class))).thenReturn(createdPod);

    // Start a new runner
    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountKeyManager);
    StateData stateData = StateData.newBuilder().executionId(POD_NAME).build();
    stateManager.initialize(RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTED, stateData));
    kdr.init();
    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA);

    verify(secrets, never()).create(any(Secret.class));
  }

  public String keyName(String serviceAccount, String keyId) {
    return "projects/-/serviceAccounts/" + serviceAccount + "/keys/" + keyId;
  }

  @Test
  public void shouldNotRunIfServiceAccountDoesNotExist() throws StateManager.IsClosed, IOException {
    stateManager.receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(0)));
    stateManager.receive(Event.success(WORKFLOW_INSTANCE));
    kdr.close();

    when(serviceAccountKeyManager.serviceAccountExists(SERVICE_ACCOUNT)).thenReturn(false);

    // Start a new runner
    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountKeyManager);
    StateData stateData = StateData.newBuilder().executionId(POD_NAME).build();
    stateManager.initialize(RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTED, stateData));
    kdr.init();

    exception.expect(InvalidExecutionException.class);
    exception.expectMessage("Referenced service account '" + SERVICE_ACCOUNT + "' was not found");

    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA);
  }

  @Test
  public void shouldCreateNewServiceAccountKeysIfKeysAreDeleted() throws IOException, IsClosed {
    stateManager.receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(0)));
    stateManager.receive(Event.success(WORKFLOW_INSTANCE));
    kdr.close();

    final ObjectMeta metadata = new ObjectMeta();
    metadata.setAnnotations(ImmutableMap.of("styx-wf-sa", SERVICE_ACCOUNT));

    final String jsonKeyName = "json-key";
    final String p12KeyName = "p12-key";
    final String newJsonKeyName = "new-json-key";
    final String newP12KeyName = "new-p12-key";

    final ServiceAccountKey jsonKey = new ServiceAccountKey()
        .setName(jsonKeyName)
        .setPrivateKeyData("json-private-key-data");

    final ServiceAccountKey p12Key = new ServiceAccountKey()
        .setName(p12KeyName)
        .setPrivateKeyData("p12-private-key-data");

    final ServiceAccountKey newJsonKey = new ServiceAccountKey()
        .setName(newJsonKeyName)
        .setPrivateKeyData("new-json-private-key-data");

    final ServiceAccountKey newP12Key = new ServiceAccountKey()
        .setName(newP12KeyName)
        .setPrivateKeyData("new-p12-private-key-data");


    when(serviceAccountKeyManager.serviceAccountExists(SERVICE_ACCOUNT)).thenReturn(true);
    when(serviceAccountKeyManager.keyExists(keyName(SERVICE_ACCOUNT, jsonKeyName))).thenReturn(false);
    when(serviceAccountKeyManager.keyExists(keyName(SERVICE_ACCOUNT, p12KeyName))).thenReturn(false);

    when(serviceAccountKeyManager.createJsonKey(any(String.class))).thenReturn(newJsonKey);
    when(serviceAccountKeyManager.createP12Key(any(String.class))).thenReturn(newP12Key);

    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(new SecretBuilder()
        .withMetadata(metadata)
        .withData(ImmutableMap.of(
            "styx-wf-sa.json", Json.OBJECT_MAPPER.writeValueAsString(jsonKey),
            "styx-wf-sa.p12", Json.OBJECT_MAPPER.writeValueAsString(p12Key)))
        .build());

    when(k8sClient.secrets()).thenReturn(secrets);
    Pod createdPod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA);
    when(pods.create(any(Pod.class))).thenReturn(createdPod);

    // Start a new runner
    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountKeyManager);
    StateData stateData = StateData.newBuilder().executionId(POD_NAME).build();
    stateManager.initialize(RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTED, stateData));
    kdr.init();

    kdr.start(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA);

    verify(secrets).create(secretCaptor.capture());

    final Secret createdSecret = secretCaptor.getValue();
    assertThat(createdSecret.getMetadata().getAnnotations(), hasEntry("styx-wf-sa", SERVICE_ACCOUNT));
    assertThat(createdSecret.getData(), hasEntry("styx-wf-sa.json", newJsonKey.getPrivateKeyData()));
    assertThat(createdSecret.getData(), hasEntry("styx-wf-sa.p12", newP12Key.getPrivateKeyData()));
  }

  @Test
  public void shouldNotRunIfSecretHasManagedServiceAccountKeySecretNamePrefix() throws StateManager.IsClosed, IOException {
    stateManager.receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(0)));
    stateManager.receive(Event.success(WORKFLOW_INSTANCE));
    kdr.close();

    // Start a new runner
    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountKeyManager);
    StateData stateData = StateData.newBuilder().executionId(POD_NAME).build();
    stateManager.initialize(RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTED, stateData));
    kdr.init();

    final String secret = "styx-wf-sa-keys-foo";

    exception.expect(InvalidExecutionException.class);
    exception.expectMessage("Referenced secret '" + secret + "' has the managed service account key secret name prefix");
    kdr.start(WORKFLOW_INSTANCE, RunSpec.create("busybox",
        ImmutableList.of(),
        false,
        Optional.of(WorkflowConfiguration.Secret.create(secret, "/foo/bar")),
        Optional.empty(),
        empty()));

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
    StateData stateData = StateData.newBuilder().executionId(POD_NAME).build();
    stateManager.initialize(RunState.create(WORKFLOW_INSTANCE, RunState.State.SUBMITTED, stateData));

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
    createdPod.setStatus(terminated("Succeeded", 20, null));

    podWatcher.eventReceived(Watcher.Action.MODIFIED, createdPod);

    assertThat(stateManager.get(WORKFLOW_INSTANCE).state(), is(RunState.State.RUNNING));
  }

  @Test
  public void shouldPollPodStatusAndEmitEventsOnRestore() throws Exception {
    // Stop the runner and change the pod status to terminated while styx is "down"
    kdr.close();
    createdPod.setStatus(terminated("Succeeded", 20, null));

    // Start a new runner
    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountKeyManager);
    kdr.init();

    // Make the runner poll states for all pods
    kdr.restore();

    // Verify that the runner polled and found out that the pods is terminated
    verify(stateManager).receive(Event.terminate(WORKFLOW_INSTANCE, Optional.of(20)));
    assertThat(stateManager.get(WORKFLOW_INSTANCE).data().lastExit(), hasValue(20));
  }

  @Test
  public void shouldRegularlyPollPodStatusAndEmitEvents() throws Exception {
    createdPod.setStatus(running(/* ready= */ true));

    // Set up a runner with short poll interval to avoid this test having to wait a long time for the poll
    kdr.close();
    kdr = new KubernetesDockerRunner(k8sClient, stateManager, stats, serviceAccountKeyManager, 1);
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
