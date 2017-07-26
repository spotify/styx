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

package com.spotify.styx.docker;

import static com.spotify.styx.testdata.TestData.WORKFLOW_ID;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Optional.empty;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.services.iam.v1.model.ServiceAccountKey;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.spotify.styx.ServiceAccountKeyManager;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.docker.KubernetesDockerRunner.KubernetesSecretSpec;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.StateManager;
import io.fabric8.kubernetes.api.model.DoneablePod;
import io.fabric8.kubernetes.api.model.DoneableSecret;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.PodResource;
import io.fabric8.kubernetes.client.dsl.Resource;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.IntSummaryStatistics;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
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
public class KubernetesGCPServiceAccountSecretManagerTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  private static final String SERVICE_ACCOUNT = "sa@example.com";

  private final static long SECRET_EPOCH = 4711;
  private final static long PAST_SECRET_EPOCH = 4710;

  private final static Clock CLOCK = Clock.fixed(Instant.now(), ZoneOffset.UTC);

  private static final Instant EXPIRED_CREATION_TIMESTAMP =
      CLOCK.instant().minus(Duration.ofDays(7).plusHours(24).plusSeconds(1));

  private static final WorkflowInstance WORKFLOW_INSTANCE = WorkflowInstance.create(WORKFLOW_ID, "foo");

  private static final RunSpec RUN_SPEC_WITH_SA = RunSpec.create("eid", "busybox",
      ImmutableList.copyOf(new String[0]),
      false,
      empty(),
      Optional.of(SERVICE_ACCOUNT),
      empty(),
      empty());

  private static final String TEST_EXEC_ID = "test-exec-id";

  private ExecutorService executor;

  @Mock NamespacedKubernetesClient k8sClient;

  @Mock ServiceAccountKeyManager serviceAccountKeyManager;

  @Mock MixedOperation<Pod, PodList, DoneablePod, PodResource<Pod, DoneablePod>> pods;
  @Mock MixedOperation<Secret, SecretList, DoneableSecret, Resource<Secret, DoneableSecret>> secrets;
  @Mock Resource<Secret, DoneableSecret> namedResource;
  @Mock SecretList secretList;

  @Mock PodList podList;
  @Captor ArgumentCaptor<Secret> secretCaptor;

  KubernetesGCPServiceAccountSecretManager sut;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    executor = Executors.newCachedThreadPool();

    when(k8sClient.inNamespace(any(String.class))).thenReturn(k8sClient);
    when(k8sClient.pods()).thenReturn(pods);
    when(pods.list()).thenReturn(podList);
    when(podList.getItems()).thenReturn(ImmutableList.of());

    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(null);
    when(k8sClient.secrets()).thenReturn(secrets);

    sut = new KubernetesGCPServiceAccountSecretManager(
        k8sClient, serviceAccountKeyManager, (now, sa) -> SECRET_EPOCH, CLOCK);
  }

  @After
  public void tearDown() throws Exception {
    executor.shutdownNow();
  }

  @Test
  public void shouldCreateServiceAccountKeysAndSecret() throws StateManager.IsClosed, IOException {

    when(serviceAccountKeyManager.serviceAccountExists(SERVICE_ACCOUNT)).thenReturn(true);

    ServiceAccountKey jsonKey = new ServiceAccountKey();
    jsonKey.setName("key.json");
    jsonKey.setPrivateKeyData("json-private-key-data");
    ServiceAccountKey p12Key = new ServiceAccountKey();
    p12Key.setName("key.p12");
    p12Key.setPrivateKeyData("p12-private-key-data");
    when(serviceAccountKeyManager.createJsonKey(any(String.class))).thenReturn(jsonKey);
    when(serviceAccountKeyManager.createP12Key(any(String.class))).thenReturn(p12Key);

    sut.ensureServiceAccountKeySecret(WORKFLOW_ID.toString(), SERVICE_ACCOUNT);

    verify(serviceAccountKeyManager).createJsonKey(SERVICE_ACCOUNT);
    verify(serviceAccountKeyManager).createP12Key(SERVICE_ACCOUNT);
    verify(secrets).create(secretCaptor.capture());

    final Secret createdSecret = secretCaptor.getValue();
    assertThat(createdSecret.getMetadata().getAnnotations(), hasEntry("styx-wf-sa", SERVICE_ACCOUNT));
    assertThat(createdSecret.getData(), hasEntry("styx-wf-sa.json", jsonKey.getPrivateKeyData()));
    assertThat(createdSecret.getData(), hasEntry("styx-wf-sa.p12", p12Key.getPrivateKeyData()));
  }

  @Test
  public void shouldNotConcurrentlyCreateServiceAccountKeysAndSecrets()
      throws StateManager.IsClosed, IOException, ExecutionException, InterruptedException {

    final ServiceAccountKey jsonKey = new ServiceAccountKey();
    jsonKey.setName("key.json");
    jsonKey.setPrivateKeyData("json-private-key-data");
    final ServiceAccountKey p12Key = new ServiceAccountKey();
    p12Key.setName("key.p12");
    p12Key.setPrivateKeyData("p12-private-key-data");

    when(namedResource.get()).thenReturn(null);

    CompletableFuture<Boolean> accountExistsFuture = new CompletableFuture<>();

    // Make the service account existence check block
    when(serviceAccountKeyManager.serviceAccountExists(SERVICE_ACCOUNT)).thenAnswer(a -> accountExistsFuture.get());

    when(serviceAccountKeyManager.createJsonKey(any(String.class))).thenReturn(jsonKey);
    when(serviceAccountKeyManager.createP12Key(any(String.class))).thenReturn(p12Key);

    // Run two concurrent requests for the same service account secret
    final Future<String> f1 = executor.submit(
        () -> sut.ensureServiceAccountKeySecret(WORKFLOW_ID.toString(), SERVICE_ACCOUNT));

    final Future<String> f2 = executor.submit(
        () -> sut.ensureServiceAccountKeySecret(WORKFLOW_ID.toString(), SERVICE_ACCOUNT));

    // Wait for a call to the blocking service account existence method
    verify(serviceAccountKeyManager, timeout(30_000)).serviceAccountExists(SERVICE_ACCOUNT);

    Thread.sleep(5000);

    // Verify that we only got one call
    verify(serviceAccountKeyManager, times(1)).serviceAccountExists(SERVICE_ACCOUNT);

    // Unblock the calling thread
    accountExistsFuture.complete(true);

    // Wait for both requests to finish
    final String secret1 = f1.get();
    final String secret2 = f2.get();

    // Check that only one secret was created
    verify(secrets, times(1)).create(secretCaptor.capture());
    final Secret createdSecret = secretCaptor.getValue();

    // Check that both requests returned the same secret name
    assertThat(secret1, is(createdSecret.getMetadata().getName()));
    assertThat(secret2, is(createdSecret.getMetadata().getName()));

    // Check that only one json and one p12 keys were created
    verify(serviceAccountKeyManager, times(1)).createJsonKey(SERVICE_ACCOUNT);
    verify(serviceAccountKeyManager, times(1)).createP12Key(SERVICE_ACCOUNT);
  }


  @Test
  public void shouldRemoveServiceAccountSecretsAndKeys() throws Exception {
    final Secret secret = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, SECRET_EPOCH, "json-key", "p12-key", EXPIRED_CREATION_TIMESTAMP.toString());

    when(k8sClient.secrets()).thenReturn(secrets);
    when(secrets.list()).thenReturn(secretList);
    when(secretList.getItems()).thenReturn(ImmutableList.of(secret));

    // Verify that an unused service account key secret is deleted
    when(podList.getItems()).thenReturn(ImmutableList.of());
    sut.cleanup();
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "json-key"));
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "p12-key"));
    verify(secrets).delete(secret);
  }

  @Test
  @Parameters({"Failed", "Succeeded"})
  public void shouldRemoveServiceAccountSecretsAndKeysUsedByTerminatedPods(String phase) throws Exception {
    final Secret secret = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, SECRET_EPOCH, "json-key", "p12-key", EXPIRED_CREATION_TIMESTAMP.toString());

    when(k8sClient.secrets()).thenReturn(secrets);
    when(secrets.list()).thenReturn(secretList);
    when(secretList.getItems()).thenReturn(ImmutableList.of(secret));

    final KubernetesSecretSpec secretSpec = KubernetesSecretSpec.builder()
        .serviceAccountSecret(secret.getMetadata().getName())
        .build();
    final Pod pod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA, secretSpec);

    final PodStatus podStatus = podStatus(phase);
    pod.setStatus(podStatus);

    when(podList.getItems()).thenReturn(ImmutableList.of());
    sut.cleanup();
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "json-key"));
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "p12-key"));
    verify(secrets).delete(secret);
  }

  @Test
  public void shouldNotRemoveServiceAccountSecretsAndKeysInUse() throws Exception {
    final Secret secret = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, SECRET_EPOCH, "json-key", "p12-key", EXPIRED_CREATION_TIMESTAMP.toString());

    when(k8sClient.secrets()).thenReturn(secrets);
    when(secrets.list()).thenReturn(secretList);
    when(secretList.getItems()).thenReturn(ImmutableList.of(secret));

    final KubernetesSecretSpec secretSpec = KubernetesSecretSpec.builder()
        .serviceAccountSecret(secret.getMetadata().getName())
        .build();
    final Pod pod = KubernetesDockerRunner.createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA, secretSpec);
    pod.setStatus(podStatus("Running"));
    when(podList.getItems()).thenReturn(ImmutableList.of(pod));
    sut.cleanup();
    verify(serviceAccountKeyManager, never()).deleteKey(anyString());
    verify(secrets, never()).delete(any(Secret.class));
  }

  @Test
  public void shouldRemoveServiceAccountSecretsInPastEpoch() throws Exception {
    final Secret secret = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, PAST_SECRET_EPOCH, "old-json-key", "old-p12-key", EXPIRED_CREATION_TIMESTAMP.toString());

    when(k8sClient.secrets()).thenReturn(secrets);
    when(secrets.list()).thenReturn(secretList);
    when(secretList.getItems()).thenReturn(ImmutableList.of(secret));
    when(podList.getItems()).thenReturn(ImmutableList.of());

    sut.cleanup();

    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "old-json-key"));
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "old-p12-key"));
    verify(secrets).delete(secret);
  }

  @Test
  public void shouldNotRemoveServiceAccountSecretsCreatedWithin48Hours() throws Exception {
    final String creationTimestamp = CLOCK.instant().minus(Duration.ofHours(48)).toString();
    final Secret secret1 = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, PAST_SECRET_EPOCH, "json-key-1", "p12-key-1", creationTimestamp);
    final Secret secret2 = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, SECRET_EPOCH, "json-key-2", "p12-key-2", creationTimestamp);

    when(k8sClient.secrets()).thenReturn(secrets);
    when(secrets.list()).thenReturn(secretList);
    when(secretList.getItems()).thenReturn(ImmutableList.of(secret1, secret2));

    sut.cleanup();

    verify(serviceAccountKeyManager, never()).deleteKey(anyString());
    verify(secrets, never()).delete(any(Secret.class));
  }

  @Test
  public void shouldHandleTooManyKeysCreated() throws IOException {
    when(serviceAccountKeyManager.serviceAccountExists(anyString())).thenReturn(true);

    final GoogleJsonResponseException resourceExhausted = new GoogleJsonResponseException(
        new HttpResponseException.Builder(429, "RESOURCE_EXHAUSTED", new HttpHeaders()),
        new GoogleJsonError().set("status", "RESOURCE_EXHAUSTED"));

    doThrow(resourceExhausted).when(serviceAccountKeyManager).createJsonKey(any());
    doThrow(resourceExhausted).when(serviceAccountKeyManager).createP12Key(any());

    exception.expect(InvalidExecutionException.class);
    exception.expectMessage("Maximum number of keys on service account reached: " + SERVICE_ACCOUNT);

    sut.ensureServiceAccountKeySecret(WORKFLOW_ID.toString(), SERVICE_ACCOUNT);
  }

  @Test
  public void shouldHandlePermissionDeniedErrorsWhenDeletingServiceAccountKeys() throws Exception {
    final Secret secret = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, SECRET_EPOCH, "json-key", "p12-key", EXPIRED_CREATION_TIMESTAMP.toString());

    when(podList.getItems()).thenReturn(ImmutableList.of());
    when(k8sClient.secrets()).thenReturn(secrets);
    when(secrets.list()).thenReturn(secretList);
    when(secretList.getItems()).thenReturn(ImmutableList.of(secret));

    // Verify that the secret is delete even if we get permission denied errors on deleting the keys
    final GoogleJsonResponseException permissionDenied = new GoogleJsonResponseException(
        new HttpResponseException.Builder(403, "Forbidden", new HttpHeaders()),
        new GoogleJsonError().set("status", "PERMISSION_DENIED"));
    doThrow(permissionDenied).when(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "json-key"));
    doThrow(permissionDenied).when(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "p12-key"));

    sut.cleanup();

    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "json-key"));
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "p12-key"));
    verify(secrets).delete(secret);
  }

  @Test
  public void shouldHandleErrorsWhenDeletingServiceAccountKeys() throws Exception {
    final Secret secret1 = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, SECRET_EPOCH, "json-key-1", "p12-key-1", EXPIRED_CREATION_TIMESTAMP.toString());
    final Secret secret2 = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, SECRET_EPOCH, "json-key-2", "p12-key-2", EXPIRED_CREATION_TIMESTAMP.toString());
    final Secret secret3 = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, SECRET_EPOCH, "json-key-3", "p12-key-3", EXPIRED_CREATION_TIMESTAMP.toString());

    when(podList.getItems()).thenReturn(ImmutableList.of());
    when(k8sClient.secrets()).thenReturn(secrets);
    when(secrets.list()).thenReturn(secretList);
    when(secretList.getItems()).thenReturn(ImmutableList.of(secret1, secret2, secret3));

    when(secrets.delete(secret1)).thenThrow(new KubernetesClientException("fail delete secret1"));
    doThrow(new IOException("fail delete json-key-2")).when(serviceAccountKeyManager).deleteKey("json-key-2");
    doThrow(new IOException("fail delete p12-key-2")).when(serviceAccountKeyManager).deleteKey("p12-key-2");

    sut.cleanup();

    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "json-key-1"));
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "p12-key-1"));
    verify(secrets).delete(secret1);

    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "json-key-2"));
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "p12-key-2"));
    verify(secrets).delete(secret2);

    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "json-key-3"));
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "p12-key-3"));
    verify(secrets).delete(secret3);
  }

  @Test
  public void shouldUseExistingServiceAccountSecret() throws StateManager.IsClosed, IOException {

    final String jsonKeyId = "json-key";
    final String p12KeyId = "p12-key";

    final Secret secret = fakeServiceAccountKeySecret(SERVICE_ACCOUNT, SECRET_EPOCH, jsonKeyId, p12KeyId,
        EXPIRED_CREATION_TIMESTAMP.toString());

    when(serviceAccountKeyManager.serviceAccountExists(SERVICE_ACCOUNT)).thenReturn(true);
    when(serviceAccountKeyManager.keyExists(keyName(SERVICE_ACCOUNT, jsonKeyId))).thenReturn(true);
    when(serviceAccountKeyManager.keyExists(keyName(SERVICE_ACCOUNT, p12KeyId))).thenReturn(true);

    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(secret);

    when(k8sClient.secrets()).thenReturn(secrets);

    final String serviceAccountSecret = sut.ensureServiceAccountKeySecret(
        WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT);

    assertThat(serviceAccountSecret, is(secret.getMetadata().getName()));

    verify(secrets, never()).create(any(Secret.class));
  }

  @Test
  public void shouldFailIfServiceAccountDoesNotExist() throws StateManager.IsClosed, IOException {
    when(serviceAccountKeyManager.serviceAccountExists(SERVICE_ACCOUNT)).thenReturn(false);

    exception.expect(InvalidExecutionException.class);
    exception.expectMessage("Referenced service account " + SERVICE_ACCOUNT + " was not found");

    sut.ensureServiceAccountKeySecret(WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT);
  }

  @Test
  public void shouldCreateNewServiceAccountKeysIfKeysAreDeleted() throws IOException {

    final ObjectMeta metadata = new ObjectMeta();
    metadata.setAnnotations(ImmutableMap.of("styx-wf-sa", SERVICE_ACCOUNT));

    final String jsonKeyId = "json-key";
    final String p12KeyId = "p12-key";
    final String newJsonKeyId = "new-json-key";
    final String newP12KeyId = "new-p12-key";

    final String creationTimestamp = CLOCK.instant().minus(Duration.ofDays(1)).toString();
    final Secret secret = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, SECRET_EPOCH, jsonKeyId, p12KeyId, creationTimestamp);

    final ServiceAccountKey newJsonKey = new ServiceAccountKey()
        .setName(newJsonKeyId)
        .setPrivateKeyData("new-json-private-key-data");

    final ServiceAccountKey newP12Key = new ServiceAccountKey()
        .setName(newP12KeyId)
        .setPrivateKeyData("new-p12-private-key-data");

    when(serviceAccountKeyManager.serviceAccountExists(SERVICE_ACCOUNT)).thenReturn(true);
    when(serviceAccountKeyManager.keyExists(keyName(SERVICE_ACCOUNT, jsonKeyId))).thenReturn(false);
    when(serviceAccountKeyManager.keyExists(keyName(SERVICE_ACCOUNT, p12KeyId))).thenReturn(false);

    when(serviceAccountKeyManager.createJsonKey(any(String.class))).thenReturn(newJsonKey);
    when(serviceAccountKeyManager.createP12Key(any(String.class))).thenReturn(newP12Key);

    when(secrets.withName(any(String.class))).thenReturn(namedResource);
    when(namedResource.get()).thenReturn(secret);

    sut.ensureServiceAccountKeySecret(WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT);

    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, jsonKeyId));
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, p12KeyId));
    verify(serviceAccountKeyManager).createJsonKey(SERVICE_ACCOUNT);
    verify(serviceAccountKeyManager).createP12Key(SERVICE_ACCOUNT);
    verify(secrets).delete(secret);
    verify(secrets).create(secretCaptor.capture());

    final Secret createdSecret = secretCaptor.getValue();
    assertThat(createdSecret.getMetadata().getAnnotations(), hasEntry("styx-wf-sa", SERVICE_ACCOUNT));
    assertThat(createdSecret.getData(), hasEntry("styx-wf-sa.json", newJsonKey.getPrivateKeyData()));
    assertThat(createdSecret.getData(), hasEntry("styx-wf-sa.p12", newP12Key.getPrivateKeyData()));
  }

  @Test
  public void shouldSmearRotationWeekly() throws Exception {
    final long hours = Duration.ofDays(7).toHours();
    final int[] rotationsPerHour = new int[(int) hours];
    final int n = 10000;
    for (int i = 0; i < n; i++) {
      long prevEpoch = 0;
      for (int hour = 0; hour < hours; hour++) {
        final long nowMillis = TimeUnit.HOURS.toMillis(hour);
        final long epoch = KubernetesGCPServiceAccountSecretManager.smearedEpoch(
            nowMillis, "sa" + i + "@example.com");
        if (prevEpoch != epoch) {
          prevEpoch = epoch;
          rotationsPerHour[hour]++;
        }
      }
    }
    final IntSummaryStatistics stats = IntStream.of(rotationsPerHour).summaryStatistics();
    final double expectedMeanRotationsPerHour = n / hours;
    assertThat(stats.getAverage(), is(closeTo(expectedMeanRotationsPerHour, expectedMeanRotationsPerHour / 2)));
    assertThat((double) stats.getMax(), is(lessThan(expectedMeanRotationsPerHour * 2)));
  }

  private static Secret fakeServiceAccountKeySecret(String serviceAccount, long epoch, String jsonKeyId,
      String p12KeyId, String creationTimestamp) {
    final String jsonKeyName = keyName(serviceAccount, jsonKeyId);
    final String p12KeyName = keyName(serviceAccount, p12KeyId);

    final ObjectMeta metadata = new ObjectMeta();
    metadata.setCreationTimestamp(creationTimestamp);
    metadata.setName("styx-wf-sa-keys-" + epoch + "-" + Hashing.sha256().hashString(serviceAccount, UTF_8));
    metadata.setAnnotations(ImmutableMap.of(
        "styx-wf-sa", serviceAccount,
        "styx-wf-sa-json-key-name", jsonKeyName,
        "styx-wf-sa-p12-key-name", p12KeyName));

    return new SecretBuilder()
        .withMetadata(metadata)
        .withData(ImmutableMap.of(
            "styx-wf-sa.json", "json-private-key-data",
            "styx-wf-sa.p12", "p12-private-key-data"))
        .build();
  }

  private static String keyName(String serviceAccount, String keyId) {
    return "projects/-/serviceAccounts/" + serviceAccount + "/keys/" + keyId;
  }

  private static PodStatus podStatus(String phase) {
    final PodStatus podStatus = new PodStatus();
    podStatus.setPhase(phase);
    return podStatus;
  }
}
