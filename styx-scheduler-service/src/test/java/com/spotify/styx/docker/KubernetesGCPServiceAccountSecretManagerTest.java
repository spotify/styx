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
import com.google.common.hash.Hashing;
import com.spotify.styx.ServiceAccountKeyManager;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.docker.KubernetesDockerRunner.KubernetesSecretSpec;
import com.spotify.styx.model.WorkflowInstance;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.api.model.Status;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.IntSummaryStatistics;
import java.util.List;
import java.util.Map;
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

  private static final String SERVICE_ACCOUNT_1 = "sa1@example.com";
  private static final String SERVICE_ACCOUNT_2 = "sa2@example.com";
  private static final String SERVICE_ACCOUNT_3 = "sa3@example.com";

  private final static long SECRET_EPOCH = 4711;
  private final static long PAST_SECRET_EPOCH = 4710;

  private final static Clock CLOCK = Clock.fixed(Instant.now(), ZoneOffset.UTC);

  private static final Instant EXPIRED_CREATION_TIMESTAMP =
      CLOCK.instant().minus(Duration.ofDays(7).plusHours(24).plusSeconds(1));

  private static final WorkflowInstance WORKFLOW_INSTANCE = WorkflowInstance.create(WORKFLOW_ID, "foo");

  private static final RunSpec RUN_SPEC_WITH_SA = RunSpec.builder()
      .executionId("eid")
      .imageName("busybox")
      .serviceAccount(SERVICE_ACCOUNT)
      .build();

  private static final String STYX_ENVIRONMENT = "testing";

  private ExecutorService executor;

  @Mock Fabric8KubernetesClient k8sClient;

  @Mock ServiceAccountKeyManager serviceAccountKeyManager;

  @Mock SecretList secretList;

  @Mock PodList podList;
  @Captor ArgumentCaptor<Secret> secretCaptor;

  KubernetesGCPServiceAccountSecretManager sut;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    executor = Executors.newCachedThreadPool();

    when(k8sClient.listPods()).thenReturn(podList);
    when(podList.getItems()).thenReturn(List.of());

    when(k8sClient.getSecret(anyString())).thenReturn(Optional.empty());
    when(k8sClient.listSecrets()).thenReturn(secretList);
    when(secretList.getItems()).thenReturn(List.of());

    sut = new KubernetesGCPServiceAccountSecretManager(
        k8sClient, serviceAccountKeyManager, (now, sa) -> SECRET_EPOCH, CLOCK);
  }

  @After
  public void tearDown() {
    executor.shutdownNow();
  }

  @Test
  public void shouldCreateServiceAccountKeysAndSecret() throws IOException {
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
    verify(k8sClient).createSecret(secretCaptor.capture());

    final Secret createdSecret = secretCaptor.getValue();
    assertThat(createdSecret.getMetadata().getAnnotations(), hasEntry("styx-wf-sa", SERVICE_ACCOUNT));
    assertThat(createdSecret.getData(), hasEntry("styx-wf-sa.json", jsonKey.getPrivateKeyData()));
    assertThat(createdSecret.getData(), hasEntry("styx-wf-sa.p12", p12Key.getPrivateKeyData()));
  }

  @Test(expected = RuntimeException.class)
  public void shouldCleanupKeysIfKeyCreationFails() throws IOException {
    when(serviceAccountKeyManager.serviceAccountExists(SERVICE_ACCOUNT)).thenReturn(true);

    ServiceAccountKey jsonKey = new ServiceAccountKey();
    jsonKey.setName("key.json");
    jsonKey.setPrivateKeyData("json-private-key-data");
    ServiceAccountKey p12Key = new ServiceAccountKey();
    p12Key.setName("key.p12");
    p12Key.setPrivateKeyData("p12-private-key-data");
    when(serviceAccountKeyManager.createJsonKey(any(String.class))).thenReturn(jsonKey);
    when(serviceAccountKeyManager.createP12Key(any(String.class))).thenThrow(new IOException());

    sut.ensureServiceAccountKeySecret(WORKFLOW_ID.toString(), SERVICE_ACCOUNT);

    verify(serviceAccountKeyManager).createJsonKey(SERVICE_ACCOUNT);
    verify(serviceAccountKeyManager).createP12Key(SERVICE_ACCOUNT);
    verify(serviceAccountKeyManager).tryDeleteKey(jsonKey.getName());
  }

  @Test
  public void shouldDeleteGCPKeysIfSecretAlreadyExists() throws IOException {
    when(serviceAccountKeyManager.serviceAccountExists(SERVICE_ACCOUNT)).thenReturn(true);

    ServiceAccountKey jsonKey = new ServiceAccountKey();
    jsonKey.setName("key.json");
    jsonKey.setPrivateKeyData("json-private-key-data");
    ServiceAccountKey p12Key = new ServiceAccountKey();
    p12Key.setName("key.p12");
    p12Key.setPrivateKeyData("p12-private-key-data");
    when(serviceAccountKeyManager.createJsonKey(any(String.class))).thenReturn(jsonKey);
    when(serviceAccountKeyManager.createP12Key(any(String.class))).thenReturn(p12Key);
    when(k8sClient.createSecret(any())).thenThrow(new KubernetesClientException(
        "Already exists", 409, new Status()));

    sut.ensureServiceAccountKeySecret(WORKFLOW_ID.toString(), SERVICE_ACCOUNT);

    verify(serviceAccountKeyManager).createJsonKey(SERVICE_ACCOUNT);
    verify(serviceAccountKeyManager).createP12Key(SERVICE_ACCOUNT);
    verify(serviceAccountKeyManager).tryDeleteKey(jsonKey.getName());
    verify(serviceAccountKeyManager).tryDeleteKey(p12Key.getName());
  }

  @Test
  public void shouldNotConcurrentlyCreateServiceAccountKeysAndSecrets()
      throws IOException, ExecutionException, InterruptedException {

    final ServiceAccountKey jsonKey = new ServiceAccountKey();
    jsonKey.setName("key.json");
    jsonKey.setPrivateKeyData("json-private-key-data");
    final ServiceAccountKey p12Key = new ServiceAccountKey();
    p12Key.setName("key.p12");
    p12Key.setPrivateKeyData("p12-private-key-data");

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
    verify(k8sClient, times(1)).createSecret(secretCaptor.capture());
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

    when(secretList.getItems()).thenReturn(List.of(secret));

    // Verify that an unused service account key secret is deleted
    sut.cleanup();
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "json-key"));
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "p12-key"));
    verify(k8sClient).deleteSecret(secret.getMetadata().getName());
  }

  @Test
  @Parameters({"Failed", "Succeeded"})
  public void shouldRemoveServiceAccountSecretsAndKeysUsedByTerminatedPods(String phase) throws Exception {
    final Secret secret = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, SECRET_EPOCH, "json-key", "p12-key", EXPIRED_CREATION_TIMESTAMP.toString());

    when(secretList.getItems()).thenReturn(List.of(secret));

    final KubernetesSecretSpec secretSpec = KubernetesSecretSpec.builder()
        .serviceAccountSecret(secret.getMetadata().getName())
        .build();
    final Pod pod = createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA, secretSpec);

    final PodStatus podStatus = podStatus(phase);
    pod.setStatus(podStatus);

    sut.cleanup();
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "json-key"));
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "p12-key"));
    verify(k8sClient).deleteSecret(secret.getMetadata().getName());
  }

  @Test
  public void shouldHandleErrorsWhenDeletingServiceAccountKeysAndSecret() throws Exception {
    final Secret secret1 = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT_1, SECRET_EPOCH, "json-key-1", "p12-key-1", EXPIRED_CREATION_TIMESTAMP.toString());
    final Secret secret2 = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT_2, SECRET_EPOCH, "json-key-2", "p12-key-2", EXPIRED_CREATION_TIMESTAMP.toString());
    final Secret secret3 = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT_3, SECRET_EPOCH, "json-key-3", "p12-key-3", EXPIRED_CREATION_TIMESTAMP.toString());

    when(secretList.getItems()).thenReturn(List.of(secret1, secret2, secret3));

    doThrow(new KubernetesClientException("fail delete secret1"))
        .when(k8sClient).deleteSecret(secret1.getMetadata().getName());
    doThrow(new IOException("fail delete json-key-2"))
        .when(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT_2,"json-key-2"));
    doThrow(new IOException("fail delete p12-key-3"))
        .when(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT_3,"p12-key-3"));

    sut.cleanup();

    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT_1, "json-key-1"));
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT_1, "p12-key-1"));
    verify(k8sClient).deleteSecret(secret1.getMetadata().getName());

    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT_2, "json-key-2"));
    verify(serviceAccountKeyManager, never()).deleteKey(keyName(SERVICE_ACCOUNT_2, "p12-key-2"));
    verify(k8sClient, never()).deleteSecret(secret2.getMetadata().getName());

    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT_3, "json-key-3"));
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT_3, "p12-key-3"));
    verify(k8sClient, never()).deleteSecret(secret3.getMetadata().getName());
  }

  @Test
  public void shouldNotRemoveServiceAccountSecretsAndKeysInUse() throws Exception {
    final Secret secret = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, SECRET_EPOCH, "json-key", "p12-key", EXPIRED_CREATION_TIMESTAMP.toString());

    when(secretList.getItems()).thenReturn(List.of(secret));

    final KubernetesSecretSpec secretSpec = KubernetesSecretSpec.builder()
        .serviceAccountSecret(secret.getMetadata().getName())
        .build();
    final Pod pod = createPod(WORKFLOW_INSTANCE, RUN_SPEC_WITH_SA, secretSpec);
    pod.setStatus(podStatus("Running"));
    when(podList.getItems()).thenReturn(List.of(pod));
    sut.cleanup();
    verify(serviceAccountKeyManager, never()).deleteKey(anyString());
    verify(k8sClient, never()).deleteSecret(any());
  }

  @Test
  public void shouldRemoveServiceAccountSecretsInPastEpoch() throws Exception {
    final Secret secret = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, PAST_SECRET_EPOCH, "old-json-key", "old-p12-key", EXPIRED_CREATION_TIMESTAMP.toString());

    when(secretList.getItems()).thenReturn(List.of(secret));

    sut.cleanup();

    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "old-json-key"));
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, "old-p12-key"));
    verify(k8sClient).deleteSecret(secret.getMetadata().getName());
  }

  @Test
  public void shouldNotRemoveServiceAccountSecretsCreatedWithin48Hours() throws Exception {
    final String creationTimestamp = CLOCK.instant().minus(Duration.ofHours(48)).toString();
    final Secret secret1 = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, PAST_SECRET_EPOCH, "json-key-1", "p12-key-1", creationTimestamp);
    final Secret secret2 = fakeServiceAccountKeySecret(
        SERVICE_ACCOUNT, SECRET_EPOCH, "json-key-2", "p12-key-2", creationTimestamp);

    when(secretList.getItems()).thenReturn(List.of(secret1, secret2));

    sut.cleanup();

    verify(serviceAccountKeyManager, never()).deleteKey(anyString());
    verify(k8sClient, never()).deleteSecret(any());
  }

  @Test
  public void shouldHandlePermissionDenied() throws IOException {
    when(serviceAccountKeyManager.serviceAccountExists(anyString())).thenReturn(true);

    final GoogleJsonResponseException permissionDenied = new GoogleJsonResponseException(
        new HttpResponseException.Builder(403, "Forbidden", new HttpHeaders()),
        new GoogleJsonError().set("status", "PERMISSION_DENIED"));

    doThrow(permissionDenied).when(serviceAccountKeyManager).createJsonKey(any());
    doThrow(permissionDenied).when(serviceAccountKeyManager).createP12Key(any());

    exception.expect(InvalidExecutionException.class);
    exception.expectMessage(String.format(
        "Permission denied when creating keys for service account: %s. Styx needs to be Service Account Key Admin.",
        SERVICE_ACCOUNT));

    sut.ensureServiceAccountKeySecret(WORKFLOW_ID.toString(), SERVICE_ACCOUNT);
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
    exception.expectMessage(String.format(
        "Maximum number of keys on service account reached: %s. Styx requires 4 keys to operate.",
        SERVICE_ACCOUNT));

    sut.ensureServiceAccountKeySecret(WORKFLOW_ID.toString(), SERVICE_ACCOUNT);
  }

  @Test
  public void shouldUseExistingServiceAccountSecret() throws IOException {

    final String jsonKeyId = "json-key";
    final String p12KeyId = "p12-key";

    final Secret secret = fakeServiceAccountKeySecret(SERVICE_ACCOUNT, SECRET_EPOCH, jsonKeyId, p12KeyId,
        EXPIRED_CREATION_TIMESTAMP.toString());

    when(serviceAccountKeyManager.serviceAccountExists(SERVICE_ACCOUNT)).thenReturn(true);
    when(serviceAccountKeyManager.keyExists(keyName(SERVICE_ACCOUNT, jsonKeyId))).thenReturn(true);
    when(serviceAccountKeyManager.keyExists(keyName(SERVICE_ACCOUNT, p12KeyId))).thenReturn(true);

    when(k8sClient.getSecret(secret.getMetadata().getName())).thenReturn(Optional.of(secret));

    final String serviceAccountSecret = sut.ensureServiceAccountKeySecret(
        WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT);

    assertThat(serviceAccountSecret, is(secret.getMetadata().getName()));

    verify(k8sClient, never()).createSecret(any());
  }

  @Test
  public void shouldFailIfServiceAccountDoesNotExist() throws IOException {
    when(serviceAccountKeyManager.serviceAccountExists(SERVICE_ACCOUNT)).thenReturn(false);

    exception.expect(InvalidExecutionException.class);
    exception.expectMessage("Referenced service account " + SERVICE_ACCOUNT + " was not found");

    sut.ensureServiceAccountKeySecret(WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT);
  }

  @Test
  public void shouldCreateNewServiceAccountKeysIfKeysAreDeleted() throws IOException {

    final ObjectMeta metadata = new ObjectMeta();
    metadata.setAnnotations(Map.of("styx-wf-sa", SERVICE_ACCOUNT));

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

    when(k8sClient.getSecret(secret.getMetadata().getName())).thenReturn(Optional.of(secret));

    sut.ensureServiceAccountKeySecret(WORKFLOW_INSTANCE.workflowId().toString(), SERVICE_ACCOUNT);

    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, jsonKeyId));
    verify(serviceAccountKeyManager).deleteKey(keyName(SERVICE_ACCOUNT, p12KeyId));
    verify(serviceAccountKeyManager).createJsonKey(SERVICE_ACCOUNT);
    verify(serviceAccountKeyManager).createP12Key(SERVICE_ACCOUNT);
    verify(k8sClient).deleteSecret(secret.getMetadata().getName());
    verify(k8sClient).createSecret(secretCaptor.capture());

    final Secret createdSecret = secretCaptor.getValue();
    assertThat(createdSecret.getMetadata().getAnnotations(), hasEntry("styx-wf-sa", SERVICE_ACCOUNT));
    assertThat(createdSecret.getData(), hasEntry("styx-wf-sa.json", newJsonKey.getPrivateKeyData()));
    assertThat(createdSecret.getData(), hasEntry("styx-wf-sa.p12", newP12Key.getPrivateKeyData()));
  }

  @Test
  public void shouldSmearRotationWeekly() {
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
    metadata.setAnnotations(Map.of(
        "styx-wf-sa", serviceAccount,
        "styx-wf-sa-json-key-name", jsonKeyName,
        "styx-wf-sa-p12-key-name", p12KeyName));

    return new SecretBuilder()
        .withMetadata(metadata)
        .withData(Map.of(
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

  private static Pod createPod(WorkflowInstance workflowInstance,
                               DockerRunner.RunSpec runSpec,
                               KubernetesSecretSpec secretSpec) {
    return KubernetesDockerRunner
        .createPod(workflowInstance, runSpec, secretSpec, STYX_ENVIRONMENT);
  }
}
