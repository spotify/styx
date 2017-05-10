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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.iam.v1.model.ServiceAccountKey;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.spotify.styx.ServiceAccountKeyManager;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.util.GcpUtil;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KubernetesGCPServiceAccountSecretManager  {

  private static final Logger logger = LoggerFactory.getLogger(KubernetesGCPServiceAccountSecretManager.class);

  private static final String STYX_WORKFLOW_SA_ID_ANNOTATION = "styx-wf-sa";
  private static final String STYX_WORKFLOW_SA_EPOCH_ANNOTATION = "styx-wf-sa-epoch";
  private static final String STYX_WORKFLOW_SA_JSON_KEY_NAME_ANNOTATION = "styx-wf-sa-json-key-name";
  private static final String STYX_WORKFLOW_SA_P12_KEY_NAME_ANNOTATION = "styx-wf-sa-p12-key-name";
  private static final String STYX_WORKFLOW_SA_SECRET_NAME = "styx-wf-sa-keys";
  private static final String STYX_WORKFLOW_SA_JSON_KEY = "styx-wf-sa.json";
  private static final String STYX_WORKFLOW_SA_P12_KEY = "styx-wf-sa.p12";

  private static final Supplier<String> DEFAULT_SECRET_EPOCH_PROVIDER =
      KubernetesGCPServiceAccountSecretManager::utcNoonSecretEpoch;

  private static final Clock DEFAULT_CLOCK = Clock.systemUTC();

  private static final Duration SECRET_GC_GRACE_PERIOD = Duration.ofMinutes(30);

  private final KubernetesClient client;
  private final ServiceAccountKeyManager serviceAccountKeyManager;
  private final Supplier<String> secretEpochProvider;
  private final Clock clock;

  private final Object secretMutationLock = new Object() {};

  KubernetesGCPServiceAccountSecretManager(
      NamespacedKubernetesClient client,
      ServiceAccountKeyManager serviceAccountKeyManager,
      Supplier<String> secretEpochProvider, Clock clock) {
    this.client = Objects.requireNonNull(client).inNamespace("default");
    this.serviceAccountKeyManager = Objects.requireNonNull(serviceAccountKeyManager);
    this.secretEpochProvider = Objects.requireNonNull(secretEpochProvider);
    this.clock = Objects.requireNonNull(clock);
  }

  public KubernetesGCPServiceAccountSecretManager(NamespacedKubernetesClient client,
      ServiceAccountKeyManager serviceAccountKeyManager) {
    this(client, serviceAccountKeyManager, DEFAULT_SECRET_EPOCH_PROVIDER, DEFAULT_CLOCK);
  }

  public String ensureServiceAccountKeySecret(String workflowId,
      String serviceAccount) throws IOException {

    final String secretEpoch = secretEpochProvider.get();

    // Check that the service account exists
    final boolean serviceAccountExists = serviceAccountKeyManager.serviceAccountExists(serviceAccount);
    if (!serviceAccountExists) {
      logger.error("[AUDIT] Workflow {} refers to non-existent service account {}", workflowId, serviceAccount);
      throw new InvalidExecutionException("Referenced service account " + serviceAccount + " was not found");
    }

    final String secretName = buildSecretName(serviceAccount, secretEpoch);

    logger.info("[AUDIT] Workflow {} refers to secret {} storing keys of {}",
             workflowId, secretName, serviceAccount);

    // TODO: shard locking to regain concurrency
    synchronized (secretMutationLock) {

      // Check if we have a valid service account key secret already
      final Secret existingSecret = client.secrets().withName(secretName).get();
      if (existingSecret != null) {
        if (serviceAccountKeysExist(existingSecret)) {
          return secretName;
        }

        logger.info("[AUDIT] Service account keys have been deleted for {}, recreating",
            serviceAccount);

        // Need to delete this secret before creating a new one
        client.secrets().delete(existingSecret);
      }

      // Create service account keys and secret
      final ServiceAccountKey jsonKey;
      final ServiceAccountKey p12Key;
      try {
        jsonKey = serviceAccountKeyManager.createJsonKey(serviceAccount);
        p12Key = serviceAccountKeyManager.createP12Key(serviceAccount);
      } catch (IOException e) {
        logger.error("[AUDIT] Failed to create keys for {}", serviceAccount, e);
        throw e;
      }

      final Map<String, String> keys = ImmutableMap.of(
          STYX_WORKFLOW_SA_JSON_KEY, jsonKey.getPrivateKeyData(),
          STYX_WORKFLOW_SA_P12_KEY, p12Key.getPrivateKeyData()
      );

      final Map<String, String> annotations = ImmutableMap.of(
          STYX_WORKFLOW_SA_JSON_KEY_NAME_ANNOTATION, jsonKey.getName(),
          STYX_WORKFLOW_SA_P12_KEY_NAME_ANNOTATION, p12Key.getName(),
          STYX_WORKFLOW_SA_ID_ANNOTATION, serviceAccount,
          STYX_WORKFLOW_SA_EPOCH_ANNOTATION, secretEpoch
      );

      final Secret newSecret = new SecretBuilder()
          .withNewMetadata()
          .withName(secretName)
          .withAnnotations(annotations)
          .endMetadata()
          .withData(keys)
          .build();

      client.secrets().create(newSecret);

      logger.info("[AUDIT] Secret {} created to store keys of {} referred by workflow {}",
          secretName, serviceAccount, workflowId);

      return secretName;
    }
  }

  public void cleanup() throws IOException {

    // Enumerate all secrets currently used by pods
    final PodList pods = client.pods().list();
    final Set<String> activeSecrets = pods.getItems().stream()
        .flatMap(pod -> pod.getSpec().getVolumes().stream())
        .map(volume -> volume.getSecret().getSecretName())
        .collect(Collectors.toSet());

    // Enumerate service account secrets to delete
    final String epoch = secretEpochProvider.get();
    final Instant creationDeadline = clock.instant().minus(SECRET_GC_GRACE_PERIOD);
    final SecretList secrets = client.secrets().list();
    final List<Secret> inactiveServiceAccountSecrets = secrets.getItems().stream()
        // Only include service account secrets
        .filter(secret -> secret.getMetadata().getName().startsWith(STYX_WORKFLOW_SA_SECRET_NAME))
        // Exclude secrets in the current epoch
        .filter(secret -> !epoch.equals(secret.getMetadata().getAnnotations().get(STYX_WORKFLOW_SA_EPOCH_ANNOTATION)))
        // Exclude recently created secrets to mitigate races with secret creation around epoch switch
        .filter(secret -> Instant.parse(secret.getMetadata().getCreationTimestamp()).isBefore(creationDeadline))
        // Exclude secrets currently in use by pods
        .filter(secret -> !activeSecrets.contains(secret.getMetadata().getName()))
        .collect(Collectors.toList());

    // Delete keys and secrets for all inactive service accounts and let them be recreated by future executions
    for (Secret secret : inactiveServiceAccountSecrets) {
      final String name = secret.getMetadata().getName();
      final String serviceAcount = secret.getMetadata().getAnnotations().get(STYX_WORKFLOW_SA_ID_ANNOTATION);

      try {
        final Map<String, String> annotations = secret.getMetadata().getAnnotations();
        final String jsonKeyName = annotations.get(STYX_WORKFLOW_SA_JSON_KEY_NAME_ANNOTATION);
        final String p12KeyName = annotations.get(STYX_WORKFLOW_SA_P12_KEY_NAME_ANNOTATION);

        logger.info("[AUDIT] Deleting service account key: {}", jsonKeyName);
        tryDeleteServiceAccountKey(jsonKeyName);

        logger.info("[AUDIT] Deleting service account key: {}", p12KeyName);
        tryDeleteServiceAccountKey(p12KeyName);

        logger.info("[AUDIT] Deleting service account {} secret {}", serviceAcount, name);
        client.secrets().delete(secret);
      } catch (IOException | KubernetesClientException e) {
        logger.warn("[AUDIT] Failed to delete service account {} keys and/or secret {}",
            serviceAcount, name);
      }
    }
  }

  /**
   * Try to delete a service account key, giving up with a warning if permission was denied.
   * @param keyName The fully qualified name of the key to delete.
   */
  private void tryDeleteServiceAccountKey(String keyName) throws IOException {
    try {
      serviceAccountKeyManager.deleteKey(keyName);
    } catch (GoogleJsonResponseException e) {
      if (GcpUtil.isPermissionDenied(e)) {
        logger.warn("[AUDIT] Permission denied when trying to delete unused service account key {}");
      } else {
        throw e;
      }
    }
  }

  private boolean serviceAccountKeysExist(Secret secret) {
    final Map<String, String> annotations = secret.getMetadata().getAnnotations();
    final String jsonKeyName = annotations.get(STYX_WORKFLOW_SA_JSON_KEY_NAME_ANNOTATION);
    final String p12KeyName = annotations.get(STYX_WORKFLOW_SA_P12_KEY_NAME_ANNOTATION);

    try {
      return serviceAccountKeyManager.keyExists(jsonKeyName)
          && serviceAccountKeyManager.keyExists(p12KeyName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static String buildSecretName(String serviceAccount, String epoch) {
    return STYX_WORKFLOW_SA_SECRET_NAME + '-' + epoch + '-'
           + Hashing.sha256().hashString(serviceAccount, UTF_8);
  }

  private static String utcNoonSecretEpoch() {
    return Long.toString((System.currentTimeMillis() + TimeUnit.HOURS.toMillis(12)) / TimeUnit.HOURS.toMillis(24));
  }
}
