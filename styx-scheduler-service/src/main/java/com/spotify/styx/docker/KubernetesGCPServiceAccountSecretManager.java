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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.spotify.styx.ServiceAccountKeyManager;
import com.spotify.styx.util.GcpUtil;
import io.fabric8.kubernetes.api.model.Pod;
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
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KubernetesGCPServiceAccountSecretManager {

  private static final Logger logger = LoggerFactory.getLogger(KubernetesGCPServiceAccountSecretManager.class);

  private static final String STYX_WORKFLOW_SA_ID_ANNOTATION = "styx-wf-sa";
  private static final String STYX_WORKFLOW_SA_EPOCH_ANNOTATION = "styx-wf-sa-epoch";
  private static final String STYX_WORKFLOW_SA_JSON_KEY_NAME_ANNOTATION = "styx-wf-sa-json-key-name";
  private static final String STYX_WORKFLOW_SA_P12_KEY_NAME_ANNOTATION = "styx-wf-sa-p12-key-name";
  private static final String STYX_WORKFLOW_SA_SECRET_NAME = "styx-wf-sa-keys";
  private static final String STYX_WORKFLOW_SA_JSON_KEY = "styx-wf-sa.json";
  private static final String STYX_WORKFLOW_SA_P12_KEY = "styx-wf-sa.p12";

  private static final EpochProvider DEFAULT_SECRET_EPOCH_PROVIDER =
      KubernetesGCPServiceAccountSecretManager::smearedDailyEpoch;

  private static final Clock DEFAULT_CLOCK = Clock.systemUTC();

  private static final Duration SECRET_GC_GRACE_PERIOD = Duration.ofHours(48);

  private final KubernetesClient client;
  private final ServiceAccountKeyManager keyManager;
  private final EpochProvider epochProvider;
  private final Clock clock;

  private final Cache<String, String> serviceAccountSecretCache = CacheBuilder.newBuilder()
      .expireAfterWrite(30, TimeUnit.SECONDS)
      .build();

  KubernetesGCPServiceAccountSecretManager(
      NamespacedKubernetesClient client,
      ServiceAccountKeyManager keyManager,
      EpochProvider epochProvider,
      Clock clock) {
    this.client = Objects.requireNonNull(client);
    this.keyManager = Objects.requireNonNull(keyManager);
    this.epochProvider = Objects.requireNonNull(epochProvider);
    this.clock = Objects.requireNonNull(clock);
  }

  KubernetesGCPServiceAccountSecretManager(NamespacedKubernetesClient client,
      ServiceAccountKeyManager keyManager) {
    this(client, keyManager, DEFAULT_SECRET_EPOCH_PROVIDER, DEFAULT_CLOCK);
  }

  String ensureServiceAccountKeySecret(String workflowId,
      String serviceAccount) throws IOException {
    final long epoch = epochProvider.epoch(clock.millis(), serviceAccount);
    final String secretName = buildSecretName(serviceAccount, epoch);

    logger.info("[AUDIT] Workflow {} refers to secret {} storing keys of {}",
             workflowId, secretName, serviceAccount);

    try {
      return serviceAccountSecretCache.get(serviceAccount, () ->
          getOrCreateSecret(workflowId, serviceAccount, epoch, secretName));
    } catch (Exception e) {
      if (e.getCause() instanceof InvalidExecutionException) {
        throw (InvalidExecutionException) e.getCause();
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  private String getOrCreateSecret(String workflowId, String serviceAccount, long epoch, String secretName)
      throws IOException {
    // Check that the service account exists
    final boolean serviceAccountExists = keyManager.serviceAccountExists(serviceAccount);
    if (!serviceAccountExists) {
      logger.error("[AUDIT] Workflow {} refers to non-existent service account {}", workflowId, serviceAccount);
      throw new InvalidExecutionException("Referenced service account " + serviceAccount + " was not found");
    }

    // Check for existing secret
    final Secret existingSecret = client.secrets().withName(secretName).get();
    if (existingSecret != null) {
      final Map<String, String> annotations = existingSecret.getMetadata().getAnnotations();
      final String jsonKeyName = annotations.get(STYX_WORKFLOW_SA_JSON_KEY_NAME_ANNOTATION);
      final String p12KeyName = annotations.get(STYX_WORKFLOW_SA_P12_KEY_NAME_ANNOTATION);

      if (keyExists(jsonKeyName) && keyExists(p12KeyName)) {
        return secretName;
      }

      logger.info("[AUDIT] Service account keys have been deleted for {}, recreating", serviceAccount);

      // Delete secret and any lingering key before creating new keys
      keyManager.deleteKey(jsonKeyName);
      keyManager.deleteKey(p12KeyName);
      client.secrets().delete(existingSecret);
    }

    // Create service account keys and secret
    createSecret(workflowId, serviceAccount, epoch, secretName);

    return secretName;
  }

  private void createSecret(String workflowId, String serviceAccount, long epoch, String secretName)
      throws IOException {
    final ServiceAccountKey jsonKey;
    final ServiceAccountKey p12Key;
    try {
      jsonKey = keyManager.createJsonKey(serviceAccount);
      p12Key = keyManager.createP12Key(serviceAccount);
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
        STYX_WORKFLOW_SA_EPOCH_ANNOTATION, Long.toString(epoch)
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
  }

  private boolean keyExists(String jsonKeyName) {
    try {
      return keyManager.keyExists(jsonKeyName);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void cleanup() throws IOException {
    // Enumerate all secrets currently used by non-terminated pods
    final PodList pods = client.pods().list();
    final Set<String> activeSecrets = pods.getItems().stream()
        .filter(pod -> !isTerminatedPod(pod))
        .flatMap(pod -> pod.getSpec().getVolumes().stream())
        .map(volume -> volume.getSecret().getSecretName())
        .collect(Collectors.toSet());

    // Enumerate service account secrets to delete
    final long nowMillis = clock.millis();
    final Instant creationDeadline = clock.instant().minus(SECRET_GC_GRACE_PERIOD);
    final SecretList secrets = client.secrets().list();
    final List<Secret> inactiveServiceAccountSecrets = secrets.getItems().stream()
        // Only include service account secrets
        .filter(secret -> secret.getMetadata().getName().startsWith(STYX_WORKFLOW_SA_SECRET_NAME))
        // Exclude secrets in the current epoch
        .filter(secret -> !Long.toString(epochProvider.epoch(nowMillis, serviceAccount(secret)))
            .equals(secretEpoch(secret)))
        // Exclude recently created secrets to mitigate races with secret creation around epoch switch
        .filter(secret -> Instant.parse(secret.getMetadata().getCreationTimestamp()).isBefore(creationDeadline))
        // Exclude secrets currently in use by pods
        .filter(secret -> !activeSecrets.contains(secret.getMetadata().getName()))
        .collect(Collectors.toList());

    // Delete keys and secrets for all inactive service accounts and let them be recreated by future executions
    for (Secret secret : inactiveServiceAccountSecrets) {
      final String name = secret.getMetadata().getName();
      final String serviceAcount = serviceAccount(secret);

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

  private boolean isTerminatedPod(Pod pod) {
    switch (pod.getStatus().getPhase()) {
      case "Succeeded":
      case "Failed":
        return true;
      default:
        return false;
    }
  }

  private String secretEpoch(Secret secret) {
    return secret.getMetadata().getAnnotations().get(STYX_WORKFLOW_SA_EPOCH_ANNOTATION);
  }

  private String serviceAccount(Secret secret) {
    return secret.getMetadata().getAnnotations().get(STYX_WORKFLOW_SA_ID_ANNOTATION);
  }

  /**
   * Try to delete a service account key, giving up with a warning if permission was denied.
   * @param keyName The fully qualified name of the key to delete.
   */
  private void tryDeleteServiceAccountKey(String keyName) throws IOException {
    try {
      keyManager.deleteKey(keyName);
    } catch (GoogleJsonResponseException e) {
      if (GcpUtil.isPermissionDenied(e)) {
        logger.warn("[AUDIT] Permission denied when trying to delete unused service account key {}");
      } else {
        throw e;
      }
    }
  }

  private static String buildSecretName(String serviceAccount, long epoch) {
    return STYX_WORKFLOW_SA_SECRET_NAME + '-' + epoch + '-'
           + Hashing.sha256().hashString(serviceAccount, UTF_8);
  }

  @VisibleForTesting
  static long smearedDailyEpoch(long nowMillis, String serviceAccount) {
    final long offset = Math.abs(serviceAccount.hashCode()) % TimeUnit.DAYS.toMillis(1);
    return (nowMillis + offset) / TimeUnit.HOURS.toMillis(24);
  }

  interface EpochProvider {
    long epoch(long nowMillis, String serviceAccount);
  }
}
