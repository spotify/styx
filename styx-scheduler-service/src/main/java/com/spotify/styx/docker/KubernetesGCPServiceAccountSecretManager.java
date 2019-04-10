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

import com.google.api.services.iam.v1.model.ServiceAccountKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.spotify.styx.ServiceAccountKeyManager;
import com.spotify.styx.util.GcpUtil;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.client.KubernetesClientException;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KubernetesGCPServiceAccountSecretManager {

  private static final Logger LOG = LoggerFactory.getLogger(KubernetesGCPServiceAccountSecretManager.class);

  private static final String STYX_WORKFLOW_SA_ID_ANNOTATION = "styx-wf-sa";
  private static final String STYX_WORKFLOW_SA_EPOCH_ANNOTATION = "styx-wf-sa-epoch";
  private static final String STYX_WORKFLOW_SA_JSON_KEY_NAME_ANNOTATION = "styx-wf-sa-json-key-name";
  private static final String STYX_WORKFLOW_SA_P12_KEY_NAME_ANNOTATION = "styx-wf-sa-p12-key-name";
  private static final String STYX_WORKFLOW_SA_SECRET_NAME = "styx-wf-sa-keys";
  private static final String STYX_WORKFLOW_SA_JSON_KEY = "styx-wf-sa.json";
  private static final String STYX_WORKFLOW_SA_P12_KEY = "styx-wf-sa.p12";

  private static final Duration DEFAULT_SECRET_EPOCH_PERIOD = Duration.ofDays(7);
  private static final EpochProvider DEFAULT_SECRET_EPOCH_PROVIDER =
      KubernetesGCPServiceAccountSecretManager::smearedEpoch;

  private static final Clock DEFAULT_CLOCK = Clock.systemUTC();

  // epoch period + timeout of "running" state
  // todo: use config value instead of hardcoded 24 hour timeout
  private static final Duration SECRET_GC_GRACE_PERIOD = DEFAULT_SECRET_EPOCH_PERIOD.plusHours(24);

  private final Fabric8KubernetesClient client;
  private final ServiceAccountKeyManager keyManager;
  private final EpochProvider epochProvider;
  private final Clock clock;

  private final Cache<String, String> serviceAccountSecretCache = CacheBuilder.newBuilder()
      .expireAfterWrite(30, TimeUnit.SECONDS)
      .build();

  KubernetesGCPServiceAccountSecretManager(
      Fabric8KubernetesClient client,
      ServiceAccountKeyManager keyManager,
      EpochProvider epochProvider,
      Clock clock) {
    this.client = Objects.requireNonNull(client);
    this.keyManager = Objects.requireNonNull(keyManager);
    this.epochProvider = Objects.requireNonNull(epochProvider);
    this.clock = Objects.requireNonNull(clock);
  }

  KubernetesGCPServiceAccountSecretManager(
      Fabric8KubernetesClient client,
      ServiceAccountKeyManager keyManager) {
    this(client, keyManager, DEFAULT_SECRET_EPOCH_PROVIDER, DEFAULT_CLOCK);
  }

  String ensureServiceAccountKeySecret(String workflowId, String serviceAccount) {
    final long epoch = epochProvider.epoch(clock.millis(), serviceAccount);
    final String secretName = buildSecretName(serviceAccount, epoch);

    LOG.info("[AUDIT] Workflow {} refers to secret {} storing keys of {}",
        workflowId, secretName, serviceAccount);

    try {
      return serviceAccountSecretCache.get(serviceAccount, () ->
          getOrCreateSecret(workflowId, serviceAccount, epoch, secretName));
    } catch (ExecutionException | UncheckedExecutionException e) {
      final Throwable cause = e.getCause();
      if (cause instanceof InvalidExecutionException) {
        throw (InvalidExecutionException) cause;
      } else if (GcpUtil.isPermissionDenied(cause)) {
        throw new InvalidExecutionException(String.format(
            "Permission denied when creating keys for service account: %s. Styx needs to be Service Account Key Admin.",
            serviceAccount));
      } else if (GcpUtil.isResourceExhausted(cause)) {
        throw new InvalidExecutionException(String.format(
            "Maximum number of keys on service account reached: %s. Styx requires 4 keys to operate.",
            serviceAccount));
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
      LOG.warn("[AUDIT] Workflow {} refers to non-existent service account {}", workflowId, serviceAccount);
      throw new InvalidExecutionException("Referenced service account " + serviceAccount + " was not found");
    }

    // Check for existing secret
    var existingSecretOpt = client.getSecret(secretName);
    if (existingSecretOpt.isPresent()) {
      var existingSecret = existingSecretOpt.orElseThrow();
      final Map<String, String> annotations = existingSecret.getMetadata().getAnnotations();
      final String jsonKeyName = annotations.get(STYX_WORKFLOW_SA_JSON_KEY_NAME_ANNOTATION);
      final String p12KeyName = annotations.get(STYX_WORKFLOW_SA_P12_KEY_NAME_ANNOTATION);

      if (keyExists(jsonKeyName) && keyExists(p12KeyName)) {
        return secretName;
      }

      LOG.info("[AUDIT] Service account keys have been deleted for {}, recreating", serviceAccount);

      // Delete secret and any lingering key before creating new keys
      keyManager.deleteKey(jsonKeyName);
      keyManager.deleteKey(p12KeyName);
      deleteSecret(existingSecret);
    }

    // Create service account keys and secret
    createKeysAndSecret(workflowId, serviceAccount, epoch, secretName);

    return secretName;
  }

  private void createKeysAndSecret(String workflowId, String serviceAccount, long epoch, String secretName)
      throws IOException {
    final ServiceAccountKey jsonKey;
    final ServiceAccountKey p12Key;
    try {
      jsonKey = keyManager.createJsonKey(serviceAccount);
      try {
        p12Key = keyManager.createP12Key(serviceAccount);
      } catch (IOException e) {
        // Best effort to rollback the creation of the first key to avoid lingering keys
        keyManager.tryDeleteKey(jsonKey.getName());
        throw e;
      }
    } catch (IOException e) {
      LOG.warn("[AUDIT] Failed to create keys for {} used by workflow {}",
          serviceAccount, workflowId, e);
      throw e;
    }

    final Map<String, String> keys = Map.of(
        STYX_WORKFLOW_SA_JSON_KEY, jsonKey.getPrivateKeyData(),
        STYX_WORKFLOW_SA_P12_KEY, p12Key.getPrivateKeyData()
    );

    final Map<String, String> annotations = Map.of(
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

    try {
      client.createSecret(newSecret);
    } catch (KubernetesClientException e) {
      // Best effort delete of the generated keys since another entity already created the secret
      keyManager.tryDeleteKey(jsonKey.getName());
      keyManager.tryDeleteKey(p12Key.getName());
      return;
    }

    LOG.info("[AUDIT] Secret {} created to store keys of {} referred by workflow {}, jsonKey: {}, p12Key: {}",
        secretName, serviceAccount, workflowId, jsonKey.getName(), p12Key.getName());
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
    final PodList pods = client.listPods();
    final Set<String> activeSecrets = pods.getItems().stream()
        .filter(pod -> !isTerminatedPod(pod))
        .flatMap(pod -> pod.getSpec().getVolumes().stream())
        .map(volume -> volume.getSecret().getSecretName())
        .collect(Collectors.toSet());

    // Enumerate service account secrets to delete
    final long nowMillis = clock.millis();
    final Instant creationDeadline = clock.instant().minus(SECRET_GC_GRACE_PERIOD);
    final SecretList secrets = client.listSecrets();
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
      final Map<String, String> annotations = secret.getMetadata().getAnnotations();
      try {
        keyManager.deleteKey(annotations.get(STYX_WORKFLOW_SA_JSON_KEY_NAME_ANNOTATION));
        keyManager.deleteKey(annotations.get(STYX_WORKFLOW_SA_P12_KEY_NAME_ANNOTATION));
        deleteSecret(secret);
      } catch (KubernetesClientException | IOException e) {
        LOG.warn("Failed to cleanup secret or keys for service account {}",
            annotations.get(STYX_WORKFLOW_SA_ID_ANNOTATION), e);
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

  private void deleteSecret(Secret secret) {
    var name = secret.getMetadata().getName();
    LOG.info("[AUDIT] Deleting service account {} secret {}", serviceAccount(secret), name);
    try {
      client.deleteSecret(name);
    } catch (KubernetesClientException e) {
      LOG.warn("[AUDIT] Failed to delete secret {}", name);
      throw e;
    }
  }

  private static String buildSecretName(String serviceAccount, long epoch) {
    return STYX_WORKFLOW_SA_SECRET_NAME + '-' + epoch + '-'
           + Hashing.sha256().hashString(serviceAccount, UTF_8);
  }

  @VisibleForTesting
  static long smearedEpoch(long nowMillis, String serviceAccount) {
    final long offset = Math.abs(serviceAccount.hashCode()) % DEFAULT_SECRET_EPOCH_PERIOD.toMillis();
    return (nowMillis + offset) / DEFAULT_SECRET_EPOCH_PERIOD.toMillis();
  }

  interface EpochProvider {
    long epoch(long nowMillis, String serviceAccount);
  }
}
