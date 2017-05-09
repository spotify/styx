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

import static com.spotify.styx.docker.KubernetesPodEventTranslator.translate;
import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static com.spotify.styx.state.RunState.State.RUNNING;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.services.iam.v1.model.ServiceAccountKey;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.styx.ServiceAccountKeyManager;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventVisitor;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.util.GcpUtil;
import com.spotify.styx.util.TriggerUtil;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.api.model.SecretList;
import io.fabric8.kubernetes.api.model.SecretVolumeSource;
import io.fabric8.kubernetes.api.model.SecretVolumeSourceBuilder;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeBuilder;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.api.model.VolumeMountBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import java.io.IOException;
import java.net.ProtocolException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A {@link DockerRunner} implementation that submits container executions to a Kubernetes cluster.
 */
class KubernetesDockerRunner implements DockerRunner {

  private static final String NAMESPACE = "default";
  static final String STYX_RUN = "styx-run";
  static final String STYX_WORKFLOW_INSTANCE_ANNOTATION = "styx-workflow-instance";
  static final String DOCKER_TERMINATION_LOGGING_ANNOTATION = "styx-docker-termination-logging";
  static final String COMPONENT_ID = "STYX_COMPONENT_ID";
  // TODO: for backward compatibility, delete later
  private static final String ENDPOINT_ID = "STYX_ENDPOINT_ID";
  static final String WORKFLOW_ID = "STYX_WORKFLOW_ID";
  static final String PARAMETER = "STYX_PARAMETER";
  static final String EXECUTION_ID = "STYX_EXECUTION_ID";
  static final String TERMINATION_LOG = "STYX_TERMINATION_LOG";
  static final String TRIGGER_ID = "STYX_TRIGGER_ID";
  static final String TRIGGER_TYPE = "STYX_TRIGGER_TYPE";
  private static final int DEFAULT_POLL_PODS_INTERVAL_SECONDS = 60;
  static final String STYX_WORKFLOW_SA_ENV_VARIABLE = "GOOGLE_APPLICATION_CREDENTIALS";
  private static final String STYX_WORKFLOW_SA_ID_ANNOTATION = "styx-wf-sa";
  private static final String STYX_WORKFLOW_SA_JSON_KEY_NAME_ANNOTATION = "styx-wf-sa-json-key-name";
  private static final String STYX_WORKFLOW_SA_P12_KEY_NAME_ANNOTATION = "styx-wf-sa-p12-key-name";
  static final String STYX_WORKFLOW_SA_SECRET_NAME = "styx-wf-sa-keys";
  private static final String STYX_WORKFLOW_SA_JSON_KEY = "styx-wf-sa.json";
  private static final String STYX_WORKFLOW_SA_P12_KEY = "styx-wf-sa.p12";
  static final String STYX_WORKFLOW_SA_SECRET_MOUNT_PATH =
      "/etc/" + STYX_WORKFLOW_SA_SECRET_NAME + "/";

  private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("k8s-scheduler-thread-%d")
      .build();

  private final ScheduledExecutorService executor =
      Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY);

  private final KubernetesClient client;
  private final StateManager stateManager;
  private final Stats stats;
  private final int pollPodsIntervalSeconds;
  private final ServiceAccountKeyManager serviceAccountKeyManager;

  private Watch watch;

  private final Object secretMutationLock = new Object() {};

  KubernetesDockerRunner(NamespacedKubernetesClient client, StateManager stateManager, Stats stats,
      ServiceAccountKeyManager serviceAccountKeyManager, int pollPodsIntervalSeconds) {
    this.stateManager = Objects.requireNonNull(stateManager);
    this.client = Objects.requireNonNull(client).inNamespace(NAMESPACE);
    this.stats = Objects.requireNonNull(stats);
    this.pollPodsIntervalSeconds = pollPodsIntervalSeconds;
    this.serviceAccountKeyManager = serviceAccountKeyManager;
  }

  KubernetesDockerRunner(NamespacedKubernetesClient client, StateManager stateManager, Stats stats,
      ServiceAccountKeyManager serviceAccountKeyManager) {
    this(client, stateManager, stats, serviceAccountKeyManager, DEFAULT_POLL_PODS_INTERVAL_SECONDS);
  }

  @Override
  public String start(WorkflowInstance workflowInstance, RunSpec runSpec) throws IOException {
    ensureSecrets(workflowInstance, runSpec);
    try {
      final Pod pod = client.pods().create(createPod(workflowInstance, runSpec));
      return pod.getMetadata().getName();
    } catch (KubernetesClientException kce) {
      throw new IOException("Failed to create Kubernetes pod", kce);
    }
  }

  @Override
  public void cleanup(Set<Workflow> workflows) throws IOException {

    final Set<String> activeServiceAccountEmails = workflows.stream()
        .map(wf -> wf.configuration().serviceAccount())
        .filter(Optional::isPresent).map(Optional::get)
        .collect(toSet());

    synchronized (secretMutationLock) {
      final SecretList secrets = client.secrets().list();
      final List<Secret> unusedServiceAccountKeySecrets = secrets.getItems().stream()
          .filter(secret -> secret.getMetadata().getName().startsWith(STYX_WORKFLOW_SA_SECRET_NAME))
          .filter(secret -> !activeServiceAccountEmails.contains(serviceAccountEmail(secret)))
          .collect(Collectors.toList());

      for (Secret secret : unusedServiceAccountKeySecrets) {
        final String name = secret.getMetadata().getName();
        final String serviceAcountEmail = serviceAccountEmail(secret);

        try {
          final Map<String, String> annotations = secret.getMetadata().getAnnotations();
          final String jsonKeyName = annotations.get(STYX_WORKFLOW_SA_JSON_KEY_NAME_ANNOTATION);
          final String p12KeyName = annotations.get(STYX_WORKFLOW_SA_P12_KEY_NAME_ANNOTATION);

          LOG.info("[AUDIT] Deleting unused service account key: {}", jsonKeyName);
          tryDeleteServiceAccountKey(jsonKeyName);

          LOG.info("[AUDIT] Deleting unused service account key: {}", p12KeyName);
          tryDeleteServiceAccountKey(p12KeyName);

          LOG.info("[AUDIT] Deleting unused service account {} secret {}", serviceAcountEmail, name);
          client.secrets().delete(secret);
        } catch (IOException | KubernetesClientException e) {
          LOG.warn("[AUDIT] Failed to delete unused service account {} keys and/or secret {}",
              serviceAcountEmail, name);
        }
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
        LOG.warn("[AUDIT] Permission denied when trying to delete unused service account key {}");
      } else {
        throw e;
      }
    }
  }

  private String serviceAccountEmail(Secret secret) {
    return secret.getMetadata().getAnnotations().get(STYX_WORKFLOW_SA_ID_ANNOTATION);
  }

  private void ensureSecrets(WorkflowInstance workflowInstance, RunSpec runSpec) throws IOException {
    ensureServiceAccountKeySecret(workflowInstance, runSpec);
    ensureCustomSecret(workflowInstance, runSpec);
  }

  private void ensureCustomSecret(WorkflowInstance workflowInstance, RunSpec runSpec) {
    runSpec.secret().ifPresent(specSecret -> {
      if (specSecret.name().startsWith(STYX_WORKFLOW_SA_SECRET_NAME)) {
        LOG.warn("[AUDIT] Workflow {} refers to secret {} with managed service account key secret name prefix, "
            + "denying execution", specSecret.name());
        throw new InvalidExecutionException(
            "Referenced secret '" + specSecret.name() + "' has the managed service account key secret name prefix");
      }

      // if it ever happens, that feels more like a hack than pure luck so let's be paranoid
      if (STYX_WORKFLOW_SA_SECRET_MOUNT_PATH.equals(specSecret.mountPath())) {
        LOG.error("[AUDIT] Workflow {} tries to mount secret {} to the reserved path",
                  workflowInstance.workflowId(), specSecret.name());
        throw new InvalidExecutionException(
            "Referenced secret '" + specSecret.name() + "' has the mount path "
            + STYX_WORKFLOW_SA_SECRET_MOUNT_PATH + " defined that is reserved");
      }

      final Secret secret = client.secrets().withName(specSecret.name()).get();
      if (secret == null) {
        LOG.error("[AUDIT] Workflow {} refers to a non-existent secret {}",
                  workflowInstance.workflowId(), specSecret.name());
        throw new InvalidExecutionException(
            "Referenced secret '" + specSecret.name() + "' was not found");
      } else {
        LOG.info("[AUDIT] Workflow {} refers to secret {}",
                 workflowInstance.workflowId(), specSecret.name());
      }
    });
  }

  private void ensureServiceAccountKeySecret(WorkflowInstance workflowInstance, RunSpec runSpec) throws IOException {
    if (!runSpec.serviceAccount().isPresent()) {
      return;
    }

    final String serviceAccount = runSpec.serviceAccount().get();

    // Check that the service account exists
    final boolean serviceAccountExists = serviceAccountKeyManager.serviceAccountExists(serviceAccount);
    if (!serviceAccountExists) {
      LOG.error("[AUDIT] Workflow {} refers to non-existent service account {}", workflowInstance.workflowId(),
          serviceAccount);
      throw new InvalidExecutionException("Referenced service account " + serviceAccount + " was not found");
    }

    final String secretName = buildSecretName(serviceAccount);

    LOG.info("[AUDIT] Workflow {} refers to secret {} storing keys of {}",
             workflowInstance.workflowId(), secretName, serviceAccount);

    // TODO: shard locking to regain concurrency
    synchronized (secretMutationLock) {

      // Check if we have a valid service account key secret already
      final Secret existingSecret = client.secrets().withName(secretName).get();
      if (existingSecret != null) {
        if (serviceAccountKeysExist(existingSecret)) {
          return;
        }

        LOG.info("[AUDIT] Service account keys have been deleted for {}, recreating",
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
        LOG.error("[AUDIT] Failed to create keys for {}", serviceAccount, e);
        throw e;
      }

      final Map<String, String> keys = ImmutableMap.of(
          STYX_WORKFLOW_SA_JSON_KEY, jsonKey.getPrivateKeyData(),
          STYX_WORKFLOW_SA_P12_KEY, p12Key.getPrivateKeyData()
      );

      final Map<String, String> annotations = ImmutableMap.of(
          STYX_WORKFLOW_SA_JSON_KEY_NAME_ANNOTATION, jsonKey.getName(),
          STYX_WORKFLOW_SA_P12_KEY_NAME_ANNOTATION, p12Key.getName(),
          STYX_WORKFLOW_SA_ID_ANNOTATION, serviceAccount
      );

      final Secret newSecret = new SecretBuilder()
          .withNewMetadata()
          .withName(secretName)
          .withAnnotations(annotations)
          .endMetadata()
          .withData(keys)
          .build();

      client.secrets().create(newSecret);

      LOG.info("[AUDIT] Secret {} created to store keys of {} referred by workflow {}",
          secretName, serviceAccount, workflowInstance.workflowId());
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

  private static String buildSecretName(String serviceAccount) {
    return STYX_WORKFLOW_SA_SECRET_NAME + '-'
           + Hashing.sha256().hashString(serviceAccount, UTF_8);
  }

  @VisibleForTesting
  static Pod createPod(WorkflowInstance workflowInstance, RunSpec runSpec) {
    final String imageWithTag = runSpec.imageName().contains(":")
        ? runSpec.imageName()
        : runSpec.imageName() + ":latest";

    final String podName = STYX_RUN + "-" + UUID.randomUUID().toString();

    final PodBuilder podBuilder = new PodBuilder()
        .withNewMetadata()
        .withName(podName)
        .addToAnnotations(STYX_WORKFLOW_INSTANCE_ANNOTATION, workflowInstance.toKey())
        .addToAnnotations(DOCKER_TERMINATION_LOGGING_ANNOTATION,
                          String.valueOf(runSpec.terminationLogging()))
        .endMetadata();

    final PodSpecBuilder specBuilder = new PodSpecBuilder()
        .withRestartPolicy("Never");

    final ContainerBuilder containerBuilder = new ContainerBuilder()
        .withName(STYX_RUN)
        .withImage(imageWithTag)
        .withArgs(runSpec.args())
        .withEnv(buildEnv(workflowInstance, runSpec, podName));

    if (runSpec.serviceAccount().isPresent()) {
      final SecretVolumeSource saVolumeSource = new SecretVolumeSourceBuilder()
          .withSecretName(buildSecretName(runSpec.serviceAccount().get()))
          .build();
      final Volume saVolume = new VolumeBuilder()
          .withName(STYX_WORKFLOW_SA_SECRET_NAME)
          .withSecret(saVolumeSource)
          .build();
      specBuilder.addToVolumes(saVolume);

      final VolumeMount saMount = new VolumeMountBuilder()
          .withMountPath(STYX_WORKFLOW_SA_SECRET_MOUNT_PATH)
          .withName(saVolume.getName())
          .withReadOnly(true)
          .build();
      containerBuilder.addToVolumeMounts(saMount);
      // TODO: do we need set this env as default value?
      containerBuilder.addToEnv(envVar(STYX_WORKFLOW_SA_ENV_VARIABLE,
                                       saMount.getMountPath() + STYX_WORKFLOW_SA_JSON_KEY));
    }

    if (runSpec.secret().isPresent()) {
      final WorkflowConfiguration.Secret secret = runSpec.secret().get();
      final SecretVolumeSource secretVolumeSource = new SecretVolumeSourceBuilder()
          .withSecretName(secret.name())
          .build();
      final Volume secretVolume = new VolumeBuilder()
          .withName(secret.name())
          .withSecret(secretVolumeSource)
          .build();
      specBuilder.addToVolumes(secretVolume);

      final VolumeMount secretMount = new VolumeMountBuilder()
          .withMountPath(secret.mountPath())
          .withName(secret.name())
          .withReadOnly(true)
          .build();
      containerBuilder.addToVolumeMounts(secretMount);
    }
    specBuilder.addToContainers(containerBuilder.build());
    podBuilder.withSpec(specBuilder.build());

    return podBuilder.build();
  }

  private static EnvVar envVar(String name, String value) {
    return new EnvVarBuilder().withName(name).withValue(value).build();
  }

  private static List<EnvVar> buildEnv(WorkflowInstance workflowInstance,
                                       RunSpec runSpec, String podName) {
    return Arrays.asList(
        envVar(COMPONENT_ID,    workflowInstance.workflowId().componentId()),
        // TODO: for backward compatibility, delete later
        envVar(ENDPOINT_ID,     workflowInstance.workflowId().id()),
        envVar(WORKFLOW_ID,     workflowInstance.workflowId().id()),
        envVar(PARAMETER,       workflowInstance.parameter()),
        envVar(EXECUTION_ID,    podName),
        envVar(TERMINATION_LOG, "/dev/termination-log"),
        envVar(TRIGGER_ID,      runSpec.trigger().map(TriggerUtil::triggerId).orElse(null)),
        envVar(TRIGGER_TYPE,    runSpec.trigger().map(TriggerUtil::triggerType).orElse(null))
    );
  }

  @Override
  public void cleanup(String executionId) {
    client.pods().withName(executionId).delete();
  }

  @Override
  public void close() throws IOException {
    if (watch != null) {
      watch.close();
    }
    executor.shutdown();
    try {
      executor.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Failed to terminate executor", e);
    }
  }

  @Override
  public void restore() {
    final Retryer<Object> retryer = RetryerBuilder.newBuilder()
        .retryIfException()
        .withWaitStrategy(WaitStrategies.exponentialWait())
        .withStopStrategy(StopStrategies.stopAfterAttempt(3))
        .build();

    try {
      retryer.call(Executors.callable(this::tryPollPods));
    } catch (ExecutionException | RetryException e) {
      throw new RuntimeException(e);
    }
  }

  public void init() {
    executor.scheduleWithFixedDelay(
        this::pollPods,
        pollPodsIntervalSeconds,
        pollPodsIntervalSeconds,
        TimeUnit.SECONDS);

    final String resourceVersion = client.pods().list().getMetadata().getResourceVersion();
    watch = client.pods()
        .withResourceVersion(resourceVersion)
        .watch(new PodWatcher(Integer.parseInt(resourceVersion)));
  }

  void examineRunningWFISandAssociatedPods(PodList podList) {
    final Set<WorkflowInstance> runningWorkflowInstances = stateManager.activeStates()
        .values()
        .stream()
        .filter(runState -> runState.state().equals(RUNNING))
        .map(RunState::workflowInstance)
        .collect(toSet());

    final Set<WorkflowInstance> workflowInstancesForPods = podList.getItems().stream()
        .filter(pod -> pod.getMetadata().getAnnotations()
            .containsKey(STYX_WORKFLOW_INSTANCE_ANNOTATION))
        .map(pod -> WorkflowInstance
            .parseKey(pod.getMetadata().getAnnotations().get(STYX_WORKFLOW_INSTANCE_ANNOTATION)))
        .collect(toSet());

    runningWorkflowInstances.removeAll(workflowInstancesForPods);
    runningWorkflowInstances.forEach(workflowInstance -> stateManager.receiveIgnoreClosed(
        Event.runError(workflowInstance, "No pod associated with this instance")));
  }

  @VisibleForTesting
  void pollPods() {
    try {
      tryPollPods();
    } catch (Throwable t) {
      LOG.warn("Error while polling pods", t);
    }
  }

  private synchronized void tryPollPods() {
    final PodList list = client.pods().list();
    examineRunningWFISandAssociatedPods(list);

    final int resourceVersion = Integer.parseInt(list.getMetadata().getResourceVersion());

    for (Pod pod : list.getItems()) {
      logEvent(Watcher.Action.MODIFIED, pod, resourceVersion, true);
      inspectPod(Watcher.Action.MODIFIED, pod);
    }
  }

  private void inspectPod(Watcher.Action action, Pod pod) {
    final Map<String, String> annotations = pod.getMetadata().getAnnotations();
    final String podName = pod.getMetadata().getName();
    if (!annotations.containsKey(KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION)) {
      LOG.warn("[AUDIT] Got pod without workflow instance annotation {}", podName);
      return;
    }

    final WorkflowInstance workflowInstance = WorkflowInstance.parseKey(
        annotations.get(KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION));

    final RunState runState = stateManager.get(workflowInstance);
    if (runState == null) {
      LOG.warn("Pod event for unknown or inactive workflow instance {}", workflowInstance);
      return;
    }

    final Optional<String> executionIdOpt = runState.data().executionId();
    if (!executionIdOpt.isPresent()) {
      LOG.warn("Pod event for state with no current executionId: {}", podName);
      return;
    }

    final String executionId = executionIdOpt.get();
    if (!podName.equals(executionId)) {
      LOG.warn("Pod event not matching current exec id, current:{} != pod:{}",
          executionId, podName);
      return;
    }

    final List<Event> events = translate(workflowInstance, runState, action, pod, stats);

    for (Event event : events) {
      if (event.accept(new PullImageErrorMatcher())) {
        stats.pullImageError();
      }

      try {
        stateManager.receive(event);
      } catch (StateManager.IsClosed isClosed) {
        LOG.warn("Could not receive kubernetes event", isClosed);
        throw Throwables.propagate(isClosed);
      }
    }
  }

  private void logEvent(Watcher.Action action, Pod pod, int resourceVersion,
                        boolean polled) {
    final String podName = pod.getMetadata().getName();
    final String workflowInstance = pod.getMetadata().getAnnotations()
        .getOrDefault(KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION, "N/A");
    final String status = readStatus(pod);

    LOG.info("{}Pod event for {} at resource version {}, action: {}, workflow instance: {}, status: {}",
             polled ? "Polled: " : "", podName, resourceVersion, action, workflowInstance, status);
  }

  private String readStatus(Pod pod) {
    try {
      return OBJECT_MAPPER.writeValueAsString(pod.getStatus());
    } catch (JsonProcessingException e) {
      return pod.getStatus().toString();
    }
  }

  public class PodWatcher implements Watcher<Pod> {

    private static final int RECONNECT_DELAY_SECONDS = 1;

    private int lastResourceVersion;

    PodWatcher(int resourceVersion) {
      this.lastResourceVersion = resourceVersion;
    }

    @Override
    public void eventReceived(Action action, Pod pod) {
      if (pod == null) {
        return;
      }

      logEvent(action, pod, lastResourceVersion, false);

      try {
        inspectPod(action, pod);
      } finally {
        // fixme: this breaks the kubernetes api convention of not interpreting the resource version
        // https://github.com/kubernetes/kubernetes/blob/release-1.2/docs/devel/api-conventions.md#metadata
        lastResourceVersion = Integer.parseInt(pod.getMetadata().getResourceVersion());
      }
    }

    private void reconnect() {
      LOG.warn("Re-establishing watching from {}", lastResourceVersion);

      try {
        watch = client.pods()
            .withResourceVersion(Integer.toString(lastResourceVersion))
            .watch(this);
      } catch (Throwable e) {
        LOG.warn("Retry threw", e);
        scheduleReconnect();
      }
    }

    private void scheduleReconnect() {
      executor.schedule(this::reconnect, RECONNECT_DELAY_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void onClose(KubernetesClientException e) {
      LOG.warn("Watch closed", e);

      // kube seems to gc old resource versions
      if (e != null && e.getCause() instanceof ProtocolException) {
        // todo: this is racy : more events can be purged while we're playing catch up
        lastResourceVersion++;
        reconnect();
      } else {
        scheduleReconnect();
      }
    }
  }

  // fixme: add a Cause enum to the runError() event instead of this string matching
  private static class PullImageErrorMatcher implements EventVisitor<Boolean> {

    @Override
    public Boolean timeTrigger(WorkflowInstance workflowInstance) {
      return false;
    }

    @Override
    public Boolean triggerExecution(WorkflowInstance workflowInstance, Trigger trigger) {
      return false;
    }

    @Override
    public Boolean info(WorkflowInstance workflowInstance, Message message) {
      return false;
    }

    @Override
    public Boolean dequeue(WorkflowInstance workflowInstance) {
      return false;
    }

    @Override
    public Boolean created(WorkflowInstance workflowInstance, String executionId, String dockerImage) {
      return false;
    }

    @Override
    public Boolean submit(WorkflowInstance workflowInstance, ExecutionDescription executionDescription) {
      return false;
    }

    @Override
    public Boolean submitted(WorkflowInstance workflowInstance, String executionId) {
      return false;
    }

    @Override
    public Boolean started(WorkflowInstance workflowInstance) {
      return false;
    }

    @Override
    public Boolean terminate(WorkflowInstance workflowInstance, Optional<Integer> exitCode) {
      return false;
    }

    @Override
    public Boolean runError(WorkflowInstance workflowInstance, String message) {
      return message.contains("failed to pull");
    }

    @Override
    public Boolean success(WorkflowInstance workflowInstance) {
      return false;
    }

    @Override
    public Boolean retryAfter(WorkflowInstance workflowInstance, long delayMillis) {
      return false;
    }

    @Override
    public Boolean retry(WorkflowInstance workflowInstance) {
      return false;
    }

    @Override
    public Boolean stop(WorkflowInstance workflowInstance) {
      return false;
    }

    @Override
    public Boolean timeout(WorkflowInstance workflowInstance) {
      return false;
    }

    @Override
    public Boolean halt(WorkflowInstance workflowInstance) {
      return false;
    }
  }
}
