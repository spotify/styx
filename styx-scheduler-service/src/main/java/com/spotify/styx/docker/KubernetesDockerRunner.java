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

import static com.spotify.styx.docker.KubernetesPodEventTranslator.hasPullImageError;
import static com.spotify.styx.docker.KubernetesPodEventTranslator.translate;
import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static com.spotify.styx.state.RunState.State.RUNNING;
import static java.net.HttpURLConnection.HTTP_GONE;
import static java.util.stream.Collectors.toSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventVisitor;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.util.Debug;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.TriggerUtil;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.ResourceRequirementsBuilder;
import io.fabric8.kubernetes.api.model.Secret;
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
import io.norberg.automatter.AutoMatter;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * A {@link DockerRunner} implementation that submits container executions to a Kubernetes cluster.
 */
class KubernetesDockerRunner implements DockerRunner {

  static final String STYX_RUN = "styx-run";
  static final String STYX_WORKFLOW_INSTANCE_ANNOTATION = "styx-workflow-instance";
  static final String DOCKER_TERMINATION_LOGGING_ANNOTATION = "styx-docker-termination-logging";
  static final String COMPONENT_ID = "STYX_COMPONENT_ID";
  // TODO: for backward compatibility, delete later
  private static final String ENDPOINT_ID = "STYX_ENDPOINT_ID";
  static final String WORKFLOW_ID = "STYX_WORKFLOW_ID";
  static final String SERVICE_ACCOUNT = "STYX_SERVICE_ACCOUNT";
  static final String DOCKER_ARGS = "STYX_DOCKER_ARGS";
  static final String DOCKER_IMAGE = "STYX_DOCKER_IMAGE";
  static final String COMMIT_SHA = "STYX_COMMIT_SHA";
  static final String PARAMETER = "STYX_PARAMETER";
  static final String EXECUTION_ID = "STYX_EXECUTION_ID";
  static final String TERMINATION_LOG = "STYX_TERMINATION_LOG";
  static final String TRIGGER_ID = "STYX_TRIGGER_ID";
  static final String TRIGGER_TYPE = "STYX_TRIGGER_TYPE";
  private static final int DEFAULT_POLL_PODS_INTERVAL_SECONDS = 60;
  private static final int DEFAULT_POD_DELETION_DELAY_SECONDS = 120;
  private static final Clock DEFAULT_CLOCK = Clock.systemUTC();
  static final String STYX_WORKFLOW_SA_ENV_VARIABLE = "GOOGLE_APPLICATION_CREDENTIALS";
  static final String STYX_WORKFLOW_SA_SECRET_NAME = "styx-wf-sa-keys";
  private static final String STYX_WORKFLOW_SA_JSON_KEY = "styx-wf-sa.json";
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
  private final KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager;
  private final Debug debug;
  private final int pollPodsIntervalSeconds;
  private final int podDeletionDelaySeconds;
  private final Clock clock;

  private Watch watch;

  KubernetesDockerRunner(NamespacedKubernetesClient client, StateManager stateManager, Stats stats,
                         KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager,
                         Debug debug, int pollPodsIntervalSeconds, int podDeletionDelaySeconds,
                         Clock clock) {
    this.stateManager = Objects.requireNonNull(stateManager);
    this.client = Objects.requireNonNull(client);
    this.stats = Objects.requireNonNull(stats);
    this.serviceAccountSecretManager = Objects.requireNonNull(serviceAccountSecretManager);
    this.debug = debug;
    this.pollPodsIntervalSeconds = pollPodsIntervalSeconds;
    this.podDeletionDelaySeconds = podDeletionDelaySeconds;
    this.clock = Objects.requireNonNull(clock);
  }

  KubernetesDockerRunner(NamespacedKubernetesClient client, StateManager stateManager, Stats stats,
                         KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager,
                         Debug debug) {
    this(client, stateManager, stats, serviceAccountSecretManager, debug,
        DEFAULT_POLL_PODS_INTERVAL_SECONDS, DEFAULT_POD_DELETION_DELAY_SECONDS, DEFAULT_CLOCK);
  }

  @Override
  public void start(WorkflowInstance workflowInstance, RunSpec runSpec) throws IOException {
    final KubernetesSecretSpec secretSpec = ensureSecrets(workflowInstance, runSpec);
    try {
      client.pods().create(createPod(workflowInstance, runSpec, secretSpec));
    } catch (KubernetesClientException kce) {
      throw new IOException("Failed to create Kubernetes pod", kce);
    }
  }

  @Override
  public void cleanup() throws IOException {
    serviceAccountSecretManager.cleanup();
  }

  private KubernetesSecretSpec ensureSecrets(WorkflowInstance workflowInstance, RunSpec runSpec) {
    return KubernetesSecretSpec.builder()
        .customSecret(ensureCustomSecret(workflowInstance, runSpec))
        .serviceAccountSecret(runSpec.serviceAccount().map(
            serviceAccount ->
                serviceAccountSecretManager.ensureServiceAccountKeySecret(
                    workflowInstance.workflowId().toString(),
                    serviceAccount)))
        .build();
  }

  private Optional<WorkflowConfiguration.Secret> ensureCustomSecret(
      WorkflowInstance workflowInstance, RunSpec runSpec) {
    return runSpec.secret().map(specSecret -> {
      if (specSecret.name().startsWith(STYX_WORKFLOW_SA_SECRET_NAME)) {
        LOG.warn("[AUDIT] Workflow {} refers to secret {} with managed service account key secret name prefix, "
            + "denying execution", workflowInstance.workflowId(), specSecret.name());
        throw new InvalidExecutionException(
            "Referenced secret '" + specSecret.name() + "' has the managed service account key secret name prefix");
      }

      // if it ever happens, that feels more like a hack than pure luck so let's be paranoid
      if (STYX_WORKFLOW_SA_SECRET_MOUNT_PATH.equals(specSecret.mountPath())) {
        LOG.warn("[AUDIT] Workflow {} tries to mount secret {} to the reserved path",
                  workflowInstance.workflowId(), specSecret.name());
        throw new InvalidExecutionException(
            "Referenced secret '" + specSecret.name() + "' has the mount path "
            + STYX_WORKFLOW_SA_SECRET_MOUNT_PATH + " defined that is reserved");
      }

      final Secret secret = client.secrets().withName(specSecret.name()).get();
      if (secret == null) {
        LOG.warn("[AUDIT] Workflow {} refers to a non-existent secret {}",
                  workflowInstance.workflowId(), specSecret.name());
        throw new InvalidExecutionException(
            "Referenced secret '" + specSecret.name() + "' was not found");
      } else {
        LOG.info("[AUDIT] Workflow {} refers to secret {}",
                 workflowInstance.workflowId(), specSecret.name());
      }

      return specSecret;
    });
  }

  @VisibleForTesting
  static Pod createPod(WorkflowInstance workflowInstance, RunSpec runSpec, KubernetesSecretSpec secretSpec) {
    final String imageWithTag = runSpec.imageName().contains(":")
        ? runSpec.imageName()
        : runSpec.imageName() + ":latest";

    final PodBuilder podBuilder = new PodBuilder()
        .withNewMetadata()
        .withName(runSpec.executionId())
        .addToAnnotations(STYX_WORKFLOW_INSTANCE_ANNOTATION, workflowInstance.toKey())
        .addToAnnotations(DOCKER_TERMINATION_LOGGING_ANNOTATION,
                          String.valueOf(runSpec.terminationLogging()))
        .endMetadata();

    final PodSpecBuilder specBuilder = new PodSpecBuilder()
        .withRestartPolicy("Never");

    final ResourceRequirementsBuilder resourceRequirements = new ResourceRequirementsBuilder();
    runSpec.memRequest().ifPresent(s -> resourceRequirements.addToRequests("memory", new Quantity(s)));
    runSpec.memLimit().ifPresent(s -> resourceRequirements.addToLimits("memory", new Quantity(s)));

    final ContainerBuilder containerBuilder = new ContainerBuilder()
        .withName(STYX_RUN)
        .withImage(imageWithTag)
        .withArgs(runSpec.args())
        .withEnv(buildEnv(workflowInstance, runSpec))
        .withResources(resourceRequirements.build());

    secretSpec.serviceAccountSecret().ifPresent(serviceAccountSecret -> {
      final SecretVolumeSource saVolumeSource = new SecretVolumeSourceBuilder()
          .withSecretName(serviceAccountSecret)
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
      containerBuilder.addToEnv(envVar(STYX_WORKFLOW_SA_ENV_VARIABLE,
                                       saMount.getMountPath() + STYX_WORKFLOW_SA_JSON_KEY));
    });

    secretSpec.customSecret().ifPresent(secret -> {
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
          .withName(secretVolume.getName())
          .withReadOnly(true)
          .build();
      containerBuilder.addToVolumeMounts(secretMount);
    });

    specBuilder.addToContainers(containerBuilder.build());
    podBuilder.withSpec(specBuilder.build());

    return podBuilder.build();
  }

  @VisibleForTesting
  static EnvVar envVar(String name, String value) {
    return new EnvVarBuilder().withName(name).withValue(value).build();
  }

  private static List<EnvVar> buildEnv(WorkflowInstance workflowInstance,
                                       RunSpec runSpec) {
    return Arrays.asList(
        envVar(COMPONENT_ID,    workflowInstance.workflowId().componentId()),
        // TODO: for backward compatibility, delete later
        envVar(ENDPOINT_ID,     workflowInstance.workflowId().id()),
        envVar(WORKFLOW_ID,     workflowInstance.workflowId().id()),
        envVar(PARAMETER,       workflowInstance.parameter()),
        envVar(COMMIT_SHA,      runSpec.commitSha().orElse("")),
        envVar(SERVICE_ACCOUNT, runSpec.serviceAccount().orElse("")),
        envVar(DOCKER_ARGS,     String.join(" ", runSpec.args())),
        envVar(DOCKER_IMAGE,    runSpec.imageName()),
        envVar(EXECUTION_ID,    runSpec.executionId()),
        envVar(TERMINATION_LOG, "/dev/termination-log"),
        envVar(TRIGGER_ID,      runSpec.trigger().map(TriggerUtil::triggerId).orElse(null)),
        envVar(TRIGGER_TYPE,    runSpec.trigger().map(TriggerUtil::triggerType).orElse(null))
    );
  }

  @Override
  public void cleanup(WorkflowInstance workflowInstance, String executionId) {
    // do not cleanup pod along with state machine transition and let polling thread
    // take care of it
  }

  @VisibleForTesting
  void cleanupWithRunState(WorkflowInstance workflowInstance, String executionId) {
    cleanup(workflowInstance, executionId, containerStatuses ->
        getStyxContainer(containerStatuses).ifPresent(containerStatus -> {
          if (hasPullImageError(containerStatus)) {
            deletePod(workflowInstance, executionId);
          } else {
            if (containerStatus.getState().getTerminated() != null) {
              deletePodIfNonDeletePeriodExpired(workflowInstance, executionId, containerStatus);
            }
          }
        }));
  }

  @VisibleForTesting
  void cleanupWithoutRunState(WorkflowInstance workflowInstance, String executionId) {
    cleanup(workflowInstance, executionId, containerStatuses -> 
        getStyxContainer(containerStatuses).ifPresent(containerStatus -> {
          if (containerStatus.getState().getTerminated() != null) {
            deletePodIfNonDeletePeriodExpired(workflowInstance, executionId, containerStatus);
          } else {
            // if not terminated, delete it directly
            deletePod(workflowInstance, executionId);
          }
        }));
  }

  private void deletePodIfNonDeletePeriodExpired(WorkflowInstance workflowInstance,
                                                 String executionId,
                                                 ContainerStatus containerStatus) {
    if (isNonDeletePeriodExpired(containerStatus)) {
      // if terminated and after graceful period, delete the pod
      // otherwise wait until next polling happens
      deletePod(workflowInstance, executionId);
    }
  }

  private void cleanup(WorkflowInstance workflowInstance, String executionId, 
                       Consumer<List<ContainerStatus>> cleaner) {
    Optional.ofNullable(client.pods().withName(executionId).get()).ifPresent(pod -> {
      final List<ContainerStatus> containerStatuses = pod.getStatus().getContainerStatuses();
      if (!containerStatuses.isEmpty()) {
        cleaner.accept(containerStatuses);
      } else {
        // for some cases such as evicted pod, there is no container status, so we delete directly
        deletePod(workflowInstance, executionId);
      }
    });
  }
  
  private static Optional<ContainerStatus> getStyxContainer(List<ContainerStatus> containerStatuses) {
    return containerStatuses.stream()
        .filter(containerStatus -> containerStatus.getName().equals(STYX_RUN))
        .findFirst();
  }
  
  private boolean isNonDeletePeriodExpired(ContainerStatus containerStatus) {
    return Optional.ofNullable(containerStatus.getState().getTerminated().getFinishedAt())
        .map(finishedAt -> Instant.parse(finishedAt)
            .isBefore(clock.instant().minus(
                Duration.ofSeconds(podDeletionDelaySeconds))))
        .orElse(true);
  }
  
  private void deletePod(WorkflowInstance workflowInstance, String executionId) {
    if (!debug.get()) {
      client.pods().withName(executionId).delete();
      LOG.info("Cleaned up {} pod: {}", workflowInstance.toKey(), executionId);
    } else {
      LOG.info("Keeping {} pod: {}", workflowInstance.toKey(), executionId);
    }
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
    // Failing here means restarting the styx scheduler and replaying all events again. This is quite time consuming
    // and distressing when deploying, so try hard.
    final Retryer<Object> retryer = RetryerBuilder.newBuilder()
        .retryIfException()
        .withWaitStrategy(WaitStrategies.exponentialWait(10, TimeUnit.SECONDS))
        .withStopStrategy(StopStrategies.stopAfterAttempt(10))
        .withRetryListener(this::onRestorePollPodsAttempt)
        .build();

    try {
      retryer.call(Executors.callable(this::tryPollPods));
    } catch (ExecutionException | RetryException e) {
      throw new RuntimeException(e);
    }
  }

  private <V> void onRestorePollPodsAttempt(Attempt<V> attempt) {
    if (attempt.hasException()) {
      LOG.warn("restore: failed polling pods, attempt = {}", attempt.getAttemptNumber(), attempt.getExceptionCause());
    }
  }

  public void init() {
    executor.scheduleWithFixedDelay(
        this::pollPods,
        pollPodsIntervalSeconds,
        pollPodsIntervalSeconds,
        TimeUnit.SECONDS);

    watch = client.pods()
        .watch(new PodWatcher());
  }

  private Set<WorkflowInstance> getRunningWorkflowInstances() {
    return stateManager.activeStates()
        .values()
        .stream()
        .filter(runState -> runState.state().equals(RUNNING))
        .map(RunState::workflowInstance)
        .collect(toSet());
  }

  private void examineRunningWFISandAssociatedPods(Set<WorkflowInstance> runningWorkflowInstances,
                                                   PodList podList) {
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
    final Set<WorkflowInstance> runningWorkflowInstances = getRunningWorkflowInstances();
    final PodList list = client.pods().list();
    examineRunningWFISandAssociatedPods(runningWorkflowInstances, list);


    for (Pod pod : list.getItems()) {
      logEvent(Watcher.Action.MODIFIED, pod, list.getMetadata().getResourceVersion(), true);
      final Optional<WorkflowInstance> workflowInstance = readPodWorkflowInstance(pod);
      if (!workflowInstance.isPresent()) {
        continue;
      }
      final Optional<RunState> runState = lookupPodRunState(pod, workflowInstance.get());
      if (runState.isPresent()) {
        emitPodEvents(Watcher.Action.MODIFIED, pod, runState.get());
        cleanupWithRunState(workflowInstance.get(), pod.getMetadata().getName());
      } else {
        cleanupWithoutRunState(workflowInstance.get(), pod.getMetadata().getName());
      }
    }
  }

  private Optional<WorkflowInstance> readPodWorkflowInstance(Pod pod) {
    final Map<String, String> annotations = pod.getMetadata().getAnnotations();
    final String podName = pod.getMetadata().getName();
    if (!annotations.containsKey(KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION)) {
      LOG.warn("[AUDIT] Got pod without workflow instance annotation {}", podName);
      return Optional.empty();
    }

    final WorkflowInstance workflowInstance = WorkflowInstance.parseKey(
        annotations.get(KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION));

    return Optional.of(workflowInstance);
  }

  private Optional<RunState> lookupPodRunState(Pod pod, WorkflowInstance workflowInstance) {
    final String podName = pod.getMetadata().getName();

    final RunState runState = stateManager.get(workflowInstance);
    if (runState == null) {
      LOG.debug("Pod event for unknown or inactive workflow instance {}", workflowInstance);
      return Optional.empty();
    }

    final Optional<String> executionIdOpt = runState.data().executionId();
    if (!executionIdOpt.isPresent()) {
      LOG.debug("Pod event for state with no current executionId: {}", podName);
      return Optional.empty();
    }

    final String executionId = executionIdOpt.get();
    if (!podName.equals(executionId)) {
      LOG.debug("Pod event not matching current exec id, current:{} != pod:{}",
          executionId, podName);
      return Optional.empty();
    }

    return Optional.of(runState);
  }

  private void emitPodEvents(Watcher.Action action, Pod pod, RunState runState) {
    final List<Event> events = translate(runState.workflowInstance(), runState, action, pod, stats);

    for (Event event : events) {
      if (event.accept(new PullImageErrorMatcher())) {
        stats.recordPullImageError();
      }

      try {
        stateManager.receive(event);
      } catch (IsClosedException isClosedException) {
        LOG.warn("Could not receive kubernetes event", isClosedException);
        throw new RuntimeException(isClosedException);
      }
    }
  }

  private void logEvent(Watcher.Action action, Pod pod, String resourceVersion,
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

    @Override
    public void eventReceived(Action action, Pod pod) {
      if (pod == null) {
        return;
      }

      logEvent(action, pod, pod.getMetadata().getResourceVersion(), false);

      readPodWorkflowInstance(pod)
          .flatMap(workflowInstance -> lookupPodRunState(pod, workflowInstance))
          .ifPresent(runState -> emitPodEvents(action, pod, runState));
    }

    private void reconnect() {
      LOG.warn("Re-establishing pod watcher");

      try {
        watch = client.pods()
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
      scheduleReconnect();
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
    public Boolean submit(WorkflowInstance workflowInstance, ExecutionDescription executionDescription,
        String executionId) {
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

  @AutoMatter
  interface KubernetesSecretSpec {
    Optional<WorkflowConfiguration.Secret> customSecret();
    Optional<String> serviceAccountSecret();

    static KubernetesSecretSpecBuilder builder() {
      return new KubernetesSecretSpecBuilder();
    }
  }
}
