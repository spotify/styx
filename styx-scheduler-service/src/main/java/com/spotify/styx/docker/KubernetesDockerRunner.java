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
import static com.spotify.styx.docker.KubernetesPodEventTranslator.isTerminated;
import static com.spotify.styx.docker.KubernetesPodEventTranslator.translate;
import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static com.spotify.styx.state.RunState.State.RUNNING;
import static com.spotify.styx.util.CloserUtil.register;
import static com.spotify.styx.util.GrpcContextUtil.currentContextExecutorService;
import static com.spotify.styx.util.GuardedRunnable.guard;
import static java.util.stream.Collectors.toSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventVisitor;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.Debug;
import com.spotify.styx.util.EventUtil;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerUtil;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.ContainerBuilder;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodSpecBuilder;
import io.fabric8.kubernetes.api.model.Quantity;
import io.fabric8.kubernetes.api.model.QuantityBuilder;
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
import io.opencensus.common.Scope;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A {@link DockerRunner} implementation that submits container executions to a Kubernetes cluster.
 */
class KubernetesDockerRunner implements DockerRunner {

  private static final Tracer tracer = Tracing.getTracer();

  static final String STYX_WORKFLOW_INSTANCE_ANNOTATION = "styx-workflow-instance";
  static final String DOCKER_TERMINATION_LOGGING_ANNOTATION = "styx-docker-termination-logging";
  static final String COMPONENT_ID = "STYX_COMPONENT_ID";
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
  static final String ENVIRONMENT = "STYX_ENVIRONMENT";
  static final String LOGGING = "STYX_LOGGING";
  private static final int DEFAULT_POLL_PODS_INTERVAL_SECONDS = 60;
  private static final int DEFAULT_POD_DELETION_DELAY_SECONDS = 120;
  private static final long PROCESS_POD_UPDATE_INTERVAL_SECONDS = 5;
  private static final int K8S_EVENT_PROCESSING_THREADS = 32;
  private static final Time DEFAULT_TIME = Instant::now;
  static final String STYX_WORKFLOW_SA_ENV_VARIABLE = "GOOGLE_APPLICATION_CREDENTIALS";
  static final String STYX_WORKFLOW_SA_SECRET_NAME = "styx-wf-sa-keys";
  private static final String STYX_WORKFLOW_SA_JSON_KEY = "styx-wf-sa.json";
  static final String STYX_WORKFLOW_SA_SECRET_MOUNT_PATH =
      "/etc/" + STYX_WORKFLOW_SA_SECRET_NAME + "/";

  private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder()
      .setDaemon(true)
      .setNameFormat("k8s-scheduler-thread-%d")
      .build();
  static final String KEEPALIVE_CONTAINER_NAME = "keepalive";
  static final String MAIN_CONTAINER_NAME = "styx-run";

  private final Closer closer = Closer.create();

  private final ScheduledExecutorService executor;

  private final KubernetesClient client;
  private final StateManager stateManager;
  private final Storage storage;
  private final Stats stats;
  private final KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager;
  private final Debug debug;
  private final String styxEnvironment;
  private final int pollPodsIntervalSeconds;
  private final int podDeletionDelaySeconds;
  private final Time time;
  private final ExecutorService eventExecutor;

  private Watch watch;

  KubernetesDockerRunner(NamespacedKubernetesClient client, StateManager stateManager, Stats stats,
                         KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager,
                         Debug debug, String styxEnvironment,
                         int pollPodsIntervalSeconds, int podDeletionDelaySeconds,
                         Time time, ScheduledExecutorService executor, Storage storage) {
    this.stateManager = Objects.requireNonNull(stateManager);
    this.client = Objects.requireNonNull(client);
    this.stats = Objects.requireNonNull(stats);
    this.serviceAccountSecretManager = Objects.requireNonNull(serviceAccountSecretManager);
    this.debug = debug;
    this.styxEnvironment = styxEnvironment;
    this.pollPodsIntervalSeconds = pollPodsIntervalSeconds;
    this.podDeletionDelaySeconds = podDeletionDelaySeconds;
    this.time = Objects.requireNonNull(time);
    this.executor = register(closer, Objects.requireNonNull(executor), "kubernetes-poll");
    this.storage = Objects.requireNonNull(storage);
    this.eventExecutor = currentContextExecutorService(
        register(closer, new ForkJoinPool(K8S_EVENT_PROCESSING_THREADS), "kubernetes-event"));
  }

  KubernetesDockerRunner(NamespacedKubernetesClient client, StateManager stateManager, Stats stats,
                         KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager,
                         Debug debug, String styxEnvironment, Storage storage) {
    this(client, stateManager, stats, serviceAccountSecretManager, debug, styxEnvironment,
        DEFAULT_POLL_PODS_INTERVAL_SECONDS, DEFAULT_POD_DELETION_DELAY_SECONDS, DEFAULT_TIME,
        Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY), storage);
  }

  @Override
  public void start(WorkflowInstance workflowInstance, RunSpec runSpec) throws IOException {
    final KubernetesSecretSpec secretSpec = ensureSecrets(workflowInstance, runSpec);
    stats.recordSubmission(runSpec.executionId());
    try {
      client.pods().create(createPod(workflowInstance, runSpec, secretSpec, styxEnvironment));
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
  static Pod createPod(WorkflowInstance workflowInstance,
                       RunSpec runSpec,
                       KubernetesSecretSpec secretSpec,
                       String styxEnvironment) {
    final String imageWithTag = runSpec.imageName().contains(":")
        ? runSpec.imageName()
        : runSpec.imageName() + ":latest";

    final String executionId = runSpec.executionId();
    final PodBuilder podBuilder = new PodBuilder()
        .withNewMetadata()
        .withName(executionId)
        .addToAnnotations(STYX_WORKFLOW_INSTANCE_ANNOTATION, workflowInstance.toKey())
        .addToAnnotations(DOCKER_TERMINATION_LOGGING_ANNOTATION,
                          String.valueOf(runSpec.terminationLogging()))
        .endMetadata();

    final PodSpecBuilder specBuilder = new PodSpecBuilder()
        .withRestartPolicy("Never");

    final ResourceRequirementsBuilder resourceRequirements = new ResourceRequirementsBuilder();
    runSpec.memRequest().ifPresent(s -> resourceRequirements.addToRequests("memory", new Quantity(s)));
    runSpec.memLimit().ifPresent(s -> resourceRequirements.addToLimits("memory", new Quantity(s)));

    final ContainerBuilder mainContainerBuilder = new ContainerBuilder()
        .withName(MAIN_CONTAINER_NAME)
        .withImage(imageWithTag)
        .withArgs(runSpec.args())
        .withEnv(buildEnv(workflowInstance, runSpec, styxEnvironment))
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
      mainContainerBuilder.addToVolumeMounts(saMount);
      mainContainerBuilder.addToEnv(envVar(STYX_WORKFLOW_SA_ENV_VARIABLE,
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
      mainContainerBuilder.addToVolumeMounts(secretMount);
    });

    specBuilder.addToContainers(mainContainerBuilder.build());
    specBuilder.addToContainers(keepaliveContainer());
    podBuilder.withSpec(specBuilder.build());

    return podBuilder.build();
  }

  private static Container keepaliveContainer() {
    return new ContainerBuilder()
        .withName(KEEPALIVE_CONTAINER_NAME)
        // Use the k8s pause container image. It sleeps forever until terminated.
        .withImage("k8s.gcr.io/pause:3.1")
        .withNewResources()
        .addToRequests("cpu", new QuantityBuilder()
            .withAmount("0")
            .build())
        .addToRequests("memory", new QuantityBuilder()
            .withAmount("0")
            .build())
        .endResources()
        .build();
  }

  @VisibleForTesting
  static EnvVar envVar(String name, String value) {
    return new EnvVarBuilder().withName(name).withValue(value).build();
  }

  private static List<EnvVar> buildEnv(WorkflowInstance workflowInstance,
                                       RunSpec runSpec,
                                       String styxEnvironment) {
    // store user provided env first to prevent accidentally/intentionally overwriting system ones
    final Map<String, String> env = new HashMap<>(runSpec.env());
    env.put(COMPONENT_ID, workflowInstance.workflowId().componentId());
    env.put(WORKFLOW_ID, workflowInstance.workflowId().id());
    env.put(PARAMETER, workflowInstance.parameter());
    env.put(COMMIT_SHA, runSpec.commitSha().orElse(""));
    env.put(SERVICE_ACCOUNT, runSpec.serviceAccount().orElse(""));
    env.put(DOCKER_ARGS, String.join(" ", runSpec.args()));
    env.put(DOCKER_IMAGE, runSpec.imageName());
    env.put(EXECUTION_ID, runSpec.executionId());
    env.put(TERMINATION_LOG, "/dev/termination-log");
    env.put(TRIGGER_ID, runSpec.trigger().map(TriggerUtil::triggerId).orElse(null));
    env.put(TRIGGER_TYPE, runSpec.trigger().map(TriggerUtil::triggerType).orElse(null));
    env.put(ENVIRONMENT, styxEnvironment);
    env.put(LOGGING, "structured");
    return env.entrySet().stream()
        .map(entry -> envVar(entry.getKey(), entry.getValue()))
        .collect(Collectors.toList());
  }

  @Override
  public void cleanup(WorkflowInstance workflowInstance, String executionId) {
    // do not cleanup pod along with state machine transition and let polling thread
    // take care of it
  }

  @VisibleForTesting
  void cleanupWithRunState(WorkflowInstance workflowInstance, Pod pod, RunState runState) {
    // Precondition: The run states were fetched after the pods
    final Optional<ContainerStatus> containerStatus = getMainContainerStatus(pod);
    if (!containerStatus.isPresent()) {
      // Do nothing, let the RunState time out if the pod fails to start. Then cleanupWithoutRunState will delete it.
      // Note: It is natural for pods to not have any container statuses for a while after creation.
      return;
    }
    if (wantsPod(runState)) {
      // Do not delete the pod if the workflow instance still wants it, e.g. it is still RUNNING.
      return;
    }
    if (isTerminated(containerStatus.get())) {
      deletePodIfNonDeletePeriodExpired(workflowInstance, pod);
    } else if (hasPullImageError(containerStatus.get())) {
      deletePod(workflowInstance, pod, "Pull image error");
    }
  }

  private boolean wantsPod(RunState runState) {
    switch (runState.state()) {
      // Be conservative and only let the pod go when we really know it is not needed anymore.
      case TERMINATED:
      case FAILED:
      case ERROR:
      case DONE:
        return false;
      default:
        return true;
    }
  }

  @VisibleForTesting
  void cleanupWithoutRunState(WorkflowInstance workflowInstance, Pod pod) {
    // Precondition: The run states were fetched after the pods
    if (isTerminated(pod)) {
      deletePodIfNonDeletePeriodExpired(workflowInstance, pod);
    } else {
      // Only pass in the pod name and not the potentially stale pod information
      refreshAndDeleteNonTerminatedPodWithoutRunState(workflowInstance, pod.getMetadata().getName());
    }
  }

  private void refreshAndDeleteNonTerminatedPodWithoutRunState(WorkflowInstance workflowInstance, String name) {
    // Fetch the pod here to avoid acting on stale information
    final Pod pod = client.pods().withName(name).get();
    if (pod == null) {
      // The pod is gone, nothing left to do here
      return;
    }
    // if not terminated, delete directly
    if (!isTerminated(pod)) {
      deletePod(workflowInstance, pod, "No RunState, not terminated");
    }
  }

  private void deletePodIfNonDeletePeriodExpired(WorkflowInstance workflowInstance, Pod pod) {
    if (getMainContainerStatus(pod).map(this::isNonDeletePeriodExpired).orElse(false)) {
      // if terminated and after graceful period, delete the pod
      // otherwise wait until next polling happens
      deletePod(workflowInstance, pod, "Terminated and expired");
    }
  }

  static Optional<ContainerStatus> getMainContainerStatus(Pod pod) {
    return readPodWorkflowInstance(pod)
        .flatMap(wfi -> pod.getStatus().getContainerStatuses().stream()
            .filter(status -> isMainContainer(status.getName(), pod))
            .findAny());
  }

  @VisibleForTesting
  static boolean isMainContainer(String name, Pod pod) {
    return name.equals(MAIN_CONTAINER_NAME);
  }

  private boolean isNonDeletePeriodExpired(ContainerStatus cs) {
    final ContainerStateTerminated t = cs.getState().getTerminated();
    if (t.getFinishedAt() == null) {
      return true;
    }
    final Instant finishedAt;
    try {
      finishedAt = Instant.parse(t.getFinishedAt());
    } catch (DateTimeParseException e) {
      LOG.warn("Failed to parse container state terminated finishedAt: '{}'", t.getFinishedAt(), e);
      return true;
    }
    final Instant deadline = time.get().minus(Duration.ofSeconds(podDeletionDelaySeconds));
    return finishedAt.isBefore(deadline);
  }

  @VisibleForTesting
  void deletePod(WorkflowInstance workflowInstance, Pod pod, String reason) {
    final String name = pod.getMetadata().getName();
    if (!debug.get()) {
      client.pods().withName(name).delete();
      LOG.info("Deleted {} pod: {}, reason: '{}'", workflowInstance, name, reason);
    } else {
      LOG.info("Keeping {} pod: {}, reason: '{}'", workflowInstance, name, reason);
    }
  }

  @Override
  public void close() throws IOException {
    if (watch != null) {
      watch.close();
    }
    closer.close();
  }

  @Override
  public void restore() {
    // Failing here means restarting the styx scheduler and replaying all events again. This is
    // quite time consuming and distressing when deploying, so try hard.
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

    final PodWatcher watcher = new PodWatcher();
    executor.scheduleWithFixedDelay(guard(watcher::processPodUpdates),
        PROCESS_POD_UPDATE_INTERVAL_SECONDS, PROCESS_POD_UPDATE_INTERVAL_SECONDS, TimeUnit.SECONDS);

    watch = client.pods().watch(watcher);
  }

  private void examineRunningWFISandAssociatedPods(
      Map<WorkflowInstance, RunState> activeStates,
      PodList podList) {

    final Map<WorkflowInstance, RunState> runningWorkflowInstances = Maps.filterValues(activeStates, runState ->
        runState.state().equals(RUNNING) && runState.data().executionId().isPresent());

    final Set<WorkflowInstance> workflowInstancesForPods = podList.getItems().stream()
        .map(pod -> pod.getMetadata().getAnnotations())
        .filter(Objects::nonNull)
        .map(annotations -> annotations.get(STYX_WORKFLOW_INSTANCE_ANNOTATION))
        .filter(Objects::nonNull)
        .map(WorkflowInstance::parseKey)
        .collect(toSet());

    // Emit errors for workflow instances that seem to be missing its pod
    runningWorkflowInstances.forEach((workflowInstance, runState) -> {

      // Is there a matching pod in the list? Bail.
      if (workflowInstancesForPods.contains(workflowInstance)) {
        return;
      }

      // The pod list might be stale so explicitly look for a pod using the execution ID.
      final String executionId = runState.data().executionId().get();
      final Pod pod = client.pods().withName(executionId).get();

      // We found a pod? Bail.
      if (pod != null) {
        return;
      }

      // No pod found. Emit an error guarded by the state counter we are basing the error conclusion on.
      stateManager.receiveIgnoreClosed(
          Event.runError(workflowInstance, "No pod associated with this instance"), runState.counter());
    });
  }

  private void pollPods() {
    try {
      try (Scope ss = tracer.spanBuilder("Styx.KubernetesDockerRunner.pollPods")
          .setRecordEvents(true)
          .setSampler(Samplers.alwaysSample())
          .startScopedSpan()) {
        tryPollPods();
      }
    } catch (Throwable t) {
      LOG.warn("Error while polling pods", t);
    }
  }

  @VisibleForTesting
  synchronized void tryPollPods() {
    // Fetch pods _before_ fetching all active states
    final PodList list = client.pods().list();
    var activeStates = storage.readActiveStatesPartial();
    var unavailableWorkflowInstance = activeStates._1;
    examineRunningWFISandAssociatedPods(activeStates._2, list);

    for (Pod pod : list.getItems()) {
      logEvent(Watcher.Action.MODIFIED, pod, list.getMetadata().getResourceVersion(), true);
      final Optional<WorkflowInstance> workflowInstance = readPodWorkflowInstance(pod);
      if (!workflowInstance.isPresent()) {
        continue;
      }

      if (unavailableWorkflowInstance.test(workflowInstance.get())) {
        LOG.debug("Ignoring unavailable workflow instance: {}", workflowInstance.get());
        continue;
      }
      final RunState runState = activeStates._2.get(workflowInstance.get());
      if (runState != null && isPodRunState(pod, runState)) {
        emitPodEvents(pod, runState);
        cleanupWithRunState(workflowInstance.get(), pod, runState);
      } else {
        // The pod is stale as we fetched the active states _after_ listing all pods.
        cleanupWithoutRunState(workflowInstance.get(), pod);
      }
    }
  }

  private static Optional<WorkflowInstance> readPodWorkflowInstance(Pod pod) {
    final Map<String, String> annotations = pod.getMetadata().getAnnotations();
    final String podName = pod.getMetadata().getName();
    if (annotations == null || !annotations.containsKey(KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION)) {
      LOG.warn("[AUDIT] Got pod without workflow instance annotation {}", podName);
      return Optional.empty();
    }

    final WorkflowInstance workflowInstance = WorkflowInstance.parseKey(
        annotations.get(KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION));

    return Optional.of(workflowInstance);
  }

  private Optional<RunState> lookupPodRunState(Pod pod, WorkflowInstance workflowInstance) {
    final Optional<RunState> runStateOpt = stateManager.getActiveState(workflowInstance);
    if (!runStateOpt.isPresent()) {
      LOG.debug("Pod event for unknown or inactive workflow instance {}", workflowInstance);
      return Optional.empty();
    }
    return runStateOpt.filter(runState -> isPodRunState(pod, runState));
  }

  private boolean isPodRunState(Pod pod, RunState runState) {
    final String podName = pod.getMetadata().getName();

    final Optional<String> executionIdOpt = runState.data().executionId();
    if (!executionIdOpt.isPresent()) {
      LOG.debug("Pod event for state with no current executionId: {}", podName);
      return false;
    }

    final String executionId = executionIdOpt.get();
    if (!podName.equals(executionId)) {
      LOG.debug("Pod event not matching current exec id, current:{} != pod:{}",
          executionId, podName);
      return false;
    }

    return true;
  }

  private void emitPodEvents(Pod pod, RunState runState) {
    final List<Event> events = translate(runState.workflowInstance(), runState, pod, stats);

    for (int i = 0; i < events.size(); ++i) {
      final Event event = events.get(i);
      if (event.accept(new PullImageErrorMatcher())) {
        stats.recordPullImageError();
      }
      if (EventUtil.name(event).equals("started")) {
        runState.data().executionId().ifPresent(stats::recordRunning);
      }

      try {
        // TODO: spoofing counter values like this can give unexpected results, e.g. if we emit two events here the
        // first one might be discarded and the second one accepted.
        stateManager.receive(event, runState.counter() + i);
      } catch (IsClosedException isClosedException) {
        LOG.warn("Could not receive kubernetes event", isClosedException);
        throw new RuntimeException(isClosedException);
      }
    }
  }

  private void logEvent(Watcher.Action action, Pod pod, String resourceVersion,
                        boolean polled) {
    final String podName = pod.getMetadata().getName();
    final String workflowInstance = Optional.ofNullable(pod.getMetadata().getAnnotations())
        .map(annotations -> annotations.get(STYX_WORKFLOW_INSTANCE_ANNOTATION))
        .orElse("N/A");
    final String status = readStatus(pod);

    LOG.info("{}Pod event for {} ({}) at resource version {}, action: {}, workflow instance: {}, status: {}",
             polled ? "Polled: " : "", podName, pod.getMetadata().getUid(), resourceVersion, action, workflowInstance,
             status);
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

    private final ConcurrentMap<String, WorkflowInstance> podUpdates = new ConcurrentHashMap<>();

    /**
     * @implNote In order to be able to keep up with the stream of events from k8s, this method should
     *           not perform any expensive processing or blocking IO.
     */
    @Override
    public void eventReceived(Action action, Pod pod) {

      if (pod == null) {
        return;
      }

      logEvent(action, pod, pod.getMetadata().getResourceVersion(), false);

      // Ignore pod deletions
      if (action == Action.DELETED) {
        return;
      }

      // Ignore non-styx pods
      final Optional<WorkflowInstance> workflowInstance = readPodWorkflowInstance(pod);
      if (!workflowInstance.isPresent()) {
        return;
      }

      // Flag this pod for later processing. Note that instead of storing the received pod status here,
      // a new status is fetched later, in order to avoid observing pod statuses out-of-order.
      podUpdates.put(pod.getMetadata().getName(), workflowInstance.get());
    }

    void processPodUpdates() {
      // Get a batch of pods to process
      final Set<String> podNames = ImmutableSet.copyOf(podUpdates.keySet());

      LOG.debug("Processing pod updates: {}", podNames.size());

      // Process the batch in parallel
      podNames.stream()
          .map(podName -> {
            // Remove from change set before processing in order to not lose updates
            final WorkflowInstance instance = podUpdates.remove(podName);
            return CompletableFuture.runAsync(() -> processPodUpdate(podName, instance), eventExecutor);
          })
          .collect(Collectors.toList())
          .forEach(CompletableFuture::join);
    }

    private void processPodUpdate(String podName, WorkflowInstance instance) {
      LOG.debug("Processing pod update: {}: {}", podName, instance);

      final Pod pod = client.pods().withName(podName).get();
      if (pod == null) {
        return;
      }

      final Optional<RunState> runState = lookupPodRunState(pod, instance);
      if (!runState.isPresent()) {
        return;
      }

      emitPodEvents(pod, runState.get());
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
    public Boolean triggerExecution(WorkflowInstance workflowInstance, Trigger trigger,
        TriggerParameters parameters) {
      return false;
    }

    @Override
    public Boolean info(WorkflowInstance workflowInstance, Message message) {
      return false;
    }

    @Override
    public Boolean dequeue(WorkflowInstance workflowInstance, Set<String> resourceIds) {
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
