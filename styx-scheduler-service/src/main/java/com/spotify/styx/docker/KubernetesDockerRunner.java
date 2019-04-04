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

import static com.spotify.styx.ScheduledExecutionUtil.scheduleWithJitter;
import static com.spotify.styx.docker.KubernetesPodEventTranslator.imageError;
import static com.spotify.styx.docker.KubernetesPodEventTranslator.isTerminated;
import static com.spotify.styx.docker.KubernetesPodEventTranslator.translate;
import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static com.spotify.styx.util.CloserUtil.register;
import static com.spotify.styx.util.GrpcContextUtil.currentContextExecutorService;
import static com.spotify.styx.util.GuardedRunnable.guard;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

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
  private static final int DEFAULT_POD_CLEANUP_INTERVAL_SECONDS = 60;
  private static final int DEFAULT_POD_DELETION_DELAY_SECONDS = 120;
  private static final Duration PROCESS_POD_UPDATE_INTERVAL = Duration.ofSeconds(5);
  private static final int K8S_POD_PROCESSING_THREADS = 32;
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

  private final ScheduledExecutorService scheduledExecutor;

  private final KubernetesClient client;
  private final StateManager stateManager;
  private final Stats stats;
  private final KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager;
  private final Debug debug;
  private final String styxEnvironment;
  private final Duration cleanupPodsInterval;
  private final Duration podDeletionDelay;
  private final Time time;
  private final ExecutorService executor;

  private Watch watch;

  KubernetesDockerRunner(NamespacedKubernetesClient client, StateManager stateManager, Stats stats,
                         KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager,
                         Debug debug, String styxEnvironment,
                         int cleanupPodsIntervalSeconds, int podDeletionDelaySeconds,
                         Time time, ScheduledExecutorService scheduledExecutor) {
    this.stateManager = Objects.requireNonNull(stateManager);
    this.client = Objects.requireNonNull(client);
    this.stats = Objects.requireNonNull(stats);
    this.serviceAccountSecretManager = Objects.requireNonNull(serviceAccountSecretManager);
    this.debug = debug;
    this.styxEnvironment = styxEnvironment;
    this.cleanupPodsInterval = Duration.ofSeconds(cleanupPodsIntervalSeconds);
    this.podDeletionDelay = Duration.ofSeconds(podDeletionDelaySeconds);
    this.time = Objects.requireNonNull(time);
    this.scheduledExecutor =
        register(closer, Objects.requireNonNull(scheduledExecutor), "kubernetes-scheduled-executor");
    this.executor = currentContextExecutorService(
        register(closer, new ForkJoinPool(K8S_POD_PROCESSING_THREADS), "kubernetes-executor"));
  }

  KubernetesDockerRunner(NamespacedKubernetesClient client, StateManager stateManager, Stats stats,
                         KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager,
                         Debug debug, String styxEnvironment) {
    this(client, stateManager, stats, serviceAccountSecretManager, debug, styxEnvironment,
        DEFAULT_POD_CLEANUP_INTERVAL_SECONDS, DEFAULT_POD_DELETION_DELAY_SECONDS, DEFAULT_TIME,
        Executors.newSingleThreadScheduledExecutor(THREAD_FACTORY));
  }

  @Override
  public void start(WorkflowInstance workflowInstance, RunSpec runSpec) throws IOException {
    // First make cheap check for if pod already exists
    var existingPod = client.pods().withName(runSpec.executionId()).get();
    if (existingPod != null) {
      LOG.info("Pod already exists, not creating: {}: {}", workflowInstance, existingPod);
      return;
    }

    // Set up secrets
    final KubernetesSecretSpec secretSpec = ensureSecrets(workflowInstance, runSpec);

    // Create pod. This might fail with 409 Conflict if the pod already exists as despite the existence
    // check above it might have been concurrently created. That is fine.
    try {
      var pod = createPod(workflowInstance, runSpec, secretSpec, styxEnvironment);
      LOG.info("Creating pod: {}: {}", workflowInstance, pod);
      var createdPod = client.pods().create(pod);
      stats.recordSubmission(runSpec.executionId());
      LOG.info("Created pod: {}: {}", workflowInstance, createdPod);
    } catch (KubernetesClientException kce) {
      if (kce.getCode() == 409 && kce.getStatus().getReason().equals("AlreadyExists")) {
        LOG.info("Pod already existed when creating: {}: {}", workflowInstance, runSpec.executionId());
        // Already launched, success!
        return;
      } else {
        throw new IOException("Failed to create Kubernetes pod", kce);
      }
    }
  }

  @Override
  public void poll(RunState runState) {
    var executionId = runState.data().executionId().orElseThrow(IllegalArgumentException::new);
    var pod = client.pods().withName(executionId).get();
    if (pod == null) {
      // No pod found. Emit an error guarded by the state counter we are basing the error conclusion on.
      stateManager.receiveIgnoreClosed(
          Event.runError(runState.workflowInstance(), "No pod associated with this instance"), runState.counter());
      return;
    }
    logEvent(Watcher.Action.MODIFIED, pod, pod.getMetadata().getResourceVersion(), true);
    emitPodEvents(pod, runState);
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
        .collect(toList());
  }

  @Override
  public void cleanup(WorkflowInstance workflowInstance, String executionId) {
    // do not cleanup pod along with state machine transition and let polling thread
    // take care of it
  }

  @VisibleForTesting
  boolean shouldDeletePodWithRunState(WorkflowInstance workflowInstance, Pod pod, RunState runState) {
    // Precondition: The run states were fetched after the pods
    final Optional<ContainerStatus> containerStatus = getMainContainerStatus(pod);
    if (containerStatus.isEmpty()) {
      // Do nothing, let the RunState time out if the pod fails to start. Then shouldDeletePodWithoutRunState will
      // mark it for deletion.
      // Note: It is natural for pods to not have any container statuses for a while after creation.
      return false;
    }
    if (wantsPod(runState)) {
      // Do not delete the pod if the workflow instance still wants it, e.g. it is still RUNNING.
      return false;
    }
    if (isTerminated(containerStatus.get())) {
      return shouldDeletePodIfNonDeletePeriodExpired(workflowInstance, pod);
    } else if (imageError(containerStatus.get()).isPresent()) {
      return shouldDeletePod(workflowInstance, pod, "Pull image error");
    }
    return false;
  }

  private static boolean wantsPod(RunState runState) {
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
  boolean shouldDeletePodWithoutRunState(WorkflowInstance workflowInstance, Pod pod) {
    // Precondition: The run states were fetched after the pods
    if (isTerminated(pod)) {
      return shouldDeletePodIfNonDeletePeriodExpired(workflowInstance, pod);
    } else {
      // Only pass in the pod name and not the potentially stale pod information
      return shouldDeleteNonTerminatedPodWithoutRunState(workflowInstance, pod.getMetadata().getName());
    }
  }

  private boolean shouldDeleteNonTerminatedPodWithoutRunState(WorkflowInstance workflowInstance, String name) {
    // Fetch the pod here to avoid acting on stale information
    final Pod pod = client.pods().withName(name).get();
    if (pod == null) {
      // The pod is gone, nothing left to do here
      return false;
    }
    // if not terminated, delete directly
    if (!isTerminated(pod)) {
      return shouldDeletePod(workflowInstance, pod, "No RunState, not terminated");
    }
    return false;
  }

  private boolean shouldDeletePodIfNonDeletePeriodExpired(WorkflowInstance workflowInstance, Pod pod) {
    // if terminated and after graceful period, delete the pod
    // otherwise wait until next polling happens
    return getMainContainerStatus(pod).map(this::isNonDeletePeriodExpired).orElse(false) &&
           shouldDeletePod(workflowInstance, pod, "Terminated and expired");
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
    final Instant deadline = time.get().minus(podDeletionDelay);
    return finishedAt.isBefore(deadline);
  }

  @VisibleForTesting
  boolean shouldDeletePod(WorkflowInstance workflowInstance, Pod pod, String reason) {
    final String name = pod.getMetadata().getName();
    if (!debug.get()) {
      LOG.info("Deleting {} pod: {}, reason: '{}'", workflowInstance, name, reason);
      return true;
    } else {
      LOG.info("Keeping {} pod: {}, reason: '{}'", workflowInstance, name, reason);
      return false;
    }
  }

  @Override
  public void close() throws IOException {
    if (watch != null) {
      watch.close();
    }
    closer.close();
  }

  public void init() {
    scheduleWithJitter(this::cleanupPods, scheduledExecutor, cleanupPodsInterval);

    final PodWatcher watcher = new PodWatcher();
    scheduleWithJitter(watcher::processPodUpdates, scheduledExecutor, PROCESS_POD_UPDATE_INTERVAL);

    watch = client.pods().watch(watcher);
  }

  private void cleanupPods() {
    try {
      try (Scope ss = tracer.spanBuilder("Styx.KubernetesDockerRunner.cleanupPods")
          .setRecordEvents(true)
          .setSampler(Samplers.alwaysSample())
          .startScopedSpan()) {
        tryCleanupPods();
      }
    } catch (Throwable t) {
      LOG.warn("Error while cleaning pods", t);
    }
  }

  /**
   * Deletes stale workflow instance execution pods.
   */
  @VisibleForTesting
  void tryCleanupPods() {
    client.pods().list().getItems().stream()
        .map(pod -> runAsync(guard(() -> tryCleanupPod(pod)), executor))
        .collect(toList())
        .forEach(CompletableFuture::join);
  }

  private void tryCleanupPod(Pod pod) {
    var workflowInstance = readPodWorkflowInstance(pod);
    if (workflowInstance.isEmpty()) {
      return;
    }
    var runState = stateManager.getActiveState(workflowInstance.orElseThrow());
    var shouldDelete = runState.isPresent() && isPodRunState(pod, runState.orElseThrow())
                       ? shouldDeletePodWithRunState(workflowInstance.orElseThrow(), pod, runState.orElseThrow())
                       : shouldDeletePodWithoutRunState(workflowInstance.orElseThrow(), pod);
    if (shouldDelete) {
      client.pods().delete(pod);
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
            return runAsync(guard(() -> processPodUpdate(podName, instance)), executor);
          })
          .collect(toList())
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
      scheduledExecutor.schedule(this::reconnect, RECONNECT_DELAY_SECONDS, TimeUnit.SECONDS);
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
