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
import static java.util.stream.Collectors.toSet;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
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
import com.spotify.styx.util.TriggerUtil;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.EnvVarBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.api.model.PodFluent;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodSpecFluent;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.VolumeMount;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import java.io.IOException;
import java.net.ProtocolException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A {@link DockerRunner} implementation that submits container executions to a Kubernetes cluster.
 */
class KubernetesDockerRunner implements DockerRunner {

  static final String NAMESPACE = "default";
  static final String STYX_RUN = "styx-run";
  static final String STYX_WORKFLOW_INSTANCE_ANNOTATION = "styx-workflow-instance";
  static final String DOCKER_TERMINATION_LOGGING_ANNOTATION = "styx-docker-termination-logging";
  static final String COMPONENT_ID = "STYX_COMPONENT_ID";
  // TODO: for backward compatibility, delete later
  static final String ENDPOINT_ID = "STYX_ENDPOINT_ID";
  static final String WORKFLOW_ID = "STYX_WORKFLOW_ID";
  static final String PARAMETER = "STYX_PARAMETER";
  static final String EXECUTION_ID = "STYX_EXECUTION_ID";
  static final String TERMINATION_LOG = "STYX_TERMINATION_LOG";
  static final String TRIGGER_ID = "STYX_TRIGGER_ID";
  static final String TRIGGER_TYPE = "STYX_TRIGGER_TYPE";
  static final int POLL_PODS_INTERVAL_SECONDS = 60;

  private static final ScheduledExecutorService EXECUTOR =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactoryBuilder()
              .setDaemon(true)
              .setNameFormat("k8s-scheduler-thread-%d")
              .build());

  private final KubernetesClient client;
  private final StateManager stateManager;
  private final Stats stats;

  private Watch watch;

  KubernetesDockerRunner(KubernetesClient client, StateManager stateManager, Stats stats) {
    this.stateManager = Objects.requireNonNull(stateManager);
    this.client = Objects.requireNonNull(client).inNamespace(NAMESPACE);
    this.stats = Objects.requireNonNull(stats);
  }

  @Override
  public String start(WorkflowInstance workflowInstance, RunSpec runSpec) throws IOException {
    try {
      runSpec.secret().ifPresent(specSecret -> {
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

      final Pod pod = client.pods().create(createPod(workflowInstance, runSpec));
      return pod.getMetadata().getName();
    } catch (KubernetesClientException kce) {
      throw new IOException("Failed to create Kubernetes pod", kce);
    }
  }

  @VisibleForTesting
  static Pod createPod(WorkflowInstance workflowInstance, RunSpec runSpec) {
    final String imageWithTag = runSpec.imageName().contains(":")
        ? runSpec.imageName()
        : runSpec.imageName() + ":latest";

    final String podName = STYX_RUN + "-" + UUID.randomUUID().toString();

    // inject environment variables
    List<EnvVar> env = Lists.newArrayList();
    env.add(new EnvVarBuilder()
        .withName(COMPONENT_ID)
        .withValue(workflowInstance.workflowId().componentId())
        .build());
    // TODO: for backward compatibility, delete later
    env.add(new EnvVarBuilder()
        .withName(ENDPOINT_ID)
        .withValue(workflowInstance.workflowId().id())
        .build());
    env.add(new EnvVarBuilder()
        .withName(WORKFLOW_ID)
        .withValue(workflowInstance.workflowId().id())
        .build());
    env.add(new EnvVarBuilder()
        .withName(PARAMETER)
        .withValue(workflowInstance.parameter())
        .build());
    env.add(new EnvVarBuilder()
        .withName(EXECUTION_ID)
        .withValue(podName)
        .build());
    env.add(new EnvVarBuilder()
        .withName(TERMINATION_LOG)
        .withValue("/dev/termination-log")
        .build());
    env.add(new EnvVarBuilder()
        .withName(TRIGGER_ID)
        .withValue(runSpec.trigger().map(TriggerUtil::triggerId).orElse(null))
        .build());
    env.add(new EnvVarBuilder()
        .withName(TRIGGER_TYPE)
        .withValue(runSpec.trigger().map(TriggerUtil::triggerType).orElse(null))
        .build());

    PodBuilder podBuilder = new PodBuilder()
        .withNewMetadata()
        .withName(podName)
        .addToAnnotations(STYX_WORKFLOW_INSTANCE_ANNOTATION, workflowInstance.toKey())
        .addToAnnotations(DOCKER_TERMINATION_LOGGING_ANNOTATION, String.valueOf(runSpec.terminationLogging()))
        .endMetadata();
    PodFluent.SpecNested<PodBuilder> spec = podBuilder.withNewSpec()
        .withRestartPolicy("Never");
    PodSpecFluent.ContainersNested<PodFluent.SpecNested<PodBuilder>> container = spec
        .addNewContainer()
            .withName(STYX_RUN)
            .withImage(imageWithTag)
            .withArgs(runSpec.args())
            .withEnv(env);

    if (runSpec.secret().isPresent()) {
      final WorkflowConfiguration.Secret secret = runSpec.secret().get();
      spec = spec.addNewVolume()
          .withName(secret.name())
          .withNewSecret()
          .withSecretName(secret.name())
          .endSecret()
          .endVolume();
      container =
          container.addToVolumeMounts(new VolumeMount(secret.mountPath(), secret.name(), true));
    }
    container.endContainer();

    return spec.endSpec().build();
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
  }

  public void init() {
    EXECUTOR.scheduleWithFixedDelay(
        this::pollPods,
        POLL_PODS_INTERVAL_SECONDS,
        POLL_PODS_INTERVAL_SECONDS,
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
      final PodList list = client.pods().list();
      examineRunningWFISandAssociatedPods(list);

      final int resourceVersion = Integer.parseInt(list.getMetadata().getResourceVersion());

      for (Pod pod : list.getItems()) {
        logEvent(Watcher.Action.MODIFIED, pod, resourceVersion, true);
        inspectPod(Watcher.Action.MODIFIED, pod);
      }
    } catch (Throwable t) {
      LOG.warn("Error while polling pods", t);
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
      EXECUTOR.schedule(this::reconnect, RECONNECT_DELAY_SECONDS, TimeUnit.SECONDS);
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
