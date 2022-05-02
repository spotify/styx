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

import static com.spotify.styx.docker.KubernetesDockerRunner.COMMIT_SHA;
import static com.spotify.styx.docker.KubernetesDockerRunner.COMPONENT_ID;
import static com.spotify.styx.docker.KubernetesDockerRunner.DOCKER_ARGS;
import static com.spotify.styx.docker.KubernetesDockerRunner.DOCKER_IMAGE;
import static com.spotify.styx.docker.KubernetesDockerRunner.DOCKER_TERMINATION_LOGGING_ANNOTATION;
import static com.spotify.styx.docker.KubernetesDockerRunner.ENVIRONMENT;
import static com.spotify.styx.docker.KubernetesDockerRunner.EXECUTION_ID;
import static com.spotify.styx.docker.KubernetesDockerRunner.LOGGING;
import static com.spotify.styx.docker.KubernetesDockerRunner.MAIN_CONTAINER_NAME;
import static com.spotify.styx.docker.KubernetesDockerRunner.PARAMETER;
import static com.spotify.styx.docker.KubernetesDockerRunner.SERVICE_ACCOUNT;
import static com.spotify.styx.docker.KubernetesDockerRunner.STYX_EXECUTION_TRIES;
import static com.spotify.styx.docker.KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION;
import static com.spotify.styx.docker.KubernetesDockerRunner.TERMINATION_LOG;
import static com.spotify.styx.docker.KubernetesDockerRunner.TRIGGER_ID;
import static com.spotify.styx.docker.KubernetesDockerRunner.TRIGGER_TYPE;
import static com.spotify.styx.docker.KubernetesDockerRunner.WORKFLOW_ID;
import static com.spotify.styx.docker.KubernetesDockerRunner.envVar;
import static com.spotify.styx.docker.LabelValue.normalize;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import com.spotify.styx.docker.KubernetesDockerRunner.KubernetesSecretSpec;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.TriggerUtil;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import org.junit.Test;

public class KubernetesDockerRunnerPodResourceTest {

  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-04-04");

  private final static KubernetesSecretSpec EMPTY_SECRET_SPEC = KubernetesSecretSpec.builder().build();

  private static final String TEST_EXECUTION_ID = "execution_1";

  private static final String STYX_ENVIRONMENT = "testing";

  @Test
  public void shouldAddLatestTag() {
    Pod pod = createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC,
        Collections.emptyMap(), StateData.zero());

    List<Container> containers = pod.getSpec().getContainers();
    assertThat(containers.size(), is(2));
    assertThat(containers.get(0).getName(), is(MAIN_CONTAINER_NAME));

    Container container = containers.get(0);
    assertThat(container.getImage(), is("busybox:latest"));
  }

  @Test
  public void shouldUseConfiguredTag() {
    Pod pod = createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.simple("eid", "busybox:v7"), EMPTY_SECRET_SPEC,
        Collections.emptyMap(), StateData.zero());

    List<Container> containers = pod.getSpec().getContainers();
    assertThat(containers.size(), is(2));
    assertThat(containers.get(0).getName(), is(MAIN_CONTAINER_NAME));

    Container container = containers.get(0);
    assertThat(container.getImage(), is("busybox:v7"));
  }

  @Test
  public void shouldAddArgs() {
    Pod pod = createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.simple("eid", "busybox", "echo", "foo", "bar"), EMPTY_SECRET_SPEC,
        Collections.emptyMap(), StateData.zero());

    List<Container> containers = pod.getSpec().getContainers();
    assertThat(containers.size(), is(2));
    assertThat(containers.get(0).getName(), is(MAIN_CONTAINER_NAME));

    Container container = containers.get(0);
    assertThat(container.getArgs(), contains("echo", "foo", "bar"));
  }

  @Test
  public void shouldAddWorkflowInstanceAnnotation() {
    Pod pod = createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC,
        Collections.emptyMap(), StateData.zero());

    Map<String, String> annotations = pod.getMetadata().getAnnotations();
    assertThat(annotations, hasEntry(STYX_WORKFLOW_INSTANCE_ANNOTATION, WORKFLOW_INSTANCE.toKey()));

    WorkflowInstance workflowInstance =
        WorkflowInstance.parseKey(annotations.get(STYX_WORKFLOW_INSTANCE_ANNOTATION));
    assertThat(workflowInstance, is(WORKFLOW_INSTANCE));
  }

  @Test
  public void shouldAddLabels() {
    DockerRunner.RunSpec spec =
        DockerRunner.RunSpec.builder().executionId("eid").imageName("busybox").trigger(Trigger.natural()).build();
    var pod = createPod(
        WORKFLOW_INSTANCE,
        spec,
        EMPTY_SECRET_SPEC,
        Collections.emptyMap(), StateData.zero());

    var labels = pod.getMetadata().getLabels();
    assertThat(labels, hasEntry(COMPONENT_ID, normalize(WORKFLOW_INSTANCE.workflowId().componentId())));
    assertThat(labels, hasEntry(WORKFLOW_ID, normalize(WORKFLOW_INSTANCE.workflowId().id())));
    assertThat(labels, hasEntry(PARAMETER, normalize(WORKFLOW_INSTANCE.parameter())));
    assertThat(labels, hasEntry(EXECUTION_ID, spec.executionId()));
    assertThat(labels, hasEntry(TRIGGER_TYPE, TriggerUtil.triggerType(Trigger.natural())));
    assertThat(labels, hasEntry(ENVIRONMENT, STYX_ENVIRONMENT));
  }

  @Test
  public void shouldAddEnvVariables() {
    DockerRunner.RunSpec spec =
        DockerRunner.RunSpec.builder().executionId("eid").imageName("busybox").trigger(Trigger.natural()).build();
    var pod = createPod(
        WORKFLOW_INSTANCE,
        spec,
        EMPTY_SECRET_SPEC,
        Map.of("env-var-1", "some-value"), StateData.zero());

    boolean containsEnvVar = containerContainsEnvKey(pod.getSpec().getContainers().get(0), "env-var-1");
    assertThat(containsEnvVar, is(true));
  }

  private boolean containerContainsEnvKey(Container container, String key) {
    for (EnvVar envVar : container.getEnv()) {
      if (Objects.equals(key, envVar.getName())) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void shouldDisableTerminationLoggingWhenFalse() {
    Pod pod = createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC, Collections.emptyMap(), StateData.zero());

    Map<String, String> annotations = pod.getMetadata().getAnnotations();
    assertThat(annotations.get(DOCKER_TERMINATION_LOGGING_ANNOTATION), is("false"));

    List<Container> containers = pod.getSpec().getContainers();
    Optional<EnvVar> terminationLogVar = containers.get(0).getEnv().stream()
        .filter(e -> TERMINATION_LOG.equals(e.getName())).findAny();
    assertThat(terminationLogVar.orElseThrow().getValue(), is("/dev/termination-log"));
  }

  @Test
  public void shouldEnableTerminationLoggingWhenTrue() {
    Pod pod = createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.builder()
            .executionId("eid")
            .imageName("busybox")
            .terminationLogging(true)
            .build(),
        EMPTY_SECRET_SPEC,
        Collections.emptyMap(), StateData.zero());

    Map<String, String> annotations = pod.getMetadata().getAnnotations();
    assertThat(annotations.get(DOCKER_TERMINATION_LOGGING_ANNOTATION), is("true"));

    List<Container> containers = pod.getSpec().getContainers();
    Optional<EnvVar> terminationLogVar = containers.get(0).getEnv().stream()
        .filter(e -> TERMINATION_LOG.equals(e.getName())).findAny();
    assertThat(terminationLogVar.orElseThrow().getValue(), is("/dev/termination-log"));
  }

  @Test
  public void shouldHaveRestartPolicyNever() {
    Pod pod = createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC, Collections.emptyMap(), StateData.zero());

    assertThat(pod.getSpec().getRestartPolicy(), is("Never"));
  }

  @Test
  public void shouldNotHaveSecretsMountIfNoSecret() {
    Pod pod = createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC, Collections.emptyMap(), StateData.zero());

    List<Volume> volumes = pod.getSpec().getVolumes();
    List<Container> containers = pod.getSpec().getContainers();
    assertThat(volumes.size(), is(0));
    assertThat(containers.size(), is(2));
    assertThat(containers.get(0).getName(), is(MAIN_CONTAINER_NAME));

    Container container = containers.get(0);
    List<VolumeMount> volumeMounts = container.getVolumeMounts();
    assertThat(volumeMounts.size(), is(0));
  }

  @Test
  public void shouldConfigureEnvironmentVariables() {
    final Pod pod = createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.builder()
            .executionId(TEST_EXECUTION_ID)
            .args("hello", "world")
            .imageName("busybox")
            .trigger(Trigger.unknown("trigger-id"))
            .commitSha("abc123")
            .build(),
        EMPTY_SECRET_SPEC,
        Collections.emptyMap(), StateData.newBuilder().tries(1).build());

    final List<EnvVar> envVars = pod.getSpec().getContainers().get(0).getEnv();

    assertThat(envVars, hasItem(envVar(COMPONENT_ID, WORKFLOW_INSTANCE.workflowId().componentId())));
    assertThat(envVars, hasItem(envVar(WORKFLOW_ID, WORKFLOW_INSTANCE.workflowId().id())));
    assertThat(envVars, hasItem(envVar(PARAMETER, WORKFLOW_INSTANCE.parameter())));
    assertThat(envVars, hasItem(envVar(SERVICE_ACCOUNT, "")));
    assertThat(envVars, hasItem(envVar(DOCKER_IMAGE, "busybox")));
    assertThat(envVars, hasItem(envVar(TRIGGER_ID, "trigger-id")));
    assertThat(envVars, hasItem(envVar(TRIGGER_TYPE, "unknown")));
    assertThat(envVars, hasItem(envVar(COMMIT_SHA, "abc123")));
    assertThat(envVars, hasItem(envVar(EXECUTION_ID, TEST_EXECUTION_ID)));
    assertThat(envVars, hasItem(envVar(DOCKER_ARGS, "hello world")));
    assertThat(envVars, hasItem(envVar(ENVIRONMENT, STYX_ENVIRONMENT)));
    assertThat(envVars, hasItem(envVar(LOGGING, "structured")));
    assertThat(envVars, hasItem(envVar(STYX_EXECUTION_TRIES, "1")));
  }

  private Pod createPod(WorkflowInstance workflowInstance,
                        DockerRunner.RunSpec runSpec,
                        KubernetesSecretSpec secretSpec,
                        Map<String, String> executionEnvVars,
                        StateData stateData) {
    return KubernetesDockerRunner
        .createPod(workflowInstance, runSpec, secretSpec, STYX_ENVIRONMENT, PodMutator.NOOP,
            executionEnvVars, stateData.tries());
  }
}
