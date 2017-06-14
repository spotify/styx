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

import static com.spotify.styx.docker.KubernetesDockerRunner.DOCKER_TERMINATION_LOGGING_ANNOTATION;
import static com.spotify.styx.docker.KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION;
import static com.spotify.styx.docker.KubernetesDockerRunner.TERMINATION_LOG;
import static java.util.Optional.empty;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.docker.KubernetesDockerRunner.KubernetesSecretSpec;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.testdata.TestData;
import io.fabric8.kubernetes.api.model.Container;
import io.fabric8.kubernetes.api.model.EnvVar;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.Volume;
import io.fabric8.kubernetes.api.model.VolumeMount;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class KubernetesDockerRunnerPodResourceTest {

  private static final String UUID_REGEX =
      "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

  private static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "2016-04-04");

  private final static KubernetesSecretSpec EMPTY_SECRET_SPEC = KubernetesSecretSpec.builder().build();

  private static final String TEST_EXECUTION_ID = "execution_1";

  @Test
  public void shouldAddLatestTag() throws Exception {
    Pod pod = KubernetesDockerRunner.createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC);

    List<Container> containers = pod.getSpec().getContainers();
    assertThat(containers.size(), is(1));

    Container container = containers.get(0);
    assertThat(container.getImage(), is("busybox:latest"));
  }

  @Test
  public void shouldUseConfiguredTag() throws Exception {
    Pod pod = KubernetesDockerRunner.createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.simple("eid", "busybox:v7"), EMPTY_SECRET_SPEC);

    List<Container> containers = pod.getSpec().getContainers();
    assertThat(containers.size(), is(1));

    Container container = containers.get(0);
    assertThat(container.getImage(), is("busybox:v7"));
  }

  @Test
  public void shouldAddArgs() throws Exception {
    Pod pod = KubernetesDockerRunner.createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.simple("eid", "busybox", "echo", "foo", "bar"), EMPTY_SECRET_SPEC);

    List<Container> containers = pod.getSpec().getContainers();
    assertThat(containers.size(), is(1));

    Container container = containers.get(0);
    assertThat(container.getArgs(), contains("echo", "foo", "bar"));
  }

  @Test
  public void shouldAddWorkflowInstanceAnnotation() throws Exception {
    Pod pod = KubernetesDockerRunner.createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC);

    Map<String, String> annotations = pod.getMetadata().getAnnotations();
    assertThat(annotations, hasEntry(STYX_WORKFLOW_INSTANCE_ANNOTATION, WORKFLOW_INSTANCE.toKey()));

    WorkflowInstance workflowInstance =
        WorkflowInstance.parseKey(annotations.get(STYX_WORKFLOW_INSTANCE_ANNOTATION));
    assertThat(workflowInstance, is(WORKFLOW_INSTANCE));
  }

  @Test
  public void shouldDisableTerminationLoggingWhenFalse() throws Exception {
    Pod pod = KubernetesDockerRunner.createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC);

    Map<String, String> annotations = pod.getMetadata().getAnnotations();
    assertThat(annotations.get(DOCKER_TERMINATION_LOGGING_ANNOTATION), is("false"));

    List<Container> containers = pod.getSpec().getContainers();
    Optional<EnvVar> terminationLogVar = containers.get(0).getEnv().stream()
        .filter(e -> TERMINATION_LOG.equals(e.getName())).findAny();
    assertThat(terminationLogVar.get().getValue(), is("/dev/termination-log"));
  }

  @Test
  public void shouldEnableTerminationLoggingWhenTrue() throws Exception {
    Pod pod = KubernetesDockerRunner.createPod(
        WORKFLOW_INSTANCE,
            DockerRunner.RunSpec.create(
                "eid", "busybox", ImmutableList.of(), true,
                empty(), empty(), empty()), EMPTY_SECRET_SPEC);

    Map<String, String> annotations = pod.getMetadata().getAnnotations();
    assertThat(annotations.get(DOCKER_TERMINATION_LOGGING_ANNOTATION), is("true"));

    List<Container> containers = pod.getSpec().getContainers();
    Optional<EnvVar> terminationLogVar = containers.get(0).getEnv().stream()
        .filter(e -> TERMINATION_LOG.equals(e.getName())).findAny();
    assertThat(terminationLogVar.get().getValue(), is("/dev/termination-log"));
  }

  @Test
  public void shouldHaveRestartPolicyNever() throws Exception {
    Pod pod = KubernetesDockerRunner.createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC);

    assertThat(pod.getSpec().getRestartPolicy(), is("Never"));
  }

  @Test
  public void shouldNotHaveSecretsMountIfNoSecret() throws Exception {
    Pod pod = KubernetesDockerRunner.createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.simple("eid", "busybox"), EMPTY_SECRET_SPEC);

    List<Volume> volumes = pod.getSpec().getVolumes();
    List<Container> containers = pod.getSpec().getContainers();
    assertThat(volumes.size(), is(0));
    assertThat(containers.size(), is(1));

    Container container = containers.get(0);
    List<VolumeMount> volumeMounts = container.getVolumeMounts();
    assertThat(volumeMounts.size(), is(0));
  }

  @Test
  public void shouldConfigureSecretsMount() throws Exception {
    WorkflowConfiguration.Secret secret = WorkflowConfiguration.Secret.create("my-secret", "/etc/secrets");
    KubernetesSecretSpec secretSpec = KubernetesSecretSpec.builder()
        .customSecret(secret)
        .build();
    Pod pod = KubernetesDockerRunner.createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec.create(
            "eid", "busybox", ImmutableList.of(), false, Optional.of(secret),
            empty(), empty()), secretSpec);

    List<Volume> volumes = pod.getSpec().getVolumes();
    List<Container> containers = pod.getSpec().getContainers();
    assertThat(volumes.size(), is(1));
    assertThat(containers.size(), is(1));

    Volume volume = volumes.get(0);
    assertThat(volume.getName(), is("my-secret"));
    assertThat(volume.getSecret().getSecretName(), is("my-secret"));

    Container container = containers.get(0);
    List<VolumeMount> volumeMounts = container.getVolumeMounts();
    assertThat(volumeMounts.size(), is(1));

    VolumeMount volumeMount = volumeMounts.get(0);
    assertThat(volumeMount.getName(), is("my-secret"));
    assertThat(volumeMount.getMountPath(), is("/etc/secrets"));
    assertThat(volumeMount.getReadOnly(), is(true));
  }

  @Test
  public void shouldConfigureEnvironmentVariables() throws Exception {
    Pod pod = KubernetesDockerRunner.createPod(
        WORKFLOW_INSTANCE,
        DockerRunner.RunSpec
            .create(TEST_EXECUTION_ID, "busybox", ImmutableList.of(), false, empty(),
                    empty(), Optional.of(Trigger.unknown("trigger-id"))), EMPTY_SECRET_SPEC);
    List<EnvVar> envVars = pod.getSpec().getContainers().get(0).getEnv();

    EnvVar workflow = new EnvVar();
    workflow.setName(KubernetesDockerRunner.WORKFLOW_ID);
    workflow.setValue(WORKFLOW_INSTANCE.workflowId().id());
    EnvVar component = new EnvVar();
    component.setName(KubernetesDockerRunner.COMPONENT_ID);
    component.setValue(WORKFLOW_INSTANCE.workflowId().componentId());
    EnvVar parameter = new EnvVar();
    parameter.setName(KubernetesDockerRunner.PARAMETER);
    parameter.setValue(WORKFLOW_INSTANCE.parameter());
    EnvVar triggerId = new EnvVar();
    triggerId.setName(KubernetesDockerRunner.TRIGGER_ID);
    triggerId.setValue("trigger-id");
    EnvVar triggerName = new EnvVar();
    triggerName.setName(KubernetesDockerRunner.TRIGGER_TYPE);
    triggerName.setValue("unknown");

    assertThat(envVars.size(), is(8));
    assertThat(envVars, hasItem(component));
    assertThat(envVars, hasItem(workflow));
    assertThat(envVars, hasItem(parameter));
    assertThat(envVars, hasItem(triggerId));
    assertThat(envVars, hasItem(triggerName));

    EnvVar execution = envVars.get(4);
    assertThat(execution.getName(),is(KubernetesDockerRunner.EXECUTION_ID));
    assertThat(execution.getValue(), is(TEST_EXECUTION_ID));
  }
}
