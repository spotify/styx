/*-
 * -\-\-
 * Spotify styx
 * --
 * Copyright (C) 2018 Spotify AB
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

import com.spotify.styx.model.WorkflowInstance;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;

class KubernetesDockerRunnerTestUtil {

  private KubernetesDockerRunnerTestUtil() {
    throw new UnsupportedOperationException();
  }

  static Pod createPod(WorkflowInstance workflowInstance, DockerRunner.RunSpec runSpec,
                       KubernetesDockerRunner.KubernetesSecretSpec secretSpec) {
    final PodBuilder podBuilder = new PodBuilder()
        .withNewMetadata()
        .withName(runSpec.executionId() + "-randomsuffix")
        .addToAnnotations(KubernetesDockerRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION,
            workflowInstance.toKey())
        .addToAnnotations(KubernetesDockerRunner.DOCKER_TERMINATION_LOGGING_ANNOTATION,
            String.valueOf(runSpec.terminationLogging()))
        .addToLabels("controller-uid", "00000000-0000-0000-0000-000000000000")
        .addToLabels("job-name", runSpec.executionId())
        .endMetadata();

    podBuilder
        .withSpec(KubernetesDockerRunner.createPodSpec(workflowInstance, runSpec, secretSpec));

    return podBuilder.build();
  }
}
