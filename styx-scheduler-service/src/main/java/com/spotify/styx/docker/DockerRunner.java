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

import com.spotify.styx.ServiceAccountKeyManager;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.util.Debug;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.norberg.automatter.AutoMatter;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines an interface to the Docker execution environment
 */
public interface DockerRunner extends Closeable {

  Logger LOG = LoggerFactory.getLogger(DockerRunner.class);

  /**
   * Fetch workflow instance state from execution engine and emit events as needed. For use when
   * when booting in order to recover executions that completed while styx was offline.
   */
  void restore();

  /**
   * Starts a workflow instance asynchronously.
   * @param workflowInstance The workflow instance that the run belongs to
   * @param runSpec          Specification of what to run
   */
  void start(WorkflowInstance workflowInstance, RunSpec runSpec) throws IOException;

  /**
   * Perform cleanup for resources such as secrets etc. Resources that are not in use by any currently live workflows
   * should be removed.
   */
  void cleanup() throws IOException;

  /**
   * Execute cleanup operations for when an execution finishes.
   * @param workflowInstance The workflow instance for which cleanup is called
   * @param executionId The execution id for which the cleanup code is called
   */
  void cleanup(WorkflowInstance workflowInstance, String executionId);

  @AutoMatter
  interface RunSpec {

    String executionId();

    String imageName();

    List<String> args();

    boolean terminationLogging();

    Optional<WorkflowConfiguration.Secret> secret();

    Optional<String> serviceAccount();

    Optional<Trigger> trigger();

    Optional<String> commitSha();

    Optional<String> memRequest();

    Optional<String> memLimit();

    static RunSpecBuilder builder() {
      return new RunSpecBuilder();
    }

    static RunSpec simple(String executionId, String imageName, String... args) {
      return builder()
          .executionId(executionId)
          .imageName(imageName)
          .args(args)
          .build();
    }
  }

  /**
   * A local runner
   *
   * @return A locally operating docker runner
   */
  static DockerRunner local(ScheduledExecutorService executorService, StateManager stateManager) {
    return new LocalDockerRunner(executorService, stateManager);
  }

  static DockerRunner kubernetes(NamespacedKubernetesClient kubernetesClient, StateManager stateManager,
      Stats stats, ServiceAccountKeyManager serviceAccountKeyManager, Debug debug) {
    final KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager =
        new KubernetesGCPServiceAccountSecretManager(kubernetesClient, serviceAccountKeyManager);
    final KubernetesDockerRunner dockerRunner =
        new KubernetesDockerRunner(kubernetesClient, stateManager, stats, serviceAccountSecretManager, debug);

    dockerRunner.init();

    return dockerRunner;
  }

  /**
   * Creates a {@link DockerRunner} that will dynamically create and route to other docker runner
   * instances using the given factory.
   *
   * <p>The active docker runner id will be read from dockerId supplier on each routing decision.
   */
  static DockerRunner routing(DockerRunnerFactory dockerRunnerFactory, Supplier<String> dockerId) {
    return new RoutingDockerRunner(dockerRunnerFactory, dockerId);
  }

  /**
   * Factory for {@link DockerRunner} instances identified by a string identifier
   */
  interface DockerRunnerFactory extends Function<String, DockerRunner> { }
}
