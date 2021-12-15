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
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.util.Debug;
import io.norberg.automatter.AutoMatter;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines an interface to the Docker execution environment
 */
public interface DockerRunner extends Closeable {

  Logger LOG = LoggerFactory.getLogger(DockerRunner.class);

  /**
   * Starts a workflow instance asynchronously.
   * @param runState         The run state of the instance.
   * @param runSpec          Specification of what to run
   * @return runner ID
   */
  String start(RunState runState, RunSpec runSpec) throws IOException;

  /**
   * Check the status of a workflow instance execution.
   *
   * @param runState         The run state of the instance.
   */
  void poll(RunState runState);

  /**
   * Perform cleanup for resources such as secrets etc. Resources that are not in use by any currently live workflows
   * should be removed.
   */
  void cleanup() throws IOException;

  @AutoMatter
  interface RunSpec {

    String executionId();

    String imageName();

    List<String> args();

    boolean terminationLogging();

    Optional<String> serviceAccount();

    Optional<Trigger> trigger();

    Optional<String> commitSha();

    Optional<String> memRequest();

    Optional<String> memLimit();

    Map<String, String> env();

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

  static DockerRunner kubernetes(String id,
                                 Fabric8KubernetesClient kubernetesClient,
                                 StateManager stateManager,
                                 Stats stats,
                                 ServiceAccountKeyManager serviceAccountKeyManager,
                                 Debug debug,
                                 String styxEnvironment,
                                 PodMutator podMutator,
                                 Map<String, String> executionEnvVars) {
    final KubernetesGCPServiceAccountSecretManager serviceAccountSecretManager =
        new KubernetesGCPServiceAccountSecretManager(kubernetesClient, serviceAccountKeyManager, stats);
    final KubernetesDockerRunner dockerRunner =
        new KubernetesDockerRunner(id, kubernetesClient, stateManager, stats,
            serviceAccountSecretManager, debug, styxEnvironment, podMutator, executionEnvVars);

    dockerRunner.init();

    return dockerRunner;
  }

  /**
   * Creates a {@link DockerRunner} that will dynamically create and route to other docker runner
   * instances using the given factory.
   *
   * <p>The active docker runner id will be read from dockerId supplier on each routing decision.
   */
  static DockerRunner routing(DockerRunnerFactory dockerRunnerFactory, Function<RunState, String> dockerId) {
    return new RoutingDockerRunner(dockerRunnerFactory, dockerId);
  }

  /**
   * Factory for {@link DockerRunner} instances identified by a string identifier
   */
  interface DockerRunnerFactory extends Function<String, DockerRunner> { }
}
