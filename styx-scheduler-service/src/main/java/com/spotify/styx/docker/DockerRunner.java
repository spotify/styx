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

import static java.util.Optional.empty;

import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import java.io.Closeable;
import java.io.IOException;
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
   * @return The execution id for the started workflow instance
   */
  String start(WorkflowInstance workflowInstance, RunSpec runSpec) throws IOException;

  /**
   * Execute cleanup operations for when an execution finishes.
   * @param executionId The execution id for which the cleanup code is called
   */
  void cleanup(String executionId);

  @AutoValue
  abstract class RunSpec {

    public abstract String imageName();

    public abstract ImmutableList<String> args();

    public abstract boolean terminationLogging();

    public abstract Optional<WorkflowConfiguration.Secret> secret();

    public abstract Optional<Trigger> trigger();

    public static RunSpec create(
        String imageName,
        ImmutableList<String> args,
        boolean terminationLogging,
        Optional<WorkflowConfiguration.Secret> secret,
        Optional<Trigger> trigger) {
      return new AutoValue_DockerRunner_RunSpec(imageName, args, terminationLogging, secret, trigger);
    }

    public static RunSpec simple(String imageName, String... args) {
      return new AutoValue_DockerRunner_RunSpec(imageName, ImmutableList.copyOf(args), false, empty(), empty());
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

  static DockerRunner kubernetes(NamespacedKubernetesClient kubernetesClient, StateManager stateManager, Stats stats) {
    final KubernetesDockerRunner dockerRunner =
        new KubernetesDockerRunner(kubernetesClient, stateManager, stats);

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
