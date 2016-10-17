package com.spotify.styx.docker;

import com.google.common.collect.Maps;
import com.google.common.io.Closer;

import com.spotify.styx.model.WorkflowInstance;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

/**
 * A {@link DockerRunner} that routes to several underlying instances that are created using the
 * injected {@link com.spotify.styx.docker.DockerRunner.DockerRunnerFactory}.
 *
 * Current implementation only creates one runner with the default id.
 */
class RoutingDockerRunner implements DockerRunner {

  public static final String DEFAULT_DOCKER_RUNNER = "default";

  private final DockerRunnerFactory dockerRunnerFactory;

  private final ConcurrentMap<String, DockerRunner> dockerRunners = Maps.newConcurrentMap();

  RoutingDockerRunner(DockerRunnerFactory dockerRunnerFactory) {
    this.dockerRunnerFactory = Objects.requireNonNull(dockerRunnerFactory);
  }

  @Override
  public String start(WorkflowInstance workflowInstance, RunSpec runSpec) throws IOException {
    return get(DEFAULT_DOCKER_RUNNER).start(workflowInstance, runSpec);
  }

  @Override
  public void cleanup(String executionId) {
    get(DEFAULT_DOCKER_RUNNER).cleanup(executionId);
  }

  @Override
  public void close() throws IOException {
    final Closer closer = Closer.create();
    dockerRunners.values().forEach(closer::register);
    closer.close();
  }

  private DockerRunner get(String id) {
    return dockerRunners.computeIfAbsent(id, dockerRunnerFactory);
  }
}
