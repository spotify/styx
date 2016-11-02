/*
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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerCertificateException;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.Image;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.StateManager;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a short term implementation of DockerRunner that keeps track of started containers,
 * periodically polls them for exit status and triggers events when a container is found to
 * have exited.
 */
class LocalDockerRunner implements DockerRunner {

  private static final Logger LOG = LoggerFactory.getLogger(LocalDockerRunner.class);

  private static final String LOCAL_DOCKER_EVENT = "local-docker-event";
  private static final int CHECK_INTERVAL = 5;

  private final DockerClient client;

  private final Map<String, WorkflowInstance> inFlight = Maps.newConcurrentMap();
  private final Set<String> started = Sets.newConcurrentHashSet();

  private final StateManager stateManager;

  LocalDockerRunner(ScheduledExecutorService executorService, StateManager stateManager) {
    LOG.info("creating a client");
    try {
      client = DefaultDockerClient.fromEnv().build();
    } catch (DockerCertificateException e) {
      throw new RuntimeException(e);
    }
    executorService.scheduleAtFixedRate(this::checkStatuses, 1, CHECK_INTERVAL, TimeUnit.SECONDS);
    this.stateManager = Objects.requireNonNull(stateManager);
  }

  @Override
  public String start(WorkflowInstance workflowInstance, RunSpec runSpec) {
    final String imageTag = runSpec.imageName().contains(":")
        ? runSpec.imageName()
        : runSpec.imageName() + ":latest";

    final ContainerCreation creation;
    try {
      boolean found = false;
      for (Image image : client.listImages()) {
        found |= image.repoTags().contains(imageTag);
      }

      if (!found) {
        client.pull(imageTag, System.out::println); // blocking
      }

      final ContainerConfig containerConfig = ContainerConfig.builder()
          .image(imageTag)
          .cmd(runSpec.args())
          .build();
      creation = client.createContainer(containerConfig);
      client.startContainer(creation.id());
    } catch (DockerException | InterruptedException e) {
      throw new RuntimeException(e);
    }

    inFlight.put(creation.id(), workflowInstance);
    LOG.info("Started container with id " + creation.id());
    return creation.id();
  }

  @Override
  public void cleanup(String executionId) {
  }

  private void checkStatuses() {
    LOG.debug("Checking running statuses, {} statuses to check", inFlight.size());

    for (String containerId : inFlight.keySet()) {
      final ContainerInfo containerInfo;
      try {
        containerInfo = client.inspectContainer(containerId);
      } catch (DockerException | InterruptedException e) {
        LOG.error("Error while reading status from docker", e);
        continue;
      }

      if (containerInfo.state().running() && !started.contains(containerId)) {
        final WorkflowInstance workflowInstance = inFlight.get(containerId);
        stateManager.receiveIgnoreClosed(Event.started(workflowInstance));
        started.add(containerId);
      }

      if (!containerInfo.state().running()) {
        final int exitCode = containerInfo.state().exitCode();
        final WorkflowInstance workflowInstance = inFlight.remove(containerId);

        // trigger started event if we didn't see the container in running before
        if (!started.contains(containerId)) {
          stateManager.receiveIgnoreClosed(Event.started(workflowInstance));
        } else {
          started.remove(containerId);
        }
        stateManager.receiveIgnoreClosed(Event.terminate(workflowInstance, exitCode));
      }
    }
  }

  @Override
  public void close() throws IOException {
    client.close();
  }
}
