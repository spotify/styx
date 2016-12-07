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

package com.spotify.styx.state.handlers;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.util.ResourceNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link OutputHandler} that starts docker runs on {@link RunState.State#SUBMITTED} transitions
 */
public class DockerRunnerHandler implements OutputHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DockerRunnerHandler.class);

  private final DockerRunner dockerRunner;
  private final StateManager stateManager;

  public DockerRunnerHandler(
      DockerRunner dockerRunner,
      StateManager stateManager) {
    this.dockerRunner = requireNonNull(dockerRunner);
    this.stateManager = requireNonNull(stateManager);
  }

  @Override
  public void transitionInto(RunState state) {
    switch (state.state()) {
      case SUBMITTING:
        try {
          final String executionId = dockerRunnerStart(state);
          // this is racy
          final Event submitted = Event.submitted(state.workflowInstance(), executionId);
          try {
            stateManager.receive(submitted);
          } catch (StateManager.IsClosed isClosed) {
            LOG.warn("Could not send 'created' event", isClosed);
          }
        } catch (ResourceNotFoundException e) {
          LOG.error("Unable to start docker procedure.", e);
          stateManager.receiveIgnoreClosed(Event.halt(state.workflowInstance()));
        } catch (IOException e) {
          try {
            LOG.error("Failed the docker starting procedure for " + state.workflowInstance().toKey(), e);
            stateManager.receive(Event.runError(state.workflowInstance(), e.getMessage()));
          } catch (StateManager.IsClosed isClosed) {
            LOG.warn("Failed to send 'runError' event", isClosed);
          }
        }
        break;

      case TERMINATED:
      case FAILED:
      case ERROR:
        if (state.data().executionId().isPresent()) {
          final String executionId = state.data().executionId().get();
          LOG.info("Cleaning up {} pod: {}", state.workflowInstance().toKey(), executionId);

          dockerRunner.cleanup(executionId);
        }
        break;

      default:
        // do nothing
    }
  }

  private String dockerRunnerStart(RunState state) throws IOException {
    final WorkflowInstance workflowInstance = state.workflowInstance();
    final Optional<ExecutionDescription> executionDescriptionOpt = state.data().executionDescription();

    final ExecutionDescription executionDescription = executionDescriptionOpt.orElseThrow(
        () -> new ResourceNotFoundException("Missing execution description for "
                                            + state.workflowInstance().toKey()));

    final String dockerImage = executionDescription.dockerImage();
    final List<String> dockerArgs = executionDescription.dockerArgs();
    final String parameter = workflowInstance.parameter();
    final List<String> command = argsReplace(dockerArgs, parameter);
    final DockerRunner.RunSpec runSpec = DockerRunner.RunSpec.create(
        dockerImage,
        ImmutableList.copyOf(command),
        executionDescription.secret());

    LOG.info("running:{} image:{} args:{}", workflowInstance.toKey(), runSpec.imageName(),
             runSpec.args());

    return dockerRunner.start(workflowInstance, runSpec);
  }

  private static List<String> argsReplace(List<String> template, String parameter) {
    List<String> result = new ArrayList<>(template);

    Collections.replaceAll(result, "{}", parameter);
    return result;
  }
}
