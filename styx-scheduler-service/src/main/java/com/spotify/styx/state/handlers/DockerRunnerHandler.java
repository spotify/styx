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

import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.docker.InvalidExecutionException;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.ResourceNotFoundException;
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
        final RunSpec runSpec;
        try {
          runSpec = createRunSpec(state);
        } catch (ResourceNotFoundException e) {
          LOG.error("Unable to start docker procedure.", e);
          stateManager.receiveIgnoreClosed(Event.halt(state.workflowInstance()));
          return;
        }

        // Emit submitted event first to guarantee it is observed before events from the pod
        final Event submitted = Event.submitted(state.workflowInstance(), runSpec.executionId());
        try {
          stateManager.receive(submitted);
        } catch (IsClosedException isClosedException) {
          LOG.warn("Could not emit 'submitted' event", isClosedException);
          return;
        }

        try {
          LOG.info("running:{} image:{} args:{} termination_logging:{}", state.workflowInstance().toKey(),
              runSpec.imageName(), runSpec.args(), runSpec.terminationLogging());
          dockerRunner.start(state.workflowInstance(), runSpec);
        } catch (Throwable e) {
          try {
            final String msg = "Failed the docker starting procedure for " + state.workflowInstance().toKey();
            if (isUserError(e)) {
              LOG.info("{}: {}", msg, e.getMessage());
            } else {
              LOG.error(msg, e);
            }
            stateManager.receive(Event.runError(state.workflowInstance(), e.getMessage()));
          } catch (IsClosedException isClosedException) {
            LOG.warn("Failed to send 'runError' event", isClosedException);
          }
        }
        break;

      case TERMINATED:
      case FAILED:
      case ERROR:
        if (state.data().executionId().isPresent()) {
          final String executionId = state.data().executionId().get();
          dockerRunner.cleanup(state.workflowInstance(), executionId);
        }
        break;

      default:
        // do nothing
    }
  }

  private boolean isUserError(Throwable e) {
    return e instanceof InvalidExecutionException;
  }

  private RunSpec createRunSpec(RunState state) throws ResourceNotFoundException {
    final Optional<ExecutionDescription> executionDescriptionOpt = state.data().executionDescription();

    final ExecutionDescription executionDescription = executionDescriptionOpt.orElseThrow(
        () -> new ResourceNotFoundException("Missing execution description for "
                                            + state.workflowInstance().toKey()));

    // For backwards compatibility, create an execution ID here for RunStates that do not have one as
    // they transitioned through a SUBMITTING state that did not create execution IDs. This execution ID
    // will be added to the RunState through the submitted event.
    final String executionId = state.data().executionId()
        .orElseGet(ExecutionDescriptionHandler::createExecutionId);

    final String dockerImage = executionDescription.dockerImage();
    final List<String> dockerArgs = executionDescription.dockerArgs();
    final String parameter = state.workflowInstance().parameter();
    final List<String> command = argsReplace(dockerArgs, parameter);
    return RunSpec.builder()
        .executionId(executionId)
        .imageName(dockerImage)
        .args(command)
        .terminationLogging(executionDescription.dockerTerminationLogging())
        .secret(executionDescription.secret())
        .serviceAccount(executionDescription.serviceAccount())
        .trigger(state.data().trigger())
        .commitSha(state.data().executionDescription().flatMap(ExecutionDescription::commitSha))
        .build();
  }

  private static List<String> argsReplace(List<String> template, String parameter) {
    List<String> result = new ArrayList<>(template);

    Collections.replaceAll(result, "{}", parameter);
    return result;
  }
}
