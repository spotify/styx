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
import com.google.common.util.concurrent.RateLimiter;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.util.ResourceNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link OutputHandler} that starts docker runs on {@link RunState.State#SUBMITTED} transitions
 */
public class DockerRunnerHandler implements OutputHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DockerRunnerHandler.class);

  private final DockerRunner dockerRunner;
  private final StateManager stateManager;
  private final RateLimiter rateLimiter;
  private final ExecutorService executor;

  public DockerRunnerHandler(
      DockerRunner dockerRunner,
      StateManager stateManager,
      RateLimiter rateLimiter,
      ExecutorService executor) {
    this.dockerRunner = requireNonNull(dockerRunner);
    this.stateManager = requireNonNull(stateManager);
    this.rateLimiter = requireNonNull(rateLimiter, "rateLimiter");
    this.executor = requireNonNull(executor, "executor");
  }

  @Override
  public void transitionInto(RunState state) {
    switch (state.state()) {
      case SUBMITTING:
        // Perform rate limited submission on a separate thread pool to avoid blocking the caller.
        executor.submit(() -> {
          rateLimiter.acquire();

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
          } catch (StateManager.IsClosed isClosed) {
            LOG.warn("Could not emit 'submitted' event", isClosed);
            return;
          }

          try {
            LOG.info("running:{} image:{} args:{} termination_logging:{}", state.workflowInstance().toKey(),
                runSpec.imageName(), runSpec.args(), runSpec.terminationLogging());
            dockerRunner.start(state.workflowInstance(), runSpec);
          } catch (Throwable e) {
            try {
              LOG.error("Failed the docker starting procedure for " + state.workflowInstance().toKey(), e);
              stateManager.receive(Event.runError(state.workflowInstance(), e.getMessage()));
            } catch (StateManager.IsClosed isClosed) {
              LOG.warn("Failed to send 'runError' event", isClosed);
            }
          }
        });
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

  private RunSpec createRunSpec(RunState state) throws ResourceNotFoundException {
    final Optional<ExecutionDescription> executionDescriptionOpt = state.data().executionDescription();

    final ExecutionDescription executionDescription = executionDescriptionOpt.orElseThrow(
        () -> new ResourceNotFoundException("Missing execution description for "
                                            + state.workflowInstance().toKey()));

    final String executionId = state.data().executionId()
        // Backwards compatibility fallback
        .orElseGet(ExecutionDescriptionHandler::createExecutionId);

    final String dockerImage = executionDescription.dockerImage();
    final List<String> dockerArgs = executionDescription.dockerArgs();
    final String parameter = state.workflowInstance().parameter();
    final List<String> command = argsReplace(dockerArgs, parameter);
    return RunSpec.create(
        executionId,
        dockerImage,
        ImmutableList.copyOf(command),
        executionDescription.dockerTerminationLogging(),
        executionDescription.secret(),
        executionDescription.serviceAccount(),
        state.data().trigger());
  }

  private static List<String> argsReplace(List<String> template, String parameter) {
    List<String> result = new ArrayList<>(template);

    Collections.replaceAll(result, "{}", parameter);
    return result;
  }
}
