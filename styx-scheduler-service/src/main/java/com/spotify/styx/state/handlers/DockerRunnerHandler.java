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

import static com.spotify.styx.state.handlers.HandlerUtil.argsReplace;
import static java.util.Objects.requireNonNull;

import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.docker.InvalidExecutionException;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.LimitsResource;
import com.spotify.styx.model.RequestsResource;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.IsClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link OutputHandler} that starts docker runs on {@link RunState.State#SUBMITTED} transitions
 */
public class DockerRunnerHandler extends AbstractRunnerHandler {

  private static final Logger LOG = LoggerFactory.getLogger(DockerRunnerHandler.class);

  private final DockerRunner dockerRunner;

  public DockerRunnerHandler(DockerRunner dockerRunner) {
    super(desc -> desc.dockerImage().isPresent());
    this.dockerRunner = requireNonNull(dockerRunner);
  }

  private boolean isUserError(Throwable e) {
    return e instanceof InvalidExecutionException;
  }

  @Override
  public void safeTransitionInto(RunState state, EventRouter eventRouter) {
    switch (state.state()) {
      case SUBMITTING:
        final var runSpec = createRunSpec(state);

        final String runnerId;
        try {
          LOG.info("running: {}, spec: {}, state: {}", state.workflowInstance(), runSpec, state);
          runnerId = dockerRunner.start(state, runSpec);
        } catch (Throwable e) {
          try {
            final var msg = "Failed the docker starting procedure for " + state.workflowInstance();
            if (isUserError(e)) {
              LOG.info("{}: {}", msg, e.getMessage());
            } else {
              LOG.error(msg, e);
            }
            eventRouter.receive(Event.runError(state.workflowInstance(), e.getMessage()), state.counter());
          } catch (IsClosedException isClosedException) {
            LOG.warn("Failed to send 'runError' event", isClosedException);
          }
          return;
        }

        // Emit `submitted` _after_ starting execution to ensure that we retry in case of failure.
        final var submitted = Event.submitted(state.workflowInstance(), runSpec.executionId(), runnerId);
        try {
          eventRouter.receive(submitted, state.counter());
        } catch (IsClosedException isClosedException) {
          LOG.warn("Could not emit 'submitted' event", isClosedException);
        }
        break;

      case SUBMITTED:
      case RUNNING:
        // This handler will be invoked regularly as long as the workflow instance is active, so while it is in the
        // SUBMITTED and RUNNING states, we poll the runner to check the status and health of the container.
        dockerRunner.poll(state);
        break;

      default:
        // do nothing
    }
  }

  private RunSpec createRunSpec(RunState state) {
    final var executionDescription = state.data().executionDescription().orElseThrow();
    final var executionId = state.data().executionId().orElseThrow();
    final var dockerImage = executionDescription.dockerImage().orElseThrow();
    final var dockerArgs = executionDescription.dockerArgs();
    final var parameter = state.workflowInstance().parameter();
    final var command = argsReplace(dockerArgs, parameter);
    return RunSpec.builder()
        .executionId(executionId)
        .imageName(dockerImage)
        .args(command)
        .terminationLogging(executionDescription.dockerTerminationLogging())
        .serviceAccount(executionDescription.serviceAccount())
        .trigger(state.data().trigger())
        .commitSha(state.data().executionDescription().flatMap(ExecutionDescription::commitSha))
        .memRequest(state.data().executionDescription().flatMap(ExecutionDescription::requests)
            .flatMap(RequestsResource::memory))
        .memLimit(state.data().executionDescription().flatMap(ExecutionDescription::limits)
            .flatMap(LimitsResource::memory))
        .cpuRequest(state.data().executionDescription().flatMap(ExecutionDescription::requests)
            .flatMap(RequestsResource::cpu))
        .cpuLimit(state.data().executionDescription().flatMap(ExecutionDescription::limits)
            .flatMap(LimitsResource::cpu))
        .env(executionDescription.env())
        .build();
  }
}
