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

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.DockerImageValidator;
import com.spotify.styx.util.MissingRequiredPropertyException;
import com.spotify.styx.util.ResourceNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionDescriptionHandler implements OutputHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionDescriptionHandler.class);

  private static final String STYX_RUN = "styx-run";

  private final Storage storage;
  private final StateManager stateManager;
  private final DockerImageValidator dockerImageValidator;

  // TODO: make runtime overridable (i.e. Storage.config())
  private static final String DEFAULT_MEM_REQUEST = "512Mi";
  private static final String DEFAULT_MEM_LIMIT =  "1024Mi";

  public ExecutionDescriptionHandler(
      Storage storage,
      StateManager stateManager,
      DockerImageValidator dockerImageValidator) {
    this.storage = requireNonNull(storage);
    this.stateManager = requireNonNull(stateManager);
    this.dockerImageValidator = requireNonNull(dockerImageValidator);
  }

  @Override
  public void transitionInto(RunState state) {
    final WorkflowInstance workflowInstance = state.workflowInstance();

    switch (state.state()) {
      case PREPARE:
        try {
          final Event submitEvent = Event.submit(
              state.workflowInstance(), getExecDescription(workflowInstance), createExecutionId());
          try {
            stateManager.receive(submitEvent);
          } catch (StateManager.IsClosed isClosed) {
            LOG.warn("Could not send 'submit' event", isClosed);
          }
        } catch (ResourceNotFoundException e) {
          LOG.info("Workflow {} does not exist, halting {}", workflowInstance.workflowId(),
                   workflowInstance);
          stateManager.receiveIgnoreClosed(Event.halt(workflowInstance));
        } catch (MissingRequiredPropertyException e) {
          LOG.warn("Failed to prepare execution description for "
                   + state.workflowInstance().toKey(), e);
          stateManager.receiveIgnoreClosed(Event.halt(workflowInstance));
        } catch (IOException e) {
          try {
            LOG.error("Failed to retrieve execution description for " + state.workflowInstance().toKey(), e);
            stateManager.receive(Event.runError(state.workflowInstance(), e.getMessage()));
          } catch (StateManager.IsClosed isClosed) {
            LOG.warn("Failed to send 'runError' event", isClosed);
          }
        }
        break;

      default:
        // do nothing
    }
  }

  private ExecutionDescription getExecDescription(WorkflowInstance workflowInstance)
      throws IOException, MissingRequiredPropertyException {
    final WorkflowId workflowId = workflowInstance.workflowId();

    final Optional<Workflow> workflowOpt = storage.workflow(workflowId);
    final Workflow workflow = workflowOpt.orElseThrow(
        () -> new ResourceNotFoundException(format("Missing %s, halting %s", workflowId, workflowInstance)));

    final Optional<List<String>> dockerArgsOpt = workflow.configuration().dockerArgs();
    if (!dockerArgsOpt.isPresent()) {
      throw new MissingRequiredPropertyException(format("%s has no docker args, halting %s",
                                                        workflowId, workflowInstance));
    }

    final WorkflowState workflowState = storage.workflowState(workflow.id());

    final Optional<String> dockerImageOpt = workflowState.dockerImage().isPresent()
                                            ? workflowState.dockerImage()
                                            : workflow.configuration().dockerImage(); // backwards compatibility

    if (!dockerImageOpt.isPresent()) {
      throw new MissingRequiredPropertyException(format("%s has no docker image, halting %s",
                                                        workflowId, workflowInstance));
    }

    final Collection<String> errors = dockerImageValidator.validateImageReference(dockerImageOpt.get());
    if (!errors.isEmpty()) {
      throw new MissingRequiredPropertyException(format(
          "%s has an invalid docker image reference, halting %s. Errors: %s",
          workflowId, workflowInstance, errors));
    }

    return ExecutionDescription.builder()
        .dockerImage(dockerImageOpt.get())
        .dockerArgs(dockerArgsOpt.get())
        .dockerTerminationLogging(workflow.configuration().dockerTerminationLogging())
        .secret(workflow.configuration().secret())
        .serviceAccount(workflow.configuration().serviceAccount())
        .commitSha(workflowState.commitSha())
        .memLimit(workflow.configuration().memLimit().orElse(DEFAULT_MEM_LIMIT))
        .memRequest(workflow.configuration().memRequest().orElse(DEFAULT_MEM_REQUEST))
        .build();
  }

  static String createExecutionId() {
    return STYX_RUN + "-" + UUID.randomUUID().toString();
  }
}
