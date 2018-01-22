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
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.MissingRequiredPropertyException;
import com.spotify.styx.util.ResourceNotFoundException;
import com.spotify.styx.util.WorkflowValidator;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionDescriptionHandler implements OutputHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionDescriptionHandler.class);

  private static final String STYX_RUN = "styx-run";

  private final Storage storage;
  private final StateManager stateManager;
  private final WorkflowValidator validator;

  public ExecutionDescriptionHandler(
      Storage storage,
      StateManager stateManager,
      WorkflowValidator validator) {
    this.storage = requireNonNull(storage);
    this.stateManager = requireNonNull(stateManager);
    this.validator = requireNonNull(validator);
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
          } catch (IsClosedException isClosedException) {
            LOG.warn("Could not send 'submit' event", isClosedException);
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
          } catch (IsClosedException isClosedException) {
            LOG.warn("Failed to send 'runError' event", isClosedException);
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

    final Workflow workflow = storage.workflow(workflowId).orElseThrow(
        () -> new ResourceNotFoundException(format("Missing %s, halting %s",
                                                   workflowId, workflowInstance)));

    final String dockerImage = workflow.configuration().dockerImage().orElseThrow(
        () -> new MissingRequiredPropertyException(format("%s has no docker image, halting %s",
                                                          workflowId,
                                                          workflowInstance))
    );

    final Collection<String> errors = validator.validateWorkflow(workflow);
    if (!errors.isEmpty()) {
      throw new MissingRequiredPropertyException(format(
          "%s configuration is invalid, halting %s. Errors: %s",
          workflowId, workflowInstance, errors));
    }


    final List<String> dockerArgs = workflow.configuration().dockerArgs()
        .orElse(Collections.emptyList());

    return ExecutionDescription.builder()
        .dockerImage(dockerImage)
        .dockerArgs(dockerArgs)
        .dockerTerminationLogging(workflow.configuration().dockerTerminationLogging())
        .secret(workflow.configuration().secret())
        .serviceAccount(workflow.configuration().serviceAccount())
        .commitSha(workflow.configuration().commitSha())
        .build();
  }

  static String createExecutionId() {
    return STYX_RUN + "-" + UUID.randomUUID().toString();
  }
}
