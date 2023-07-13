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
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import com.spotify.styx.MissingRequiredPropertyException;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.model.LimitsResource;
import com.spotify.styx.model.RequestsResource;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.ResourceNotFoundException;
import com.spotify.styx.util.WorkflowValidator;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExecutionDescriptionHandler implements OutputHandler {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionDescriptionHandler.class);

  private static final String STYX_RUN = "styx-run";

  private final Storage storage;
  private final WorkflowValidator validator;

  private final Function<String, String> styxExecIdToFlyteNameMapper;

  public ExecutionDescriptionHandler(
      Storage storage,
      WorkflowValidator validator) {
    this(storage, validator, new StyxIdToFlyteExecNameMapper());
  }

  ExecutionDescriptionHandler(Storage storage,
                              WorkflowValidator validator,
                              Function<String, String> styxExecIdToFlyteNameMapper) {
    this.storage = requireNonNull(storage);
    this.validator = requireNonNull(validator);
    this.styxExecIdToFlyteNameMapper = styxExecIdToFlyteNameMapper;
  }

  @Override
  public void transitionInto(RunState state, EventRouter eventRouter) {
    final WorkflowInstance workflowInstance = state.workflowInstance();

    switch (state.state()) {
      case PREPARE:
        try {
          final String executionId = createExecutionId();
          final String flyteExecutionId = styxExecIdToFlyteNameMapper.apply(executionId);
          final Event submitEvent = Event.submit(
              state.workflowInstance(),
              getExecDescription(workflowInstance, state.data(), flyteExecutionId),
              executionId);
          try {
            eventRouter.receive(submitEvent, state.counter());
          } catch (IsClosedException isClosedException) {
            LOG.warn("Could not send 'submit' event", isClosedException);
          }
        } catch (ResourceNotFoundException e) {
          LOG.info("Halting {}: {}", workflowInstance, e.getMessage());
          eventRouter.receiveIgnoreClosed(Event.halt(workflowInstance), state.counter());
        } catch (MissingRequiredPropertyException e) {
          LOG.warn("Failed to prepare execution description for " + state.workflowInstance(), e);
          eventRouter.receiveIgnoreClosed(Event.halt(workflowInstance), state.counter());
        } catch (IOException e) {
          try {
            LOG.error("Failed to retrieve execution description for " + state.workflowInstance(), e);
            eventRouter.receive(Event.runError(state.workflowInstance(), e.getMessage()), state.counter());
          } catch (IsClosedException isClosedException) {
            LOG.warn("Failed to send 'runError' event", isClosedException);
          }
        }
        break;

      default:
        // do nothing
    }
  }

  private ExecutionDescription getExecDescription(WorkflowInstance workflowInstance, StateData data, String flyteExecutionId)
      throws IOException, MissingRequiredPropertyException {
    var workflowId = workflowInstance.workflowId();

    var workflowWithState = storage.workflowWithState(workflowId).orElseThrow(
        () -> new ResourceNotFoundException(format("Missing %s, halting %s", workflowId, workflowInstance)));

    var workflow = workflowWithState.workflow();

    var isWorkflowEnabled = workflowWithState.state().enabled().orElse(false);
    var isNaturalTrigger = data.trigger().map(t -> t.equals(Trigger.natural())).orElse(false);

    if (!isWorkflowEnabled && isNaturalTrigger) {
      throw new MissingRequiredPropertyException(format("%s is disabled, halting %s", workflowId, workflowInstance));
    }

    final Collection<String> errors = validator.validateWorkflow(workflow);
    if (!errors.isEmpty()) {
      throw new MissingRequiredPropertyException(format(
          "%s configuration is invalid, halting %s. Errors: %s",
          workflowId, workflowInstance, errors));
    }

    final Map<String, String> env = new HashMap<>(workflow.configuration().env());
    data.triggerParameters().ifPresent(p -> env.putAll(p.env()));

    final Optional<String> dockerImage = workflow.configuration().dockerImage();
    final List<String> dockerArgs = workflow.configuration().dockerArgs()
        .orElse(Collections.emptyList());
    final Optional<RequestsResource> requests = workflow.configuration().requests();
    final Optional<LimitsResource> limits = workflow.configuration().limits();
    final List<String> command = argsReplace(dockerArgs, workflowInstance.parameter());
    final Optional<FlyteExecConf> flyteExecConf = workflow.configuration().flyteExecConf();

    if (workflow.configuration().flyteExecConf().isEmpty() && dockerImage.isEmpty()) {
      throw new MissingRequiredPropertyException(format("%s has no execution configuration, "
                                                        + "halting %s", workflowId, workflowInstance));
    }

    return ExecutionDescription.builder()
        .dockerImage(dockerImage)
        .dockerArgs(command)
        .dockerTerminationLogging(workflow.configuration().dockerTerminationLogging())
        .serviceAccount(workflow.configuration().serviceAccount())
        .commitSha(workflow.configuration().commitSha())
        .env(env)
        .runningTimeout(workflow.configuration().runningTimeout())
        .retryCondition(workflow.configuration().retryCondition())
        .flyteExecConf(flyteExecConf)
        .flyteExecutionId(Optional.of(flyteExecutionId))
        .requests(requests)
        .limits(limits)
        .build();
  }

  private static String createExecutionId() {
    return STYX_RUN + "-" + UUID.randomUUID().toString();
  }
}
