/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2020 Spotify AB
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

package com.spotify.styx.flyte;

import static com.spotify.styx.state.RunState.MISSING_DEPS_EXIT_CODE;
import static com.spotify.styx.state.RunState.SUCCESS_EXIT_CODE;
import static com.spotify.styx.state.RunState.UNKNOWN_ERROR_EXIT_CODE;
import static com.spotify.styx.state.RunState.UNRECOVERABLE_FAILURE_EXIT_CODE;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.spotify.styx.flyte.client.FlyteAdminClient;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.state.TriggerVisitor;
import com.spotify.styx.util.IsClosedException;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.admin.ExecutionOuterClass.ExecutionMetadata.ExecutionMode;
import flyteidl.core.Execution;
import flyteidl.core.IdentifierOuterClass;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlyteAdminClientRunner implements FlyteRunner {

  private static final Logger LOG = LoggerFactory.getLogger(FlyteAdminClientRunner.class);
  private final String runnerId;
  private final FlyteAdminClient flyteAdminClient;
  private final StateManager stateManager;

  public FlyteAdminClientRunner(final String runnerId,
                                final FlyteAdminClient flyteAdminClient,
                                final StateManager stateManager) {
    this.runnerId = requireNonNull(runnerId);
    this.flyteAdminClient = requireNonNull(flyteAdminClient);
    this.stateManager = requireNonNull(stateManager);
  }

  @Override
  public String createExecution(RunState runState, final String execName, final FlyteExecConf flyteExecConf)
      throws CreateExecutionException {
    requireNonNull(runState, "runState");
    requireNonNull(execName, "name");
    requireNonNull(flyteExecConf, "flyteExecConf");
    final var launchPlanIdentifier = flyteExecConf.referenceId();
    final var execMode = runState.data().trigger()
        .map(this::toFlyteExecutionMode)
        .orElse(ExecutionMode.UNRECOGNIZED);

    // TODO: verify if the execution already exist
    try {
      flyteAdminClient.createExecution(
          launchPlanIdentifier.project(),
          launchPlanIdentifier.domain(),
          execName,
          IdentifierOuterClass.Identifier.newBuilder()
              .setName(launchPlanIdentifier.name())
              .setProject(launchPlanIdentifier.project())
              .setDomain(launchPlanIdentifier.domain())
              .setResourceType(IdentifierOuterClass.ResourceType.LAUNCH_PLAN)
              .setVersion(launchPlanIdentifier.version())
              .build(),
          execMode);
      return runnerId;
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        throw new LaunchPlanNotFound(flyteExecConf, e);
      } // TODO: Consider other status codes that could be interesting
      throw new CreateExecutionException(flyteExecConf, e);
    } catch (Exception e) {
      throw new CreateExecutionException(flyteExecConf, e);
    }
  }

  @Override
  public void poll(FlyteExecutionId flyteExecutionId, RunState runState)
      throws PollingException {
    requireNonNull(flyteExecutionId, "flyteExecutionId");
    requireNonNull(runState, "runState");
    try {
      final ExecutionOuterClass.Execution execution =
          flyteAdminClient.getExecution(flyteExecutionId.project(),
              flyteExecutionId.domain(), flyteExecutionId.name());
      emitFlyteEvents(execution, runState);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        throw new ExecutionNotFoundException(flyteExecutionId, e);
      }
      throw new PollingException(flyteExecutionId, e);
    } catch (Exception e) {
      throw new PollingException(flyteExecutionId, e);
    }

  }

  @VisibleForTesting
  void emitFlyteEvents(ExecutionOuterClass.Execution execution, RunState runState)
      throws IsClosedException {
    final Execution.WorkflowExecution.Phase phase = execution.getClosure().getPhase();
    final FlytePhase flytePhase = FlytePhase.fromProto(phase);
    switch (flytePhase) {
      case UNDEFINED:
        LOG.info("Keep polling with FlytePhase UNDEFINED for: " + runState.workflowInstance());
        break;
      case SUCCEEDED:
        LOG.info("Issue 'terminate' event for: " + runState.workflowInstance());
        stateManager.receive(Event.terminate(runState.workflowInstance(), Optional.of(SUCCESS_EXIT_CODE)));
        break;
      case FAILED:
      case ABORTED:
      case TIMED_OUT:
        final String flyteCode = execution.getClosure().getError().getCode();
        final int styxCode = flyteErrorCodeToStyx(flyteCode);
        LOG.info("Issue 'terminate' event for: " + runState.workflowInstance());
        stateManager.receive(Event.terminate(runState.workflowInstance(), Optional.of(styxCode)));
        break;
    }
  }

  private int flyteErrorCodeToStyx(final String errorCode) {
      switch (errorCode) {
        case "USER:NotReady":
          return MISSING_DEPS_EXIT_CODE;
        case "USER:NotRetryable":
          return UNRECOVERABLE_FAILURE_EXIT_CODE;
        default:
          return UNKNOWN_ERROR_EXIT_CODE;
    }
  }

  private ExecutionMode toFlyteExecutionMode(Trigger trigger) {
    return trigger.accept(TriggerToExecutionModeVisitor.INSTANCE);
  }

  private static class TriggerToExecutionModeVisitor implements TriggerVisitor<ExecutionMode> {
    private static final TriggerToExecutionModeVisitor INSTANCE = new TriggerToExecutionModeVisitor();

    @Override
    public ExecutionMode natural() {
      return ExecutionMode.SCHEDULED;
    }

    @Override
    public ExecutionMode adhoc(String triggerId) {
      return ExecutionMode.MANUAL;
    }

    @Override
    public ExecutionMode backfill(String triggerId) {
      // Backfills in Styx doesn't provide idempotency guaranties so ExecutionMode.RELAUNCH doesn't apply
      return ExecutionMode.MANUAL;
    }

    @Override
    public ExecutionMode unknown(String triggerId) {
      return ExecutionMode.UNRECOGNIZED;
    }
  }
}
