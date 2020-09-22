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

import static com.spotify.styx.flyte.FlyteEventTranslator.translate;
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
import flyteidl.core.IdentifierOuterClass;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlyteAdminClientRunner implements FlyteRunner {

  private static final Logger LOG = LoggerFactory.getLogger(FlyteAdminClientRunner.class);
  @VisibleForTesting static final String TERMINATE_CAUSE_PREFIX = "Styx workflow instance execution reached state: ";

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
  public String createExecution(final RunState runState, final String execName, final FlyteExecConf flyteExecConf,
                                final Map<String, String> annotations)
      throws CreateExecutionException {
    requireNonNull(runState, "runState");
    requireNonNull(execName, "name");
    requireNonNull(flyteExecConf, "flyteExecConf");
    requireNonNull(annotations, "annotations");
    final var launchPlanIdentifier = flyteExecConf.referenceId();
    final var execMode = runState.data().trigger()
        .map(this::toFlyteExecutionMode)
        .filter(mode -> mode != ExecutionMode.UNRECOGNIZED)
        .orElseThrow(() -> new CreateExecutionException("Missing trigger or unknown in StateData: " + runState.data()));

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
          execMode,
          annotations);
      return runnerId;
    } catch (StatusRuntimeException e) {
      switch (e.getStatus().getCode()) {
        case ALREADY_EXISTS:
          // TODO: 🤔 How do we make sure that we tell apart between low probabilities of collisions
          //  on hashing of styx-run-id --> flyte exec and repetitions of same styx-id execution.
          //  For now we always assume the later.
          return runnerId;
        case NOT_FOUND:
          throw new LaunchPlanNotFound(flyteExecConf, e);
      }
      throw new CreateExecutionException(flyteExecConf, e);
    } catch (Exception e) {
      throw new CreateExecutionException(flyteExecConf, e);
    }
  }

  @Override
  public void terminateExecution(RunState runState, FlyteExecutionId flyteExecutionId) {
    requireNonNull(runState, "runState");
    requireNonNull(flyteExecutionId, "flyteExecutionId");
    try {
      // Flyte admin tolerates terminate request over an already terminated workflow execution
      // so no need to check that workflow execution hasn't been terminated yet
      flyteAdminClient.terminateExecution(
          flyteExecutionId.project(),
          flyteExecutionId.domain(),
          flyteExecutionId.name(),
          getCause(runState));
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        LOG.warn("Trying to terminate non existent flyte execution: {}", flyteExecutionId);
        return;
      } // TODO: Consider other status codes that could be interesting
      LOG.error("Couldn't terminate flyte execution: {}", flyteExecutionId, e);
    } catch (Exception e) {
      LOG.error("Couldn't terminate flyte execution: {}", flyteExecutionId, e);
    }
  }

  private String getCause(RunState runState) {
    return TERMINATE_CAUSE_PREFIX + runState.state();
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
    final List<Event> events = translate(execution, runState);
    for (Event event : events) {
      stateManager.receive(event);
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
