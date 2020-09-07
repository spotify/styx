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

import com.spotify.styx.flyte.client.FlyteAdminClient;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.util.IsClosedException;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.core.Execution;
import flyteidl.core.IdentifierOuterClass;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Objects;
import java.util.Optional;

public class FlyteAdminClientRunner implements FlyteRunner {

  private final FlyteAdminClient flyteAdminClient;
  private final StateManager stateManager;

  public FlyteAdminClientRunner(final FlyteAdminClient flyteAdminClient,
                                final StateManager stateManager) {
    this.flyteAdminClient = flyteAdminClient;
    this.stateManager = stateManager;
  }

  public FlyteExecution createExecution(final String name, final FlyteExecConf flyteExecConf)
      throws CreateExecutionException {
    final var flyteIdentifier = flyteExecConf.referenceId();
    try {
      final ExecutionOuterClass.ExecutionCreateResponse response = flyteAdminClient.createExecution(
          flyteIdentifier.project(),
          flyteIdentifier.domain(),
          name,
          IdentifierOuterClass.Identifier.newBuilder()
              .setName(flyteIdentifier.name())
              .setProject(flyteIdentifier.project())
              .setDomain(flyteIdentifier.domain())
              .setResourceType(IdentifierOuterClass.ResourceType.LAUNCH_PLAN)
              .setVersion(flyteIdentifier.version())
              .build(),
          // TODO: We should propagate Styx natural trigger and what not
          ExecutionOuterClass.ExecutionMetadata.ExecutionMode.SCHEDULED);
      return FlyteExecution.fromProto(response);
    } catch (StatusRuntimeException e) {
      if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
        throw new LaunchPlanNotFound(flyteExecConf, e);
      } // TODO: Consider other status codes that could be interesting
      throw new CreateExecutionException(flyteExecConf, e);
    } catch (Exception e) {
      throw new CreateExecutionException(flyteExecConf, e);
    }
  }

  public void poll(final String project, final String domain, final String name, RunState runState) {
    Objects.requireNonNull(project);
    Objects.requireNonNull(domain);
    Objects.requireNonNull(name);
    final ExecutionOuterClass.Execution execution =
        flyteAdminClient.getExecution(project, domain, name);
    emitFlyteEvents(execution, runState);
  }

  private void emitFlyteEvents(ExecutionOuterClass.Execution execution, RunState runState) {
    final Execution.WorkflowExecution.Phase phase = execution.getClosure().getPhase();
    final FlytePhase flytePhase = FlytePhase.fromProto(phase);
    try {
      switch (flytePhase) {
        case SUCCEEDED:
          stateManager.receive(Event.terminate(runState.workflowInstance(), Optional.of(0)));
          break;
        case FAILED:
        case ABORTED:
        case TIMED_OUT:
          final String flyteCode = execution.getClosure().getError().getCode();
          final int styxCode = flyteCodeToStyx(flyteCode);
          stateManager.receive(Event.terminate(runState.workflowInstance(), Optional.of(styxCode)));
          break;
      }
    } catch (IsClosedException e) {
      return;
    }
  }

  private int flyteCodeToStyx(final String flyteCode) {
      switch (flyteCode) {
        case "USER:NotReady":
          return 20;
        case "USER:NotRetryable":
          return 50;
        default:
          return 1;
    }
  }

}
