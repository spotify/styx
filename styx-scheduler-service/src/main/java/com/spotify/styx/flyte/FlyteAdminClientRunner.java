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
import com.spotify.styx.model.FlyteExecConf;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.core.IdentifierOuterClass;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

public class FlyteAdminClientRunner implements FlyteRunner {

  private final FlyteAdminClient flyteAdminClient;

  public FlyteAdminClientRunner(final FlyteAdminClient flyteAdminClient) {
    this.flyteAdminClient = flyteAdminClient;
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
}
