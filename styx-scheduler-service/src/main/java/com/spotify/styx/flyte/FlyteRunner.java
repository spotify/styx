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

public class FlyteRunner {

  private FlyteAdminClient flyteAdminClient;

  public FlyteRunner(final FlyteAdminClient flyteAdminClient) {
    this.flyteAdminClient = flyteAdminClient;
  }

  public FlyteExecution createExecution(final FlyteExecConf flyteExecConf) {
    final var flyteIdentifier = flyteExecConf.referenceId();
    final ExecutionOuterClass.ExecutionCreateResponse response =
        flyteAdminClient.createExecution(
            flyteIdentifier.project(),
            flyteIdentifier.domain(),
            IdentifierOuterClass.Identifier.newBuilder()
                .setName(flyteIdentifier.name())
                .setProject(flyteIdentifier.project())
                .setDomain(flyteIdentifier.domain())
                .setResourceType(IdentifierOuterClass.ResourceType.LAUNCH_PLAN)
                .setVersion(flyteIdentifier.version())
                .build(),
            ExecutionOuterClass.ExecutionMetadata.ExecutionMode.SCHEDULED);
    return FlyteExecution.fromProto(response);
  }
}
