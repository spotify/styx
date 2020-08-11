/*-
 * -\-\-
 * Spotify Styx Flyte Client
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

package com.spotify.styx.flyte.client;

import flyteidl.admin.ExecutionOuterClass;

public enum ExecutionMode {
  MANUAL,
  SCHEDULED,
  SYSTEM,
  RELAUNCH,
  CHILD_WORKFLOW;

  ExecutionOuterClass.ExecutionMetadata.ExecutionMode toProto() {
    switch (this) {
      case MANUAL:
        return ExecutionOuterClass.ExecutionMetadata.ExecutionMode.MANUAL;
      case SCHEDULED:
        return ExecutionOuterClass.ExecutionMetadata.ExecutionMode.SCHEDULED;
      case SYSTEM:
        return ExecutionOuterClass.ExecutionMetadata.ExecutionMode.SYSTEM;
      case RELAUNCH:
        return ExecutionOuterClass.ExecutionMetadata.ExecutionMode.RELAUNCH;
      case CHILD_WORKFLOW:
        return ExecutionOuterClass.ExecutionMetadata.ExecutionMode.CHILD_WORKFLOW;
      default:
        return ExecutionOuterClass.ExecutionMetadata.ExecutionMode.UNRECOGNIZED;
    }
  }
}
