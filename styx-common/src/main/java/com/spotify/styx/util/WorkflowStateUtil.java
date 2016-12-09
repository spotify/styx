/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.util;

import com.spotify.styx.model.WorkflowState;
import java.util.Optional;

public final class WorkflowStateUtil {

  private WorkflowStateUtil() { }

  public static WorkflowState patchWorkflowState(
      final Optional<WorkflowState> originalWorkflowState,
      final WorkflowState patch) {

    return originalWorkflowState.map(
        o -> {
          WorkflowState.Builder builder = o.toBuilder()
              .enabled(patch.enabled().orElse(o.enabled().orElse(false)));
          patch.dockerImage().ifPresent(dockerImage -> builder.dockerImage(dockerImage));
          patch.commitSha().ifPresent(commitSha -> builder.commitSha(commitSha));
          patch.nextNaturalTrigger()
              .ifPresent(nextNaturalTrigger -> builder.nextNaturalTrigger(nextNaturalTrigger));
          return builder.build();
        }
    ).orElse(patch);
  }
}
