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

package com.spotify.styx.docker;

import com.google.common.base.Throwables;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * A utility class for validating that a {@link Workflow} has the needed docker configuration for
 * Styx to pick it up.
 */
public final class WorkflowValidator {

  private WorkflowValidator() {
  }

  public static boolean hasDockerConfiguration(Workflow workflow) {
    return workflow.configuration().dockerArgs().isPresent();
  }

  public static boolean hasDockerConfiguration(Workflow workflow, Storage storage) {
    final Optional<List<String>> dockerArgs = workflow.configuration().dockerArgs();
    if (!dockerArgs.isPresent()) {
      return false;
    }

    try {
      return storage.getDockerImage(workflow.id()).isPresent();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
