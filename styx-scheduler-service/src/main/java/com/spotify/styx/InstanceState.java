/*-
 * -\-\-
 * Spotify styx
 * --
 * Copyright (C) 2017 Spotify AB
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
package com.spotify.styx;

import com.google.auto.value.AutoValue;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;

@AutoValue
abstract class InstanceState {
  abstract WorkflowInstance workflowInstance();
  abstract RunState runState();

  static InstanceState create(WorkflowInstance workflowInstance, RunState runState) {
    return new AutoValue_InstanceState(workflowInstance, runState);
  }
}
