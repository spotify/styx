/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

package com.spotify.styx.model;

import io.norberg.automatter.AutoMatter;
import java.util.Optional;

@AutoMatter
public interface TriggerRequest {

  WorkflowId workflowId();

  String parameter();

  Optional<TriggerParameters> triggerParameters();

  static TriggerRequestBuilder builder() {
    return new TriggerRequestBuilder();
  }

  static TriggerRequest of(WorkflowId workflowId, String parameter) {
    return builder()
        .workflowId(workflowId)
        .parameter(parameter)
        .build();
  }

  static TriggerRequest of(WorkflowId workflowId, String parameter,
                           TriggerParameters triggerParameters) {
    return builder()
        .workflowId(workflowId)
        .parameter(parameter)
        .triggerParameters(triggerParameters)
        .build();
  }
}
