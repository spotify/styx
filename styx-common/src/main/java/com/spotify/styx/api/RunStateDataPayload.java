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

package com.spotify.styx.api;

import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.StateData;
import io.norberg.automatter.AutoMatter;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Value type for all current active states
 */
@AutoMatter
public interface RunStateDataPayload {

  List<RunStateData> activeStates();

  @AutoMatter
  interface RunStateData {

    Comparator<RunStateData> PARAMETER_COMPARATOR =
        Comparator.comparing(a -> a.workflowInstance().parameter());

    Comparator<RunStateData> WORKFLOW_COMPARATOR =
        Comparator.comparing(a -> a.workflowInstance().toString());

    WorkflowInstance workflowInstance();

    String state();

    StateData stateData();

    Optional<Long> initialTimestamp();

    Optional<Long> latestTimestamp();

    RunStateDataBuilder builder();

    static RunStateDataBuilder newBuilder() {
      return new RunStateDataBuilder();
    }

    static RunStateData create(
        WorkflowInstance workflowInstance,
        String state,
        StateData stateData) {
      return newBuilder()
          .workflowInstance(workflowInstance)
          .state(state)
          .stateData(stateData)
          .build();
    }
  }

  static RunStateDataPayload create(
      List<RunStateData> runStateDataList) {
    return new RunStateDataPayloadBuilder().activeStates(runStateDataList).build();
  }
}
