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

package com.spotify.styx.api.deprecated;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.spotify.styx.model.deprecated.WorkflowInstance;
import com.spotify.styx.state.StateData;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Value type for all current active states
 */
@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
@Deprecated
public abstract class RunStateDataPayload {

  @JsonProperty
  //todo change the name of this variable, remove 'active'
  public abstract List<RunStateData> activeStates();

  @AutoValue
  @JsonIgnoreProperties(ignoreUnknown = true)
  @Deprecated
  public abstract static class RunStateData {

    public static final Comparator<RunStateData> PARAMETER_COMPARATOR =
        Comparator.comparing(a -> a.workflowInstance().parameter());

    @JsonProperty
    public abstract WorkflowInstance workflowInstance();

    @JsonProperty
    public abstract String state();

    @JsonProperty
    public abstract StateData stateData();

    @JsonCreator
    public static RunStateData create(
        @JsonProperty("workflow_instance") WorkflowInstance workflowInstance,
        @JsonProperty("state") String state,
        @JsonProperty("state_data") StateData stateData) {
      return new AutoValue_RunStateDataPayload_RunStateData(workflowInstance, state, stateData);
    }

    public static RunStateData create(
        com.spotify.styx.api.RunStateDataPayload.RunStateData runStateData) {
      return new AutoValue_RunStateDataPayload_RunStateData(
          WorkflowInstance.create(runStateData.workflowInstance()), runStateData.state(),
          runStateData.stateData());
    }
  }

  @JsonCreator
  public static RunStateDataPayload create(
      @JsonProperty("active_states") List<RunStateData> runStateDataList) {
    return new AutoValue_RunStateDataPayload(runStateDataList);
  }

  public static RunStateDataPayload create(
      com.spotify.styx.api.RunStateDataPayload runStateDataPayload) {
    return new AutoValue_RunStateDataPayload(runStateDataPayload.activeStates().stream().map(
        RunStateData::create).collect(Collectors.toList()));
  }
}
