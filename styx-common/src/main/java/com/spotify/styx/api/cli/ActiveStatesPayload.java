/*
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
package com.spotify.styx.api.cli;

import static com.spotify.styx.model.EventSerializer.PersistentEvent;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.spotify.styx.model.WorkflowInstance;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Value type for all current active states
 */
@AutoValue
public abstract class ActiveStatesPayload {

  @JsonProperty
  public abstract List<ActiveState> activeStates();

  @AutoValue
  public abstract static class ActiveState {

    public static final Comparator<ActiveState> PARAMETER_COMPARATOR =
        (a, b) -> a.workflowInstance().parameter().compareTo(b.workflowInstance().parameter());

    @JsonProperty
    public abstract WorkflowInstance workflowInstance();

    @JsonProperty
    public abstract String state();

    @JsonProperty
    public abstract String lastExecutionId();

    @JsonProperty
    public abstract Optional<PersistentEvent> previousExecutionLastEvent();

    @JsonCreator
    public static ActiveState create(
        @JsonProperty("workflow_instance") WorkflowInstance workflowInstance,
        @JsonProperty("state") String state,
        @JsonProperty("last_execution_id") String lastExecutionId,
        @JsonProperty("previous_execution_last_event") Optional<PersistentEvent> previousExecutionLastEvent) {
      return new AutoValue_ActiveStatesPayload_ActiveState(
          workflowInstance,
          state,
          lastExecutionId,
          previousExecutionLastEvent);
    }
  }

  @JsonCreator
  public static ActiveStatesPayload create(
      @JsonProperty("active_states") List<ActiveState> activeStates) {
    return new AutoValue_ActiveStatesPayload(activeStates);
  }
}
