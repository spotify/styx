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

package com.spotify.styx.model.data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.spotify.styx.model.TriggerParameters;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Value representing a {@link com.spotify.styx.model.WorkflowInstance} trigger
 */
@AutoValue
public abstract class Trigger {

  @JsonProperty
  public abstract String triggerId();

  @JsonProperty
  public abstract Instant timestamp();

  @JsonProperty
  public abstract TriggerParameters parameters();

  @JsonProperty
  public abstract boolean complete();

  @JsonProperty
  public abstract List<Execution> executions();

  @JsonCreator
  public static Trigger create(
      @JsonProperty("trigger_id") String triggerId,
      @JsonProperty("timestamp") Instant timestamp,
      @JsonProperty("parameters") @Nullable TriggerParameters parameters,
      @JsonProperty("complete") boolean complete,
      @JsonProperty("executions") List<Execution> executions) {
    return new AutoValue_Trigger(
        triggerId,
        timestamp,
        Optional.ofNullable(parameters).orElse(TriggerParameters.zero()),
        complete,
        executions);
  }
}
