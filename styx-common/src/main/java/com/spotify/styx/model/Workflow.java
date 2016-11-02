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
package com.spotify.styx.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import java.net.URI;

@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class Workflow {

  @JsonProperty
  public abstract String componentId();

  @JsonProperty
  public abstract String endpointId();

  @JsonProperty
  public abstract URI componentUri();

  @JsonProperty
  public abstract DataEndpoint schedule();

  public WorkflowId id() {
    return WorkflowId.ofWorkflow(this);
  }

  @JsonCreator
  public static Workflow create(
      @JsonProperty("component_id") String componentId,
      @JsonProperty("component_uri") URI componentUri,
      @JsonProperty("schedule") DataEndpoint schedule) {
    return new AutoValue_Workflow(componentId, schedule.id(), componentUri, schedule);
  }
}
