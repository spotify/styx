/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.spotify.styx.api.Api;
import java.net.URI;
import java.util.Optional;

@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class Workflow {

  @JsonProperty
  public abstract String componentId();

  @JsonProperty
  public abstract String workflowId();

  // component URI is a legacy concept and discouraged for new Workflows
  @JsonProperty
  public abstract Optional<URI> componentUri();

  // this gives a hint that the workflow registration/modification came from
  // a certain version of Styx API
  @JsonProperty("__from_api")
  public abstract Optional<Api.Version> fromApi();

  @JsonProperty
  public abstract WorkflowConfiguration configuration();

  public WorkflowId id() {
    return WorkflowId.ofWorkflow(this);
  }

  @JsonCreator
  public static Workflow create(
      @JsonProperty("component_id") String componentId,
      @JsonProperty("component_uri") Optional<URI> componentUri,
      @JsonProperty("__from_api") Optional<Api.Version> fromApi,
      @JsonProperty("configuration") WorkflowConfiguration configuration) {
    return new AutoValue_Workflow(componentId,
                                  configuration.id(),
                                  componentUri,
                                  fromApi,
                                  configuration);
  }

  // wrapper for use of legacy factory function
  public static Workflow create(
      String componentId,
      URI componentUri,
      WorkflowConfiguration configuration) {
    return new AutoValue_Workflow(componentId,
                                  configuration.id(),
                                  Optional.of(componentUri),
                                  Optional.empty(),
                                  configuration);
  }
}
