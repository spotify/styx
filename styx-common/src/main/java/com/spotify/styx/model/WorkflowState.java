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

package com.spotify.styx.model;

import static java.util.Optional.of;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import java.util.Optional;

/**
 * A data object containing the state of a {@link com.spotify.styx.model.Workflow}
 */
@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class WorkflowState {

  @JsonProperty
  public abstract Optional<Boolean> enabled();

  @JsonProperty
  public abstract Optional<String> dockerImage();

  @JsonProperty
  public abstract Optional<String> commitSha();

  @JsonCreator
  public static WorkflowState create(
      @JsonProperty("enabled") Optional<Boolean> enabled,
      @JsonProperty("docker_image") Optional<String> dockerImage,
      @JsonProperty("commit_sha") Optional<String> commitSha) {
    return new AutoValue_WorkflowState(enabled, dockerImage, commitSha);
  }

  public static WorkflowState all(boolean enabled, String dockerImage, String commitSha) {
    return create(of(enabled), of(dockerImage), of(commitSha));
  }

  public static WorkflowState empty() {
    return create(Optional.empty(), Optional.empty(), Optional.empty());
  }

  public static WorkflowState patchDockerImage(String dockerImage) {
    return create(Optional.empty(), of(dockerImage), Optional.empty());
  }

  public static WorkflowState patchEnabled(boolean enabled) {
    return create(of(enabled), Optional.empty(), Optional.empty());
  }
}
