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

import static java.util.Collections.emptyList;
import static java.util.Optional.empty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import java.util.List;
import java.util.Optional;

@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class ExecutionDescription {

  @JsonProperty
  public abstract String dockerImage();

  @JsonProperty
  public abstract List<String> dockerArgs();

  @JsonProperty
  public abstract boolean dockerTerminationLogging();

  @JsonProperty
  public abstract Optional<WorkflowConfiguration.Secret> secret();

  @JsonProperty
  public abstract Optional<String> serviceAccount();

  @JsonProperty
  public abstract Optional<String> commitSha();

  @JsonCreator
  public static ExecutionDescription create(
      @JsonProperty("docker_image") String dockerImage,
      @JsonProperty("docker_args") List<String> dockerArgs,
      @JsonProperty("docker_termination_logging") boolean dockerTerminationLogging,
      @JsonProperty("secret") Optional<WorkflowConfiguration.Secret> secret,
      @JsonProperty("service_account") Optional<String> serviceAccount,
      @JsonProperty("commit_sha") Optional<String> commitSha) {
    return new AutoValue_ExecutionDescription(dockerImage, dockerArgs, dockerTerminationLogging,
                                              secret, serviceAccount, commitSha);
  }

  public static ExecutionDescription forImage(String dockerImage) {
    return new AutoValue_ExecutionDescription(dockerImage, emptyList(), false, empty(), empty(),
                                              empty());
  }
}
