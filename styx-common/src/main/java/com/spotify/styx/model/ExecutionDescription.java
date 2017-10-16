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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.norberg.automatter.AutoMatter;
import java.util.List;
import java.util.Optional;

@AutoMatter
@JsonIgnoreProperties(ignoreUnknown = true)
public interface ExecutionDescription {

  @JsonProperty("docker_image")
  String dockerImage();

  @JsonProperty("docker_args")
  List<String> dockerArgs();

  @JsonProperty("docker_termination_logging")
  boolean dockerTerminationLogging();

  @JsonProperty("secret")
  Optional<WorkflowConfiguration.Secret> secret();

  @JsonProperty("service_account")
  Optional<String> serviceAccount();

  @JsonProperty("commit_sha")
  Optional<String> commitSha();



  static ExecutionDescriptionBuilder builder() {
    return new ExecutionDescriptionBuilder();
  }

  static ExecutionDescription forImage(String dockerImage) {
    return builder().dockerImage(dockerImage).build();
  }
}
