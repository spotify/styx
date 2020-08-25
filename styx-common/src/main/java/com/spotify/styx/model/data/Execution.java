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
import com.spotify.styx.model.FlyteExecConf;
import java.util.List;
import java.util.Optional;

/**
 * Value representing an execution attempt
 */
@AutoValue
public abstract class Execution {

  @JsonProperty
  public abstract Optional<String> executionId();

  @JsonProperty
  public abstract Optional<String> dockerImage();

  @JsonProperty
  public abstract Optional<String> commitSha();

  @JsonProperty
  public abstract Optional<FlyteExecConf> flyteExecConf();

  @JsonProperty
  public abstract Optional<String> runnerId();

  @JsonProperty
  public abstract List<ExecStatus> statuses();

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @JsonCreator
  public static Execution create(
      @JsonProperty("execution_id") Optional<String> executionId,
      @JsonProperty("docker_image") Optional<String> dockerImage,
      @JsonProperty("commit_sha") Optional<String> commitSha,
      @JsonProperty("flyte_exec_conf") Optional<FlyteExecConf> flyteExecConf,
      @JsonProperty("runner_id") Optional<String> runnerId,
      @JsonProperty("statuses") List<ExecStatus> statuses) {
    if (dockerImage.isPresent() && flyteExecConf.isPresent()) {
      throw new IllegalArgumentException(
          "Conflicting configuration: Both docker image and flyte conf specified for exec id: "
          + executionId.orElse("unknown")
      );
    }
    return new AutoValue_Execution(executionId, dockerImage, commitSha, flyteExecConf, runnerId, statuses);
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public static Execution create(
      Optional<String> executionId,
      Optional<String> dockerImage,
      Optional<String> commitSha,
      Optional<String> runnerId,
      List<ExecStatus> statuses) {
    return create(executionId, dockerImage, commitSha, Optional.empty(), runnerId, statuses);
  }


  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  public static Execution create(
      Optional<String> executionId,
      Optional<FlyteExecConf> flyteExecConf,
      Optional<String> runnerId,
      List<ExecStatus> statuses) {
    return create(executionId, Optional.empty(), Optional.empty(), flyteExecConf, runnerId, statuses);
  }
}
