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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import java.util.List;

/**
 * Value representing an execution attempt
 */
@AutoValue
public abstract class Execution {

  @JsonProperty
  public abstract String executionId();

  @JsonProperty
  public abstract String dockerImage();

  @JsonProperty
  public abstract List<ExecStatus> statuses();

  @JsonCreator
  public static Execution create(
      @JsonProperty("execution_id") String executionId,
      @JsonProperty("docker_image") String dockerImage,
      @JsonProperty("statuses") List<ExecStatus> statuses) {
    return new AutoValue_Execution(executionId, dockerImage, statuses);
  }
}
