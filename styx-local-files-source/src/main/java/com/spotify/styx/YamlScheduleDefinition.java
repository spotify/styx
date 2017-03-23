/*-
 * -\-\-
 * Spotify Styx Local Files Schedule Source
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

package com.spotify.styx;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.spotify.styx.model.WorkflowConfiguration;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;

@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
abstract class YamlScheduleDefinition {

  @JsonProperty
  public abstract List<WorkflowConfiguration> schedules();

  @JsonCreator
  public static YamlScheduleDefinition create(
      @JsonProperty("schedules") @Nullable List<WorkflowConfiguration> workflowConfigurations) {
    if (workflowConfigurations == null) {
      workflowConfigurations = Collections.emptyList();
    }
    return new AutoValue_YamlScheduleDefinition(workflowConfigurations);
  }
}
