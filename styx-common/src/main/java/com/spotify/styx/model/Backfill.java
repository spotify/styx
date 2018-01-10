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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.norberg.automatter.AutoMatter;
import java.time.Instant;
import java.util.Optional;

@AutoMatter
@JsonIgnoreProperties(ignoreUnknown = true)
public interface Backfill {

  @JsonProperty
  String id();

  @JsonProperty
  Instant start();

  @JsonProperty
  Instant end();

  @JsonProperty
  WorkflowId workflowId();

  @JsonProperty
  int concurrency();

  @JsonProperty
  Instant nextTrigger();

  @JsonProperty
  Schedule schedule();

  @JsonProperty
  boolean allTriggered();

  @JsonProperty
  boolean halted();

  @JsonProperty
  Optional<String> description();

  BackfillBuilder builder();

  static BackfillBuilder newBuilder() {
    return new BackfillBuilder();
  }
}
