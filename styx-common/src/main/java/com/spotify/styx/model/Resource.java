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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;

/**
 * A data object containing the state of a {@link Workflow}
 */
@AutoValue
public abstract class Resource {

  @JsonProperty
  public abstract String id();

  @JsonProperty
  public abstract long concurrency();

  @JsonProperty
  public abstract RequestsResource requests();

  @JsonProperty
  public abstract LimitsResource limits();

  @JsonCreator
  public static Resource create(
      @JsonProperty("id") String id,
      @JsonProperty("concurrency") long concurrency,
      @JsonProperty("requests") RequestsResource requests,
      @JsonProperty("limits") LimitsResource limits) {
    return new AutoValue_Resource(id, concurrency, requests, limits);
  }
}
