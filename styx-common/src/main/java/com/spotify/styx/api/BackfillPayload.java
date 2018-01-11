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

package com.spotify.styx.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.spotify.styx.model.Backfill;
import java.util.Optional;

@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
public abstract class BackfillPayload {

  @JsonProperty
  public abstract Backfill backfill();

  @JsonProperty
  public abstract Optional<RunStateDataPayload> statuses();

  @JsonCreator
  public static BackfillPayload create(
      @JsonProperty("backfill") Backfill backfill,
      @JsonProperty("statuses") Optional<RunStateDataPayload> statuses) {
    return new AutoValue_BackfillPayload(backfill, statuses);
  }
}
