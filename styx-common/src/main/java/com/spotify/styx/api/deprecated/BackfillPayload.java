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

package com.spotify.styx.api.deprecated;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.spotify.styx.model.deprecated.Backfill;
import java.util.Optional;

@AutoValue
@JsonIgnoreProperties(ignoreUnknown = true)
@Deprecated
public abstract class BackfillPayload {

  @JsonProperty
  public abstract Backfill backfill();

  @JsonProperty
  public abstract Optional<RunStateDataPayload> statuses();

  public static BackfillPayload create(
      com.spotify.styx.api.BackfillPayload backfillPayload) {
    final com.spotify.styx.model.Backfill backfill = backfillPayload.backfill();
    final Optional<com.spotify.styx.api.RunStateDataPayload>
        runStateDataPayload = backfillPayload.statuses();

    final Backfill deprecatedBackfill = Backfill.create(backfill);
    final Optional<RunStateDataPayload> deprecatedRunStateDataPayload =
        runStateDataPayload.map(RunStateDataPayload::create);
    return new AutoValue_BackfillPayload(deprecatedBackfill, deprecatedRunStateDataPayload);
  }
}
