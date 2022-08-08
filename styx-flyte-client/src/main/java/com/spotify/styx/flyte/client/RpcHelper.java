/*-
 * -\-\-
 * Spotify Styx Flyte Client
 * --
 * Copyright (C) 2016 - 2022 Spotify AB
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

package com.spotify.styx.flyte.client;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class RpcHelper {

  private RpcHelper() {}

  // Test this filter by running it towards FlyteAdmin
  public static String getExecutionsListFilter(Instant timeNow, Duration since, Duration to) {

    final String dateSince = timeNow.minus(since)
        .atZone(ZoneId.of("UTC"))
        .toLocalDateTime()
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));

    final String dateTo = timeNow.minus(to)
        .atZone(ZoneId.of("UTC"))
        .toLocalDateTime()
        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"));

    return String.format("value_in(phase,RUNNING)+gte(execution-created-at,%s)+lte(execution-created-at,%s)", dateSince,
        dateTo);
  }
}
