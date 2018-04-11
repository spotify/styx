/*-
 * -\-\-
 * Spotify Styx Standalone Service
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

import com.google.common.annotations.VisibleForTesting;
import com.spotify.apollo.AppInit;
import com.spotify.apollo.Environment;
import com.spotify.apollo.httpservice.HttpService;
import com.spotify.apollo.httpservice.LoadingException;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.styx.monitoring.MetricsStats;
import com.spotify.styx.monitoring.Stats;
import java.time.Instant;

public class StyxService {

  private static Stats STATS;

  private StyxService() {
    throw new UnsupportedOperationException();
  }

  public static void main(String[] args) throws LoadingException {
    final StyxScheduler scheduler = StyxScheduler.newBuilder()
        .setStatsFactory(StyxService::stats)
        .build();
    final StyxApi api = StyxApi.newBuilder()
        .setStatsFactory(StyxService::stats)
        .build();

    final AppInit init = (env) -> {
      scheduler.create(env);
      api.create(env);
    };

    HttpService.boot(init, "styx-standalone", args);
  }

  @VisibleForTesting
  static synchronized Stats stats(Environment environment) {
    if (STATS == null) {
      STATS = new MetricsStats(environment.resolve(SemanticMetricRegistry.class), Instant::now);
    }
    return STATS;
  }
}
