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

import com.spotify.apollo.AppInit;
import com.spotify.apollo.httpservice.HttpService;
import com.spotify.apollo.httpservice.LoadingException;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.styx.monitoring.MetricsStats;
import com.spotify.styx.monitoring.StatsFactory;
import java.time.Instant;

public class StyxService {

  private StyxService() {
    throw new UnsupportedOperationException();
  }

  public static void main(String[] args) throws LoadingException {
    final AppInit init = (env) -> {
      final MetricsStats stats =
          new MetricsStats(env.resolve(SemanticMetricRegistry.class), Instant::now);
      final StatsFactory statsFactory = (ignored) -> stats;

      final StyxScheduler scheduler = StyxScheduler.newBuilder()
          .setStatsFactory(statsFactory)
          .build();
      final StyxApi api = StyxApi.newBuilder()
          .setStatsFactory(statsFactory)
          .build();

      scheduler.create(env);
      api.create(env);
    };

    HttpService.boot(init, "styx-standalone", args);
  }
}
