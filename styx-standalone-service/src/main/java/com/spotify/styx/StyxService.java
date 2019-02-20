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
import com.spotify.styx.api.AuthenticatorFactory;
import com.spotify.styx.monitoring.MetricsStats;
import com.spotify.styx.monitoring.StatsFactory;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceConfiguration;
import io.opencensus.exporter.trace.stackdriver.StackdriverTraceExporter;
import java.io.IOException;
import java.time.Instant;
import javaslang.Function1;

public class StyxService {

  private static final String SERVICE_NAME = "styx-standalone";

  private StyxService() {
    throw new UnsupportedOperationException();
  }

  public static void main(String[] args) throws LoadingException, IOException {
    StackdriverTraceExporter.createAndRegister(
        StackdriverTraceConfiguration.builder().build());

    final AppInit init = (env) -> {
      final MetricsStats stats =
          new MetricsStats(env.resolve(SemanticMetricRegistry.class), Instant::now);
      final StatsFactory statsFactory = (ignored) -> stats;

      final AuthenticatorFactory authenticatorFactory =
          Function1.of(AuthenticatorFactory.DEFAULT::apply).memoized()::apply;

      final StyxScheduler scheduler = StyxScheduler.newBuilder()
          .setServiceName(SERVICE_NAME)
          .setStatsFactory(statsFactory)
          .setAuthenticatorFactory(authenticatorFactory)
          .build();
      final StyxApi api = StyxApi.newBuilder()
          .setServiceName(SERVICE_NAME)
          .setStatsFactory(statsFactory)
          .setAuthenticatorFactory(authenticatorFactory)
          .build();

      scheduler.create(env);
      api.create(env);
    };

    HttpService.boot(init, SERVICE_NAME, args);
  }
}
