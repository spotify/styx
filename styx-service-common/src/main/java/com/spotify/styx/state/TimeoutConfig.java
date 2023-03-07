/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx.state;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.spotify.styx.util.ConfigUtil.get;

/**
 * Configuration object for mappings from {@link RunState.State} to TTL values.
 */
public class TimeoutConfig {

  private static final String DEFAULT_TTL_KEY = "default";
  private static final String STYX_RUNNING_STATE_MAX_TTL_CONFIG = "styx.max-running-timeout";
  private static final String STYX_STALE_STATE_TTL_CONFIG = "styx.stale-state-ttls";

  private final Map<RunState.State, Duration> ttls;
  private final Duration defaultTtl;
  private final Duration maxRunningTimeout;

  private TimeoutConfig(Map<RunState.State, Duration> ttls, Duration defaultTtl, Optional<Duration> maxRunningTimeout) {
    this.ttls = Objects.requireNonNull(ttls);
    this.defaultTtl = Objects.requireNonNull(defaultTtl);
    this.maxRunningTimeout = maxRunningTimeout.orElseGet(() -> ttlOf(RunState.State.RUNNING));
  }

  public Duration getMaxRunningTimeout() {
    return maxRunningTimeout;
  }

  public static TimeoutConfig createFromConfig(Config config) {
    final Config ttlSubConfig = config.getConfig(STYX_STALE_STATE_TTL_CONFIG);

    final Duration defaultTtl = Duration.parse(ttlSubConfig.getString(DEFAULT_TTL_KEY));

    final ImmutableMap.Builder<RunState.State, Duration> map = ImmutableMap.builder();
    for (RunState.State state : RunState.State.values()) {
      final String key = state.name().toLowerCase();
      if (ttlSubConfig.hasPath(key)) {
        final Duration ttl = Duration.parse(ttlSubConfig.getString(key));
        map.put(state, ttl);
      }
    }

    Optional<Duration> maxRunningTimeout = get(config, config::getString, STYX_RUNNING_STATE_MAX_TTL_CONFIG).map(Duration::parse);

    return new TimeoutConfig(map.build(), defaultTtl, maxRunningTimeout);
  }

  @VisibleForTesting
  public static TimeoutConfig createWithDefaultTtl(Duration defaultTtl) {
    return new TimeoutConfig(Map.of(), defaultTtl, Optional.of(defaultTtl));
  }

  public Duration ttlOf(RunState.State state) {
    return ttls.getOrDefault(state, defaultTtl);
  }
}
