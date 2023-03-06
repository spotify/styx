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

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import java.time.Duration;
import java.util.Map;
import java.util.Objects;

/**
 * Configuration object for mappings from {@link RunState.State} to TTL values.
 */
public class TimeoutConfig {

  private static final String DEFAULT_TTL_KEY = "default";
  private static final String MAX_RUNNING_TIMEOUT_KEY = "running_max";

  private final Map<RunState.State, Duration> ttls;
  private final Duration defaultTtl;

  private TimeoutConfig(Map<RunState.State, Duration> ttls, Duration defaultTtl) {
    this.ttls = Objects.requireNonNull(ttls);
    this.defaultTtl = Objects.requireNonNull(defaultTtl);
  }

  public static TimeoutConfig createFromConfig(Config ttlSubConfig) {
    final Duration defaultTtl = Duration.parse(ttlSubConfig.getString(DEFAULT_TTL_KEY));

    final ImmutableMap.Builder<RunState.State, Duration> map = ImmutableMap.builder();
    for (RunState.State state : RunState.State.values()) {
      final String keyForDefaultValue = state.name().toLowerCase();
      if (ttlSubConfig.hasPath(keyForDefaultValue)) {
        final Duration ttl = Duration.parse(ttlSubConfig.getString(keyForDefaultValue));
        map.put(state, ttl);
      }
    }

    return new TimeoutConfig(map.build(), defaultTtl);
  }

  public static TimeoutConfig createWithDefaultTtl(Duration defaultTtl) {
    return new TimeoutConfig(Map.of(), defaultTtl);
  }

  public Duration ttlOf(RunState.State state) {
    return ttls.getOrDefault(state, defaultTtl);
  }
}
