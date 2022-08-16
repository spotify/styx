/*
 * -\-\-
 * Spotify Styx Scheduler Service
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

import io.norberg.automatter.AutoMatter;
import java.util.Optional;
import java.util.Set;

@AutoMatter
public interface StyxConfig {

  /**
   * Get the id of the current docker runner id
   */
  String globalDockerRunnerId();

  /**
   * Get the id of the current flyte runner id
   */
  String globalFlyteRunnerId();

  /**
   * Get the global enabled flag for Styx.
   */
  boolean globalEnabled();

  /**
   * Get the debug flag for Styx.
   */
  boolean debugEnabled();

  /**
   * Get the global concurrency for Styx.
   */
  Optional<Long> globalConcurrency();

  /**
   * Get the requests memory
   */
  Optional<String> requestsMemory();

  /**
   * Get the per-second submission rate limit for Styx.
   */
  Optional<Double> submissionRateLimit();

  /**
   * Get client blacklist.
   */
  Set<String> clientBlacklist();

  static StyxConfigBuilder newBuilder() {
    return new StyxConfigBuilder();
  }
}
