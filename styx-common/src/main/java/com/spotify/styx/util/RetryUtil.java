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

package com.spotify.styx.util;

import java.time.Duration;
import java.util.Objects;
import java.util.Random;

/**
 * Utility for calculating exponential backoff
 */
public class RetryUtil {

  private static final Random RANDOM = new Random();

  private final Duration baseDelay;
  private final int maxExponent;

  public RetryUtil(Duration baseDelay, int maxExponent) {
    this.baseDelay = Objects.requireNonNull(baseDelay);
    this.maxExponent = Objects.requireNonNull(maxExponent);
  }

  public Duration calculateDelay(int tries) {
    final int cappedTries = (tries < maxExponent) ? tries : maxExponent;
    final int multiplier = Math.max(1, RANDOM.nextInt(1 << cappedTries));

    return baseDelay.multipliedBy(multiplier);
  }
}
