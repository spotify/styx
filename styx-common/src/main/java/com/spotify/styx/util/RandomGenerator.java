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

import java.time.Instant;
import java.util.Objects;
import java.util.Random;

public class RandomGenerator {

  public static final RandomGenerator DEFAULT = new RandomGenerator(Instant::now);

  private static final Random RANDOM = new Random();
  private static final int BOUND = 100000;

  public final Time time;

  RandomGenerator(Time time) {
    this.time = Objects.requireNonNull(time);
  }

  public String generateNumber() {
    var num = RANDOM.nextInt(BOUND);
    return String.format("%05d", num);
  }

  public String generateUniqueId(String prefix) {
    return String.format("%s-%013d-%s", prefix, time.get().toEpochMilli(), generateNumber());
  }
}
