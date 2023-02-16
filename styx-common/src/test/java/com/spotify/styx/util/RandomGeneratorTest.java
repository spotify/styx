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

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertNotEquals;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Instant;
import org.junit.Test;

public class RandomGeneratorTest {

  private long time = 1_471_616_832_629L;
  private RandomGenerator randomGenerator =
      new RandomGenerator(() -> Instant.ofEpochMilli(time));

  @Test
  public void shouldGenerateRandomId() {
    String prefix = "test";
    String id = randomGenerator.generateUniqueId(prefix);
    assertThat(id, startsWith(prefix + "-" + time + "-"));
  }

  @Test
  public void shouldGenerateDifferentIds() {
    String prefix = "test";
    String id1 = randomGenerator.generateUniqueId(prefix);
    time = 1471616832630L;
    String id2 = randomGenerator.generateUniqueId(prefix);
    assertNotEquals(id1, id2);
  }
}
