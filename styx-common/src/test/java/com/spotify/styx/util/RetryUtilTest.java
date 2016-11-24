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

import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class RetryUtilTest {

  private static final Duration BASE_DELAY = Duration.ofMillis(500);
  private static final int MAX_EXPONENT = 6;

  private RetryUtil retryUtil = new RetryUtil(BASE_DELAY, MAX_EXPONENT);

  @Test
  public void shouldCalculateExpectedAverage() throws Exception {
    List<Long> delays = new ArrayList<>();
    int runs = 10000;
    for (int i = 0; i < runs; i++) {
      delays.add(retryUtil.calculateDelay(MAX_EXPONENT).toMillis());
    }

    double average = delays.stream()
        .mapToLong(i -> i)
        .average()
        .getAsDouble();

    double expected = BASE_DELAY.toMillis() * (1 << (MAX_EXPONENT - 1));
    double diff = Math.abs(expected - average);

    assertThat(diff, lessThan(expected * 0.05));
  }
}
