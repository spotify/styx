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

package com.spotify.styx;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.time.Instant;
import java.util.function.Supplier;
import org.junit.Test;

public class CachedSupplierTest {

  Instant instant = Instant.parse("2016-10-17T15:00:00Z");
  int x = 42;
  int callCount = 0;

  Supplier<Integer> sut = new CachedSupplier<>(this::real, () -> instant, 10_000);

  int real() {
    callCount++;
    return x;
  }

  @Test
  public void testCachesCalls() throws Exception {
    int a = sut.get();
    x = 100;
    int b = sut.get();

    assertThat(callCount, is(1));
    assertThat(a, is(42));
    assertThat(b, is(42));
  }

  @Test
  public void testCacheTimesOut() throws Exception {
    int a = sut.get();
    x = 100;
    instant = Instant.parse("2016-10-17T15:00:11Z");
    int b = sut.get();

    assertThat(callCount, is(2));
    assertThat(a, is(42));
    assertThat(b, is(100));
  }

  @Test
  public void testCacheDoesNotTimeOutWithinTimeout() throws Exception {
    int a = sut.get();
    x = 100;
    instant = Instant.parse("2016-10-17T15:00:09Z");
    int b = sut.get();

    assertThat(callCount, is(1));
    assertThat(a, is(42));
    assertThat(b, is(42));
  }
}
