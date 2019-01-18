/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class MaybeCachedValueTest {

  @Test
  public void createShouldAssignProperties() {
    testFor(true, 1);
    testFor(false, 2);
  }

  public void testFor(boolean expectedCache, int expectedValue) {
    MaybeCachedValue<Integer> maybeCacheValue = MaybeCachedValue.create(expectedCache, expectedValue);

    assertThat(maybeCacheValue.isCached(), equalTo(expectedCache));
    assertThat(maybeCacheValue.value(), equalTo(expectedValue));
  }
}