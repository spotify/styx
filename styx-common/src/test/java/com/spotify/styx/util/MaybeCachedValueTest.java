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

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class MaybeCachedValueTest {

  @Parameterized.Parameters(name = "{index}: create({0}, {1})")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        { false, 0 }, { true, 1 }, { false, 2 }, { true, 3 }
    });
  }

  private boolean cached;
  private int value;

  public MaybeCachedValueTest(boolean cached, int value) {
    this.cached = cached;
    this.value = value;
  }

  @Test
  public void createShouldAssignProperties() {
    MaybeCachedValue<Integer> maybeCacheValue = MaybeCachedValue.create(cached, value);

    assertThat(maybeCacheValue.isCached(), equalTo(cached));
    assertThat(maybeCacheValue.value(), equalTo(value));
  }
}