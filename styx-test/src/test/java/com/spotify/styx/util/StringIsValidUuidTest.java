/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.UUID;
import org.junit.Test;

public class StringIsValidUuidTest {

  @Test
  public void matchesSafely() {
    assertThat(new StringIsValidUuid().matchesSafely(UUID.randomUUID().toString()), is(true));
    assertThat(new StringIsValidUuid().matchesSafely(UUID.randomUUID().toString().replace("-", "")), is(true));
    assertThat(new StringIsValidUuid().matchesSafely("foobar"), is(false));
    assertThat(new StringIsValidUuid().matchesSafely("foo-bar"), is(false));
    assertThat(new StringIsValidUuid().matchesSafely(""), is(false));
  }
}
