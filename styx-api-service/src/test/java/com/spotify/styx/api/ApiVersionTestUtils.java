/*-
 * -\-\-
 * Spotify Styx API Service
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

package com.spotify.styx.api;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Testing utility for api tests covering many versions
 */
class ApiVersionTestUtils {

  static Api.Version[] ALL_VERSIONS = Api.Version.values();

  /**
   * A matcher that matches if the inspected {@link Api.Version} is at least as high as the given
   * lower bound.
   *
   * @param lowerBound  The lower bound to match against
   * @return A hamcrest matcher as described above
   */
  static Matcher<Api.Version> isAtLeast(Api.Version lowerBound) {
    return new TypeSafeMatcher<Api.Version>() {
      @Override
      protected boolean matchesSafely(Api.Version item) {
        return item.ordinal() >= lowerBound.ordinal();
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("Version is at least");
        description.appendValue(lowerBound);
      }
    };
  }
}
