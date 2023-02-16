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

import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasJsonPath;
import static com.jayway.jsonpath.matchers.JsonPathMatchers.hasNoJsonPath;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasPayload;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.spotify.apollo.Response;
import okio.ByteString;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

class JsonMatchers {

  private JsonMatchers() {
    throw new UnsupportedOperationException();
  }

  static <T> void assertJson(Response<ByteString> response, String jsonPath,
                             Matcher<T> matcher) {
    assertThat(response, hasPayload(asByteString(hasJsonPath(jsonPath, matcher))));
  }

  static void assertNoJson(Response<ByteString> response, String jsonPath) {
    assertThat(response, hasPayload(asByteString(hasNoJsonPath(jsonPath))));
  }

  static void assertJsonNullValue(Response<ByteString> response, String jsonPath) {
    assertThat(response, hasPayload(asByteString(hasJsonPath(jsonPath, nullValue()))));
  }

  private static Matcher<ByteString> asByteString(Matcher<? super String> strMatcher) {
    return new TypeSafeMatcher<>() {
      @Override
      protected boolean matchesSafely(ByteString byteString) {
        return strMatcher.matches(byteString.utf8());
      }

      @Override
      public void describeTo(Description description) {
        strMatcher.describeTo(description);
      }
    };
  }
}
