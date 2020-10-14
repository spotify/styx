/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2020 Spotify AB
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

import org.junit.Test;

public class LabelValueTest {

  @Test
  public void shouldHandleEmptyString() {
    var input = "";
    var output = LabelValue.normalize(input);
    var expected = "e3b0c44";
    assertThat(expected, is(output));
  }

  @Test
  public void shouldNotModifySimpleValues() {
    var input = "test-value_including-0-and-9";
    var output = LabelValue.normalize(input);
    assertThat(input, is(output));
  }

  @Test
  public void shouldNotModifySimple63ByteValue() {
    var input = repeat("0", LabelValue.GCP_LABEL_MAX_LENGTH);
    var output = LabelValue.normalize(input);
    assertThat(input, is(output));
  }

  @Test
  public void shouldSuffixChangedShortValue() {
    var input = "spotify.net";
    var output = LabelValue.normalize(input);
    var expected = "spotifynet17df707";
    assertThat(expected, is(output));
  }

  @Test
  public void shouldTruncateAndSuffixLongValue() {
    var input = repeat("0", LabelValue.GCP_LABEL_MAX_LENGTH + 1);
    var output = LabelValue.normalize(input);
    var expected = repeat("0", LabelValue.PREFIX_MAX_LENGTH) + "60e05bd";
    assertThat(expected, is(output));
  }

  @Test
  public void shouldProperlyHandleMultiByteValue() {
    var input = "M\u0101rti\u0146\u0161";
    var output = LabelValue.normalize(input);
    var expected = "mrticd76e17";
    assertThat(expected, is(output));
  }

  @Test
  public void shouldProperlyHandleMultiByteLongValue() {
    var input = repeat("\u0410", LabelValue.GCP_LABEL_MAX_LENGTH + 1);
    var output = LabelValue.normalize(input);
    var expected = "fc135e7";
    assertThat(expected, is(output));
  }

  @Test
  public void shouldKeepAsMuchAsPossibleNormalCharactersEvenWithWeirdMultiCharSymbols() {
    var input = repeat("a🤡", LabelValue.GCP_LABEL_MAX_LENGTH + 1);
    var output = LabelValue.normalize(input);
    var expected = repeat("a", LabelValue.PREFIX_MAX_LENGTH) + "31b551a";
    assertThat(expected, is(output));
  }

  private static String repeat(String a, int count) {
    return a.repeat(Math.max(0, count));
  }
}
