/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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
package com.spotify.styx.docker;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.spotify.styx.util.ClassEnforcer;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class LabelValueTest {

  private static final int KUBERNETES_LABEL_MAX_LENGTH = 63;
  private static final int DIGEST_SUFFIX_LENGTH = 7;
  private static final int PREFIX_MAX_LENGTH = KUBERNETES_LABEL_MAX_LENGTH - DIGEST_SUFFIX_LENGTH;

  @Test
  @Parameters({
      "",
      "a",
      "test-value_including-0-and-9",
  })
  public void shouldHandleSimpleCases(String input) {
    var output = LabelValue.normalize(input);
    assertThat(output, is(input));
  }

  @Test
  public void shouldNotModifySimple63ByteValue() {
    var input = repeat("0", KUBERNETES_LABEL_MAX_LENGTH);
    var output = LabelValue.normalize(input);
    assertThat(output, is(input));
  }

  @Test
  public void shouldPadInvalidFirstChar() {
    var input = "-spotify";
    var output = LabelValue.normalize(input);
    var expected = "p-spotifye6181c8";
    assertThat(output, is(expected));
  }

  @Test
  public void shouldSuffixChangedShortValue() {
    var input = "spotify#com";
    var output = LabelValue.normalize(input);
    var expected = "spotifycom3de352c";
    assertThat(output, is(expected));
  }

  @Test
  public void shouldTruncateAndSuffixLongValue() {
    var input = repeat("0", KUBERNETES_LABEL_MAX_LENGTH + 1);
    var output = LabelValue.normalize(input);
    var expected = repeat("0", PREFIX_MAX_LENGTH) + "60e05bd";
    assertThat(output, is(expected));
  }

  @Test
  public void shouldRemoveMultiByteValueAndSuffix() {
    var input = "M\u0101rti\u0146\u0161";
    var output = LabelValue.normalize(input);
    var expected = "mrticd76e17";
    assertThat(output, is(expected));
  }

  @Test
  public void shouldRemoveMultiByteValueTruncateAndSuffix() {
    var input = repeat("\u0410", KUBERNETES_LABEL_MAX_LENGTH + 1);
    var output = LabelValue.normalize(input);
    var expected = "fc135e7";
    assertThat(output, is(expected));
  }

  @Test
  public void shouldKeepAsMuchAsPossibleNormalCharactersEvenWithWeirdMultiCharSymbols() {
    var input = repeat("a🤡", KUBERNETES_LABEL_MAX_LENGTH + 1);
    var output = LabelValue.normalize(input);
    var expected = repeat("a", PREFIX_MAX_LENGTH) + "31b551a";
    assertThat(output, is(expected));
  }

  @Test
  public void shouldNotBeConstructable() throws ReflectiveOperationException {
    assertThat(ClassEnforcer.assertNotInstantiable(LabelValue.class), is(true));
  }

  @Test
  @Parameters({
      "",
      "a",
      "test-value_including-0-and-9",
  })
  public void testIsNormalizedShouldAcceptConformingValues(String value) {
    assertTrue(LabelValue.isNormalized(value));
  }

  @Test
  @Parameters({
      "UPPER",
      "-dashes",
      "unsupported#chars",
  })
  public void testIsNormalizedShouldRejectNonConformingValues(String value) {
    assertFalse(LabelValue.isNormalized(value));
  }

  private static String repeat(String a, int count) {
    return a.repeat(Math.max(0, count));
  }
}
