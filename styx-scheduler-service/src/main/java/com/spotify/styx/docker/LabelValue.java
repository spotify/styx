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

import com.google.common.io.BaseEncoding;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

final class LabelValue {

  private static final int KUBERNETES_LABEL_MAX_LENGTH = 63;
  private static final int DIGEST_SUFFIX_LENGTH = 7;
  private static final int PREFIX_MAX_LENGTH = KUBERNETES_LABEL_MAX_LENGTH - DIGEST_SUFFIX_LENGTH;

  private static final Pattern VALID =
      Pattern.compile(String.format("^(?:[a-z0-9]|[a-z0-9][a-z0-9_-]{0,%s}[a-z0-9])$",
          KUBERNETES_LABEL_MAX_LENGTH - 2));
  private static final Pattern INVALID = Pattern.compile("[^a-z0-9_-]");
  private static final List<Character> NON_ALPHANUMERICS = List.of('_', '-');

  private static final BaseEncoding BASE16_ENCODING_LOWER_CASE = BaseEncoding.base16().lowerCase();

  /**
   * Cleanup the label value to comply with Kubernetes restrictions as described by:
   * https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set
   * 
   * Note that this normalizer implements a subset of the restriction, e.g. lower case only, no `.`.
   *
   * @param value value of the label to store.
   *
   * @return normalized value, might contain less information than input.
   */
  public static String normalize(String value) {
    if (value.isEmpty() || VALID.matcher(value).matches()) {
      return value;
    }

    var paddedValue = padIfInvalidFirstChar(value);

    // MessageDigest is not thread safe: https://stackoverflow.com/a/17555580
    final MessageDigest sha256;
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    var digest = sha256.digest(paddedValue.getBytes(StandardCharsets.UTF_8));
    var hexDigest = BASE16_ENCODING_LOWER_CASE.encode(digest);
    var suffix = hexDigest.substring(0, DIGEST_SUFFIX_LENGTH);

    var lowerPrefix = paddedValue.toLowerCase(Locale.ROOT);
    var validPrefix = INVALID.matcher(lowerPrefix).replaceAll("");
    var prefix = validPrefix.substring(0, Math.min(PREFIX_MAX_LENGTH, validPrefix.length()));

    return prefix + suffix;
  }

  private static String padIfInvalidFirstChar(String value) {
    if (NON_ALPHANUMERICS.contains(value.charAt(0))) {
      return "p" + value;
    }
    return value;
  }

  private LabelValue() {
    throw new UnsupportedOperationException();
  }
}
