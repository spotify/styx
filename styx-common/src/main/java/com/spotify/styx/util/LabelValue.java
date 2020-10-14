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

import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Locale;

public final class LabelValue {

  @VisibleForTesting
  static final int GCP_LABEL_MAX_LENGTH = 63;
  @VisibleForTesting
  static final int DIGEST_SUFFIX_LENGTH = 7;
  @VisibleForTesting
  static final int PREFIX_MAX_LENGTH = GCP_LABEL_MAX_LENGTH - DIGEST_SUFFIX_LENGTH;

  // Part of `bytesToHex` implementation.
  private static final char[] HEX_ARRAY = "0123456789abcdef".toCharArray();

  // https://stackoverflow.com/a/9855338
  private static String bytesToHex(byte[] bytes) {
    char[] hexChars = new char[bytes.length * 2];
    for (int i = 0; i < bytes.length; i++) {
      int v = bytes[i] & 0xFF;
      hexChars[i * 2] = HEX_ARRAY[v >>> 4];
      hexChars[i * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }

  /**
   * Cleanup the label value to comply with GCP restrictions.
   *
   * <p>Function returns original value in case it does not need any cleanup. If original value
   * needs cleanup (lowercase, replacing not allowed characters, truncating, etc), then SHA256 HEX
   * encoded value (first 7 characters) is suffixed to the cleaned value.
   *
   * @param value value of the label to store.
   *
   * @return normalized value, might contain less information than input.
   */
  public static String normalize(String value) {
    if (value.matches("^[a-z0-9_-]{1," + GCP_LABEL_MAX_LENGTH + "}$")) {
      return value;
    }

    // MessageDigest is not thread safe: https://stackoverflow.com/a/17555580
    final MessageDigest sha256;
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException(e);
    }

    byte[] digest = sha256.digest(value.getBytes(StandardCharsets.UTF_8));
    String hexDigest = bytesToHex(digest);
    String suffix = hexDigest.substring(0, DIGEST_SUFFIX_LENGTH);

    String lowerPrefix = value.toLowerCase(Locale.ROOT);
    String validPrefix = lowerPrefix.replaceAll("[^a-z0-9_-]", "");
    String prefix = validPrefix.substring(0, Math.min(PREFIX_MAX_LENGTH, validPrefix.length()));

    return prefix + suffix;
  }

  private LabelValue() {
    throw new UnsupportedOperationException();
  }
}
