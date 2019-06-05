/*-
 * -\-\-
 * Spotify End-to-End Integration Tests
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

package com.spotify.styx.e2e_tests;

import static com.spotify.styx.e2e_tests.EndToEndTestBase.TIMESTAMP_FORMATTER;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TestNamespaces {

  private static final Logger log = LoggerFactory.getLogger(TestNamespaces.class);

  static final Duration TEST_NAMESPACE_TTL = Duration.ofHours(2);

  static final String TEST_NAMESPACE_PREFIX = "styx-e2e-test";

  static final Pattern TEST_NAMESPACE_PATTERN = Pattern.compile(
      TEST_NAMESPACE_PREFIX + "-(?<timestamp>\\w+-\\w+)-(?<random>\\w+)");

  private TestNamespaces() {
    throw new UnsupportedOperationException();
  }

  /**
   * Generate a unique test namespace.
   */
  static String createTestNamespace(Instant now) {
    return String.join("-",
        TEST_NAMESPACE_PREFIX,
        TIMESTAMP_FORMATTER.format(now),
        Integer.toHexString(ThreadLocalRandom.current().nextInt()));
  }

  /**
   * Check whether a namespace is expired and can be safely deleted.
   */
  static boolean isExpiredTestNamespace(String namespace, Instant now) {
    var matcher = TEST_NAMESPACE_PATTERN.matcher(namespace);
    if (!matcher.matches()) {
      return false;
    }
    final Instant timestamp;
    var timestampString = matcher.group("timestamp");
    try {
      timestamp = TIMESTAMP_FORMATTER.parse(timestampString, Instant::from);
    } catch (Exception e) {
      log.warn("Failed to parse namespace timestamp: " + timestampString, e);
      return false;
    }
    // Consider test namespaces to be expired after a reasonable TTL in order to not
    // interfere with concurrently executing tests.
    if (timestamp.isAfter(now.minus(TEST_NAMESPACE_TTL))) {
      return false;
    }
    return true;
  }

  static String testNamespaceTimeTimestamp(String namespace) {
    var matcher = TEST_NAMESPACE_PATTERN.matcher(namespace);
    if (!matcher.matches()) {
      throw new IllegalArgumentException();
    }
    return matcher.group("timestamp");
  }

  static String testNamespaceRandom(String namespace) {
    var matcher = TEST_NAMESPACE_PATTERN.matcher(namespace);
    if (!matcher.matches()) {
      throw new IllegalArgumentException();
    }
    return matcher.group("random");
  }
}
