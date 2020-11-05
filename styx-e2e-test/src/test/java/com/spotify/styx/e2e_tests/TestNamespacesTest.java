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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.time.Instant;
import org.junit.Test;

public class TestNamespacesTest {

  private static final Instant NOW = Instant.now();

  private static final String EXPIRED_NAMESPACE =
      TestNamespaces.createTestNamespace(NOW
          .minus(TestNamespaces.TEST_NAMESPACE_TTL)
          .minusSeconds(1));

  private static final String VALID_NAMESPACE =
      TestNamespaces.createTestNamespace(NOW);

  @Test
  public void isExpiredTestNamespace() {
    assertThat(TestNamespaces.isExpiredTestNamespace(EXPIRED_NAMESPACE, NOW), is(true));
    assertThat(TestNamespaces.isExpiredTestNamespace(VALID_NAMESPACE, NOW), is(false));
  }
}
