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

package com.spotify.styx.state.handlers;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.stream.Stream;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class StyxIdToFlyteExecNameMapperTest {

  private final StyxIdToFlyteExecNameMapper mapper = new StyxIdToFlyteExecNameMapper();

  @Test
  @Parameters(method = "styxRunIds")
  public void shouldMapStyxRunIdsToDNS1123subdomains(String styxRunId) {
    var execName = mapper.apply(styxRunId);

    assertThat(execName, matchesRegex("^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$"));
  }

  @Test
  @Parameters(method = "styxRunIds")
  public void shouldMapStyxRunIdsToStringStartingWithLowerAlphabeticChar(String styxRunId) {
    var execName = mapper.apply(styxRunId);

    var startChar = execName.charAt(0);
    assertTrue(Character.isAlphabetic(startChar));
    assertTrue(Character.isLowerCase(startChar));
  }

  @Test
  @Parameters(method = "styxRunIds")
  public void shouldMapStyxRunIdsToStringOfRightSize(String styxRunId) {
    var execName = mapper.apply(styxRunId);

    assertThat(execName, Matchers.hasLength(20));
  }

  @Test
  public void differentStyxRunIdsShouldMapIdsToDifferentFlyteExecNames() {
    var distinctCount = Stream.of(styxRunIds())
        .map(mapper)
        .distinct()
        .count();

    assertEquals(styxRunIds().length, distinctCount);
  }

  @Test
  @Parameters(method = "invalidStyxRunIds")
  public void shouldRejectInvalidStyxRunIds(String invalidStyxRunId) {
    var exception = assertThrows(IllegalArgumentException.class, () -> mapper.apply(invalidStyxRunId));

    assertThat(exception.getMessage(), is("Not valid styx run id: [" + invalidStyxRunId + "]"));
  }

  private String[] styxRunIds() {
    return new String[] {
        "styx-run-6685aa66-9d12-4ddf-8212-c1a942a6e5ca",
        "styx-run-ca2dad2b-e6ab-4463-85de-5f9523722bbd",
        "styx-run-fe60fdd1-1100-4d70-b052-b2b0d77b648c",
        "styx-run-71d0d15f-d2bc-4db1-993e-9cdfdb6057c0",
        "styx-run-c73cc52c-522f-4de7-9c85-e7d39f7a7b2f",
        "styx-run-8747c0eb-c0e9-4aee-bc80-5ffe3aa336d6",
        "styx-run-cbd69d50-7f30-451b-94b8-3d817d64cb0b",
        "styx-run-f31c3ba9-4d7e-4d4b-803f-4f186b3ce354",
        "styx-run-1160a5fd-72b3-4806-9f55-95a87cef74dc",
        "styx-run-92765d3f-0630-488a-8c76-0ee2401479e1"
    };
  }

  private String[] invalidStyxRunIds() {
    return new String[] {
        "noprefix-ca2dad2b-e6ab-4463-85de-5f9523722bbd", // prefix
        "styx-run-6685AA66-9D12-4DDF-8212-C1A942A6E5CA", // uppercase
        "styx-run-ca2dad2b-e6ab-4463-85de-zzzzzzzzzzzz", // not ex
        "styx-run-71d0d15f-d2bc-4db1-993e-9cdfdb6057c0-ff" // wrong size
    };
  }
}