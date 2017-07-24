/*-
 * -\-\-
 * Spotify styx
 * --
 * Copyright (C) 2017 Spotify AB
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

import static org.junit.Assert.assertSame;

import org.junit.Test;

public class ApiVersionTest {

  @Test
  public void shouldParse() {
    assertSame(Api.Version.V0, Api.Version.valueOf("V0"));
    assertSame(Api.Version.V3, Api.Version.valueOf("V3"));
  }
}
