/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.api;

import io.norberg.automatter.AutoMatter;
import java.util.Optional;

@AutoMatter
public interface TestServiceAccountUsageAuthorizationResponse {

  /**
   * Successfully authorized?
   */
  boolean authorized();

  /**
   * A message describing the authorization or denial reason.
   */
  Optional<String> message();

  /**
   * The service account that usage authorization was tested against.
   */
  String serviceAccount();

  /**
   * The principal (user) that service account usage authorization was tested for.
   */
  String principal();

  static TestServiceAccountUsageAuthorizationResponseBuilder builder() {
    return new TestServiceAccountUsageAuthorizationResponseBuilder();
  }
}
