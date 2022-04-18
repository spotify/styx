/*-
 * -\-\-
 * Spotify Styx API Service
 * --
 * Copyright (C) 2016 - 2022 Spotify AB
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

package com.spotify.styx.api.util;

public enum QueryParams {
  DEPLOYMENT_TYPE("deployment_type"),
  DEPLOYMENT_TIME_BEFORE("deployment_time_before"),
  DEPLOYMENT_TIME_AFTER("deployment_time_after");


  private final String string;

  QueryParams(String string) {
    this.string = string;
  }

  public String getString() {
    return string;
  }
}
