/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 Spotify AB
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

package com.spotify.styx.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;

public enum Schedule {
  HOURS, DAYS, WEEKS, MONTHS;

  @JsonValue
  public String toJson() {
    return name().toLowerCase(Locale.US);
  }

  @JsonCreator
  public static Schedule fromJson(String json) {
    final String upperCaseSchedule = json.toUpperCase(Locale.US);
    switch (upperCaseSchedule) {
      case "DAILY":
        return DAYS;
      case "HOURLY":
        return HOURS;
      case "WEEKLY":
        return WEEKS;
      case "MONTHLY":
        return MONTHS;

      default:
        return valueOf(upperCaseSchedule);
    }
  }
}
