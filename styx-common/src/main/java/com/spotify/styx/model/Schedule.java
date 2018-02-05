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
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableSet;
import java.util.Locale;
import java.util.Set;

@AutoValue
public abstract class Schedule {

  public static final Schedule HOURS = parse("@hourly");
  public static final Schedule DAYS = parse("@daily");
  public static final Schedule WEEKS = parse("@weekly");
  public static final Schedule MONTHS = parse("@monthly");
  public static final Schedule YEARS = parse("@yearly");

  public abstract String expression();

  public WellKnown wellKnown() {
    return toWellKnown(expression());
  }

  private static WellKnown toWellKnown(String expression) {
    switch (expression.toLowerCase(Locale.US)) {
      case "@hourly":
      case "hourly":
      case "hours":
        return WellKnown.HOURLY;
      case "@daily":
      case "daily":
      case "days":
        return WellKnown.DAILY;
      case "@weekly":
      case "weekly":
      case "weeks":
        return WellKnown.WEEKLY;
      case "@monthly":
      case "monthly":
      case "months":
        return WellKnown.MONTHLY;
      case "@annually":
      case "annually":
      case "@yearly":
      case "yearly":
      case "years":
        return WellKnown.YEARLY;

      default:
        return WellKnown.UNKNOWN;
    }
  }

  @Override
  public String toString() {
    switch (wellKnown()) {
      case HOURLY:
        return "HOURS";
      case DAILY:
        return "DAYS";
      case WEEKLY:
        return "WEEKS";
      case MONTHLY:
        return "MONTHS";
      case YEARLY:
        return "YEARS";

      default:
        return expression();
    }
  }

  @JsonValue
  public String toJson() {
    return toString().toLowerCase(Locale.US);
  }

  @JsonCreator
  public static Schedule parse(String expression) {
    final String normalizedExpression;
    switch (toWellKnown(expression)) {
      case HOURLY:
        normalizedExpression = "@hourly";
        break;
      case DAILY:
        normalizedExpression = "@daily";
        break;
      case WEEKLY:
        normalizedExpression = "@weekly";
        break;
      case MONTHLY:
        normalizedExpression = "@monthly";
        break;
      case YEARLY:
        normalizedExpression = "@yearly";
        break;

      default:
        normalizedExpression = expression;
        break;
    }
    return new AutoValue_Schedule(normalizedExpression);
  }

  // todo: remove
  public static Set<Schedule> values() {
    return ImmutableSet.of(HOURS, DAYS, WEEKS, MONTHS);
  }

  public enum WellKnown {
    HOURLY, DAILY, WEEKLY, MONTHLY, YEARLY, UNKNOWN
  }
}
