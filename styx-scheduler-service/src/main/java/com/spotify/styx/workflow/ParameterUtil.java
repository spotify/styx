/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx.workflow;

import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.WorkflowInstance;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import javaslang.control.Either;

/**
 * Helpers for working with {@link com.spotify.styx.model.Workflow} parameter handling.
 */
public final class ParameterUtil {

  public static final String HOUR_PATTERN = "yyyy-MM-dd'T'HH";
  public static final String DAY_PATTERN = "yyyy-MM-dd";

  private ParameterUtil() {
  }

  private static final int MIN_YEAR_WIDTH = 4;
  private static final int MAX_YEAR_WIDTH = 10;
  private static final DateTimeFormatter ISO_LOCAL_MONTH = new DateTimeFormatterBuilder()
      .appendValue(YEAR, MIN_YEAR_WIDTH, MAX_YEAR_WIDTH, SignStyle.EXCEEDS_PAD)
      .appendLiteral('-')
      .appendValue(MONTH_OF_YEAR, 2)
      .toFormatter();

  private static final DateTimeFormatter ISO_LOCAL_DATE_HOUR = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral('T')
      .appendValue(ChronoField.HOUR_OF_DAY, 2)
      .toFormatter();

  public static String formatDateTime(Instant instant) {
    return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(
        instant.atOffset(ZoneOffset.UTC));
  }

  public static String formatDate(Instant instant) {
    return DateTimeFormatter.ISO_LOCAL_DATE.format(
        instant.atOffset(ZoneOffset.UTC));
  }

  public static String formatDateHour(Instant instant) {
    return ISO_LOCAL_DATE_HOUR.format(
        instant.truncatedTo(ChronoUnit.HOURS)
            .atOffset(ZoneOffset.UTC));
  }

  public static String formatMonth(Instant instant) {
    return ISO_LOCAL_MONTH.format(
        instant.truncatedTo(ChronoUnit.DAYS)
            .atOffset(ZoneOffset.UTC));
  }

  public static Either<String, Instant> instantFromWorkflowInstance(
      WorkflowInstance workflowInstance,
      Partitioning partitioning) {
    switch (partitioning) {
      case HOURS:
        try {
          final LocalDateTime localDateTime = LocalDateTime.parse(
              workflowInstance.parameter(),
              DateTimeFormatter.ofPattern(HOUR_PATTERN));
          return Either.right(localDateTime.toInstant(ZoneOffset.UTC));
        } catch (DateTimeParseException e) {
          return Either.left(parseErrorMessage(partitioning, HOUR_PATTERN));
        }

      case DAYS:
        try {
          final LocalDate localDate = LocalDate.parse(
              workflowInstance.parameter(),
              DateTimeFormatter.ofPattern(DAY_PATTERN));
          return Either.right(localDate.atStartOfDay().toInstant(ZoneOffset.UTC));
        } catch (DateTimeParseException e) {
          return Either.left(parseErrorMessage(partitioning, DAY_PATTERN));
        }

      case WEEKS:
        try {
          LocalDate localDate = LocalDate.parse(
              workflowInstance.parameter(),
              DateTimeFormatter.ofPattern(DAY_PATTERN));
          int daysToSubtract = localDate.getDayOfWeek().getValue();
          localDate = localDate.minusDays(daysToSubtract - 1);
          return Either.right(localDate.atStartOfDay().toInstant(ZoneOffset.UTC));
        } catch (DateTimeParseException e) {
          return Either.left(parseErrorMessage(partitioning, DAY_PATTERN));
        }

      default:
        return Either.left("Partitioning not supported: " + partitioning);
    }
  }

  private static String parseErrorMessage(Partitioning partitioning, String pattern) {
    return String.format(
        "Cannot parse time parameter. Expected partitioning is %s: %s",
        partitioning,
        pattern);
  }
}
