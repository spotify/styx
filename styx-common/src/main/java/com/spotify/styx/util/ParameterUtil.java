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

package com.spotify.styx.util;

import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

import com.google.common.collect.Lists;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.WorkflowInstance;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.List;
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
        instant.truncatedTo(ChronoUnit.SECONDS)
            .atOffset(UTC));
  }

  public static String formatDate(Instant instant) {
    return DateTimeFormatter.ISO_LOCAL_DATE.format(
        instant.atOffset(UTC));
  }

  public static String formatDateHour(Instant instant) {
    return ISO_LOCAL_DATE_HOUR.format(
        instant.truncatedTo(ChronoUnit.HOURS)
            .atOffset(UTC));
  }

  public static String formatMonth(Instant instant) {
    return ISO_LOCAL_MONTH.format(
        instant.truncatedTo(ChronoUnit.DAYS)
            .atOffset(UTC));
  }

  public static Instant parseDate(String date) {
    return Instant.from(LocalDate.from(
        DateTimeFormatter.ISO_LOCAL_DATE.parse(date))
                            .atStartOfDay(ZoneOffset.UTC));
  }

  public static Instant parseDateHour(String dateHour) {
    return Instant.from(LocalDateTime.from(
        ISO_LOCAL_DATE_HOUR.parse(dateHour)).atOffset(ZoneOffset.UTC));
  }

  public static String toParameter(Schedule schedule, Instant instant) {
    switch (schedule.wellKnown()) {
      case DAILY:
      case WEEKLY:
        return ParameterUtil.formatDate(instant);
      case HOURLY:
        return ParameterUtil.formatDateHour(instant);
      case MONTHLY:
        return ParameterUtil.formatMonth(instant);

      default:
        return ParameterUtil.formatDateTime(instant);
    }
  }

  /**
   * Increments an instant for an amount of 1 {@link Schedule} unit.
   */
  public static Instant incrementInstant(Instant instant, Schedule schedule) {
    LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneId.of(UTC.toString()));
    dateTime = dateTime.plus(1, scheduleToTemporalUnit(schedule));
    Instant updatedInstant = dateTime.atZone(ZoneId.of(UTC.toString())).toInstant();
    return updatedInstant;
  }

  /**
   * Converts {@link Schedule} to {@link ChronoUnit}.
   */
  public static TemporalUnit scheduleToTemporalUnit(Schedule schedule) {
    switch (schedule.wellKnown()) {
      case HOURLY:
        return ChronoUnit.HOURS;
      case DAILY:
        return ChronoUnit.DAYS;
      case WEEKLY:
        return ChronoUnit.WEEKS;
      case MONTHLY:
        return ChronoUnit.MONTHS;

      default:
        throw new IllegalArgumentException("Schedule not supported: " + schedule);
    }
  }

  /**
   * Truncates an instant based on schedule, e.g. '2016-10-10T15:22:111' and schedule HOURS,
   * the result would be '2016-10-10T15:00:000'.
   */
  public static Instant truncateInstant(Instant instant, Schedule schedule) {
    switch (schedule.wellKnown()) {
      case HOURLY:
        return instant.truncatedTo(ChronoUnit.HOURS);

      case DAILY:
        return instant.truncatedTo(ChronoUnit.DAYS);

      case WEEKLY:
        LocalDateTime dateTime = LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
        int daysToSubtract = dateTime.getDayOfWeek().getValue();
        dateTime = dateTime.minusDays(daysToSubtract - 1);
        Instant resultInstant = dateTime.toInstant(ZoneOffset.UTC);
        return resultInstant.truncatedTo(ChronoUnit.DAYS);

      case MONTHLY:
        ZonedDateTime truncatedToMonth = instant.atZone(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1);
        return truncatedToMonth.toInstant();

      default:
        throw new IllegalArgumentException("Schedule not supported: " + schedule);
    }
  }

  /**
   * Generates a list of {@link Instant}s obtained by partitioning a time range defined by a
   * starting and a ending {@link Instant}s, and based on the provided {@link Schedule}.
   *
   * @param startInstant              Defines the start of the time range (inclusive)
   * @param endInstant                Defines the end of the time range (exclusive)
   * @param schedule                  The schedule unit to split the time range into
   * @throws IllegalArgumentException If the starting {@link Instant} is later than the ending
   *                                  {@link Instant}
   */
  public static List<Instant> rangeOfInstants(Instant startInstant, Instant endInstant, Schedule schedule) {
    if (endInstant.isBefore(startInstant)) {
      throw new IllegalArgumentException("End time cannot be earlier the start time");
    }
    final List<Instant> listOfInstants = Lists.newArrayList();

    Instant instantToProcess = startInstant;
    while (instantToProcess.isBefore(endInstant)) {
      listOfInstants.add(instantToProcess);
      instantToProcess = incrementInstant(instantToProcess, schedule);
    }

    return listOfInstants;
  }

  public static Either<String, Instant> instantFromWorkflowInstance(
      WorkflowInstance workflowInstance,
      Schedule schedule) {
    switch (schedule.wellKnown()) {
      case HOURLY:
        try {
          final LocalDateTime localDateTime = LocalDateTime.parse(
              workflowInstance.parameter(),
              DateTimeFormatter.ofPattern(HOUR_PATTERN));
          return Either.right(localDateTime.toInstant(UTC));
        } catch (DateTimeParseException e) {
          return Either.left(parseErrorMessage(schedule, HOUR_PATTERN));
        }
      case DAILY:
        try {
          final LocalDate localDate = LocalDate.parse(
              workflowInstance.parameter(),
              DateTimeFormatter.ofPattern(DAY_PATTERN));
          return Either.right(localDate.atStartOfDay().toInstant(UTC));
        } catch (DateTimeParseException e) {
          return Either.left(parseErrorMessage(schedule, DAY_PATTERN));
        }
      case WEEKLY:
        try {
          LocalDate localDate = LocalDate.parse(
              workflowInstance.parameter(),
              DateTimeFormatter.ofPattern(DAY_PATTERN));
          int daysToSubtract = localDate.getDayOfWeek().getValue();
          localDate = localDate.minusDays(daysToSubtract - 1);
          return Either.right(localDate.atStartOfDay().toInstant(UTC));
        } catch (DateTimeParseException e) {
          return Either.left(parseErrorMessage(schedule, DAY_PATTERN));
        }

      default:
        return Either.left("Schedule not supported: " + schedule);
    }
  }

  private static String parseErrorMessage(Schedule schedule, String pattern) {
    return String.format(
        "Cannot parse time parameter. Expected schedule is %s: %s",
        schedule,
        pattern);
  }
}
