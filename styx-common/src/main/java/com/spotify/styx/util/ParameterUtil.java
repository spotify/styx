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

import static com.spotify.styx.model.Schedule.WellKnown.MONTHLY;
import static com.spotify.styx.model.Schedule.WellKnown.YEARLY;
import static java.lang.Integer.parseInt;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;

import com.google.common.collect.Lists;
import com.spotify.styx.model.Schedule;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helpers for working with {@link com.spotify.styx.model.Workflow} parameter handling.
 */
public final class ParameterUtil {

  private static final Pattern YEAR_PATTERN = Pattern.compile("(\\d{4})(-\\d{2})?");
  private static final Pattern YEAR_MONTH_PATTERN = Pattern.compile("(\\d{4})-(\\d{2})");

  private static final int MIN_YEAR_WIDTH = 4;
  private static final int MAX_YEAR_WIDTH = 10;

  private static final DateTimeFormatter DATE_HOUR_FORMAT = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .optionalStart()
      .appendLiteral('T')
      .appendValue(ChronoField.HOUR_OF_DAY, 2)
      .optionalStart()
      .appendLiteral(':')
      .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
      .optionalStart()
      .appendLiteral(':')
      .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
      .optionalStart()
      .appendLiteral('.')
      .appendValue(ChronoField.NANO_OF_SECOND, 1, 9, SignStyle.NORMAL)
      .optionalEnd()
      .optionalEnd()
      .optionalEnd()
      .optionalStart()
      .appendZoneOrOffsetId()
      .optionalEnd()
      .optionalEnd()
      .toFormatter()
      .withResolverStyle(ResolverStyle.STRICT)
      .withZone(UTC);

  private static final DateTimeFormatter ISO_LOCAL_YEAR = new DateTimeFormatterBuilder()
      .appendValue(ChronoField.YEAR, MIN_YEAR_WIDTH, MAX_YEAR_WIDTH, SignStyle.EXCEEDS_PAD)
      .toFormatter();

  private static final DateTimeFormatter ISO_LOCAL_MONTH = new DateTimeFormatterBuilder()
      .appendValue(ChronoField.YEAR, MIN_YEAR_WIDTH, MAX_YEAR_WIDTH, SignStyle.EXCEEDS_PAD)
      .appendLiteral('-')
      .appendValue(MONTH_OF_YEAR, 2)
      .toFormatter();

  private static final DateTimeFormatter ISO_LOCAL_DATE_HOUR = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral('T')
      .appendValue(ChronoField.HOUR_OF_DAY, 2)
      .toFormatter();

  private ParameterUtil() {
    throw new UnsupportedOperationException();
  }

  public static Instant parseDate(String date) {
    return Instant.from(LocalDate.from(
        DateTimeFormatter.ISO_LOCAL_DATE.parse(date))
                            .atStartOfDay(UTC));
  }

  public static Instant parseDateHour(String dateHour) {
    return Instant.from(LocalDateTime.from(
        ISO_LOCAL_DATE_HOUR.parse(dateHour)).atOffset(UTC));
  }

  public static String toParameter(Schedule schedule, Instant instant) {
    switch (schedule.wellKnown()) {
      case DAILY:
      case WEEKLY:
        return formatDate(instant);
      case HOURLY:
        return formatDateHour(instant);
      case MONTHLY:
        return formatMonth(instant);
      case YEARLY:
        return formatYear(instant);

      default:
        return formatDateTime(instant);
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
      instantToProcess = TimeUtil.nextInstant(instantToProcess, schedule);
    }

    return listOfInstants;
  }

  public static Instant parseAlignedInstant(String dateHour, Schedule schedule) {
    final Schedule.WellKnown wellKnown = schedule.wellKnown();

    Matcher matcher;
    final ZonedDateTime parsed;
    if (wellKnown == YEARLY && (matcher = YEAR_PATTERN.matcher(dateHour)).matches()) {
      parsed = LocalDate.of(parseInt(matcher.group(1)), 1, 1).atStartOfDay(UTC);
    } else if (wellKnown == MONTHLY && (matcher = YEAR_MONTH_PATTERN.matcher(dateHour)).matches()) {
      parsed = LocalDate.of(parseInt(matcher.group(1)), parseInt(matcher.group(2)), 1).atStartOfDay(UTC);
    } else {
      parsed = tryParseDateHour(dateHour);
    }

    final Instant instant = parsed.toInstant();

    // test alignment
    if (!TimeUtil.isAligned(instant, schedule)) {
      return TimeUtil.previousInstant(instant, schedule);
    } else {
      return instant;
    }
  }

  private static ZonedDateTime tryParseDateHour(String dateTime) {
    try {
      try {
        return ZonedDateTime.parse(dateTime, DATE_HOUR_FORMAT);
      } catch (DateTimeParseException ignore) {
        return LocalDate.parse(dateTime, DATE_HOUR_FORMAT).atStartOfDay(UTC);
      }
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          "Cannot parse time parameter " + dateTime + " - " + e.getMessage(),
          e);
    }
  }

  private static String formatDateTime(Instant instant) {
    return ISO_OFFSET_DATE_TIME.format(
        instant.truncatedTo(ChronoUnit.MINUTES)
            .atOffset(UTC));
  }

  private static String formatDate(Instant instant) {
    return DateTimeFormatter.ISO_LOCAL_DATE.format(
        instant.atOffset(UTC));
  }

  private static String formatDateHour(Instant instant) {
    return ISO_LOCAL_DATE_HOUR.format(
        instant.truncatedTo(ChronoUnit.HOURS)
            .atOffset(UTC));
  }

  private static String formatMonth(Instant instant) {
    return ISO_LOCAL_MONTH.format(
        instant.truncatedTo(ChronoUnit.DAYS)
            .atOffset(UTC));
  }

  private static String formatYear(Instant instant) {
    return ISO_LOCAL_YEAR.format(
        instant.truncatedTo(ChronoUnit.DAYS)
            .atOffset(UTC));
  }
}
