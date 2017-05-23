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

import static com.spotify.styx.model.Schedule.DAYS;
import static com.spotify.styx.model.Schedule.HOURS;
import static com.spotify.styx.model.Schedule.MONTHS;
import static com.spotify.styx.model.Schedule.WEEKS;
import static com.spotify.styx.model.Schedule.YEARS;
import static com.spotify.styx.util.ParameterUtil.parseAlignedInstant;
import static com.spotify.styx.util.ParameterUtil.rangeOfInstants;
import static java.time.Instant.parse;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.auto.value.AutoValue;
import com.spotify.styx.model.Schedule;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import org.junit.Test;

public class ParameterUtilTest {

  private static final Instant TIME = parse("2016-01-19T09:11:22.333Z");

  @Test
  public void testToParameter() throws Exception {
    assertThat(ParameterUtil.toParameter(Schedule.HOURS, TIME), is("2016-01-19T09"));
    assertThat(ParameterUtil.toParameter(Schedule.DAYS, TIME), is("2016-01-19"));
    assertThat(ParameterUtil.toParameter(Schedule.WEEKS, TIME), is("2016-01-19"));
    assertThat(ParameterUtil.toParameter(Schedule.MONTHS, TIME), is("2016-01"));
    assertThat(ParameterUtil.toParameter(Schedule.YEARS, TIME), is("2016"));

    assertThat(ParameterUtil.toParameter(Schedule.parse("0 * * * *"), TIME),
        is("2016-01-19T09:11:00Z"));
  }

  @Test
  public void shouldParseDateHour() throws Exception {
    final Instant instant = ParameterUtil.parseDateHour("2016-01-19T08");

    assertThat(instant, is(parse("2016-01-19T08:00:00.000Z")));
  }

  @Test
  public void shouldParseDate() throws Exception {
    final Instant instant = ParameterUtil.parseDate("2016-01-19");

    assertThat(instant, is(parse("2016-01-19T00:00:00.000Z")));
  }

  @Test
  public void shouldRangeOfInstantsHours() throws Exception {
    final Instant startInstant = parse("2016-12-31T23:00:00.00Z");
    final Instant endInstant = parse("2017-01-01T02:00:00.00Z");

    List<Instant> list = rangeOfInstants(startInstant, endInstant, HOURS);
    assertThat(list, contains(
        parse("2016-12-31T23:00:00.00Z"),
        parse("2017-01-01T00:00:00.00Z"),
        parse("2017-01-01T01:00:00.00Z"))
    );
  }

  @Test
  public void shouldRangeOfInstantsDays() throws Exception {
    final Instant startInstant = parse("2016-12-31T00:00:00.00Z");
    final Instant endInstant = parse("2017-01-03T00:00:00.00Z");

    List<Instant> list = rangeOfInstants(startInstant, endInstant, Schedule.DAYS);
    assertThat(list, contains(
        parse("2016-12-31T00:00:00.00Z"),
        parse("2017-01-01T00:00:00.00Z"),
        parse("2017-01-02T00:00:00.00Z"))
    );
  }

  @Test
  public void shouldRangeOfInstantsWeeks() throws Exception {
    final Instant startInstant = parse("2016-12-26T00:00:00.00Z");
    final Instant endInstant = parse("2017-01-16T00:00:00.00Z");

    List<Instant> list = rangeOfInstants(startInstant, endInstant, Schedule.WEEKS);
    assertThat(list, contains(
        parse("2016-12-26T00:00:00.00Z"),
        parse("2017-01-02T00:00:00.00Z"),
        parse("2017-01-09T00:00:00.00Z"))
    );
  }

  @Test
  public void shouldRangeOfInstantsMonths() throws Exception {
    final Instant startInstant = parse("2017-01-01T00:00:00.00Z");
    final Instant endInstant = parse("2017-04-01T00:00:00.00Z");

    List<Instant> list = rangeOfInstants(startInstant, endInstant, MONTHS);
    assertThat(list, contains(
        parse("2017-01-01T00:00:00.00Z"),
        parse("2017-02-01T00:00:00.00Z"),
        parse("2017-03-01T00:00:00.00Z"))
    );
  }

  @Test
  public void shouldReturnEmptyListStartEqualsEnd() throws Exception {
    final Instant startInstant = parse("2016-12-31T23:00:00.00Z");
    final Instant endInstant = parse("2016-12-31T23:00:00.00Z");

    List<Instant> list = rangeOfInstants(startInstant, endInstant, HOURS);
    assertThat(list, hasSize(0));
  }

  @Test(expected=IllegalArgumentException.class)
  public void shouldRaiseRangeOfInstantsStartAfterEnd() throws Exception {
    final Instant startInstant = parse("2016-12-31T23:00:00.00Z");
    final Instant endInstant = parse("2016-01-01T01:00:00.00Z");

    rangeOfInstants(startInstant, endInstant, HOURS);
  }

  private static final List<ParseExample> PARSE_EXAMPLES;
  private static final List<ParseExample> UNPARSABLE;

  static {
    PARSE_EXAMPLES = Arrays.asList(
        example("2017-03-25T10",             HOURS,  parse("2017-03-25T10:00:00.00Z")),
        example("2017-03-25T10:15",          HOURS,  parse("2017-03-25T10:00:00.00Z")),
        example("2017-03-25T10:15:25",       HOURS,  parse("2017-03-25T10:00:00.00Z")),
        example("2017-03-25T10:15:25.1",     HOURS,  parse("2017-03-25T10:00:00.00Z")),
        example("2017-03-25T10:15:25.12",    HOURS,  parse("2017-03-25T10:00:00.00Z")),
        example("2017-03-25T10:15:25.123",   HOURS,  parse("2017-03-25T10:00:00.00Z")),
        example("2017-03-25T10:15:25.123Z",  HOURS,  parse("2017-03-25T10:00:00.00Z")),
        example("2017-03-25T10:15:25+01:00", HOURS,  parse("2017-03-25T09:00:00.00Z")),
        example("2017-03-25T10:15:25-01:00", HOURS,  parse("2017-03-25T11:00:00.00Z")),

        example("2017-03-25",                DAYS,   parse("2017-03-25T00:00:00.00Z")),
        example("2017-03-25T10:15",          DAYS,   parse("2017-03-25T00:00:00.00Z")),
        example("2017-03-25T10:15:25",       DAYS,   parse("2017-03-25T00:00:00.00Z")),
        example("2017-03-25T10:15:25.1",     DAYS,   parse("2017-03-25T00:00:00.00Z")),
        example("2017-03-25T10:15:25.12",    DAYS,   parse("2017-03-25T00:00:00.00Z")),
        example("2017-03-25T10:15:25.123",   DAYS,   parse("2017-03-25T00:00:00.00Z")),
        example("2017-03-25T10:15:25.123Z",  DAYS,   parse("2017-03-25T00:00:00.00Z")),
        example("2017-03-25T10:15:25+01:00", DAYS,   parse("2017-03-25T00:00:00.00Z")),
        example("2017-03-25T10:15:25-01:00", DAYS,   parse("2017-03-25T00:00:00.00Z")),
        example("2017-03-25T23:00-03:00",    DAYS,   parse("2017-03-26T00:00:00.00Z")),
        example("2017-03-25T01:00+03:00",    DAYS,   parse("2017-03-24T00:00:00.00Z")),

        example("2017-03-25",                WEEKS,  parse("2017-03-20T00:00:00.00Z")),
        example("2017-03-25T10:10:10",       WEEKS,  parse("2017-03-20T00:00:00.00Z")),
        example("2017-03-27",                WEEKS,  parse("2017-03-27T00:00:00.00Z")),
        example("2017-03-27T10:10:10",       WEEKS,  parse("2017-03-27T00:00:00.00Z")),
        example("2017-03-29",                WEEKS,  parse("2017-03-27T00:00:00.00Z")),
        example("2017-03-29T10:10:10",       WEEKS,  parse("2017-03-27T00:00:00.00Z")),
        example("2017-03-27T00+01:00",       WEEKS,  parse("2017-03-20T00:00:00.00Z")),
        example("2017-03-26T23-01:00",       WEEKS,  parse("2017-03-27T00:00:00.00Z")),

        example("2017-02",                   MONTHS, parse("2017-02-01T00:00:00.00Z")),
        example("2017-02-03",                MONTHS, parse("2017-02-01T00:00:00.00Z")),
        example("2017-02-03",                MONTHS, parse("2017-02-01T00:00:00.00Z")),
        example("2017-02-03T04",             MONTHS, parse("2017-02-01T00:00:00.00Z")),
        example("2017-02-01T00+01:00",       MONTHS, parse("2017-01-01T00:00:00.00Z")),
        example("2017-01-31T23-01:00",       MONTHS, parse("2017-02-01T00:00:00.00Z")),

        example("2017",                      YEARS,  parse("2017-01-01T00:00:00.00Z")),
        example("2017-02",                   YEARS,  parse("2017-01-01T00:00:00.00Z")),
        example("2017-02-03",                YEARS,  parse("2017-01-01T00:00:00.00Z")),
        example("2017-02-03T13",             YEARS,  parse("2017-01-01T00:00:00.00Z")),
        example("2017-01-01T00+01:00",       YEARS,  parse("2016-01-01T00:00:00.00Z")),
        example("2016-12-31T23-01:00",       YEARS,  parse("2017-01-01T00:00:00.00Z")),

        example("2017-03-25T10:15:25+01:00", Schedule.parse("* * * * *"),
            parse("2017-03-25T09:15:00Z")),
        example("2017-03-25T10:15:25", Schedule.parse("0 2 * * *"),
            parse("2017-03-25T02:00:00Z")),
        example("2017-03-25T10:15:25", Schedule.parse("* 3,8 1 1 *"),
            parse("2017-01-01T08:59:00Z")),
        example("2017-01-01T07:15:25", Schedule.parse("* 3,8 1 1 *"),
            parse("2017-01-01T03:59:00Z"))
    );

    UNPARSABLE = Arrays.asList(
        example("2017-03-26+01:00", DAYS),
        example("2017-03-26+01:00", WEEKS),
        example("2017-04-31", DAYS)
    );
  }

  @Test
  public void parseAllExamples() throws Exception {
    for (ParseExample example : PARSE_EXAMPLES) {
      Instant parsed = parseAlignedInstant(example.toParse(), example.schedule());
      assertThat(
          "Parsing " + example.toParse() + " " + example.schedule(),
          parsed, is(example.expected()));
    }
  }

  @Test
  public void unparsableExamples() throws Exception {
    for (ParseExample example : UNPARSABLE) {
      try {
        parseAlignedInstant(example.toParse(), example.schedule());
        fail();
      } catch (IllegalArgumentException e) {
        continue;
      }
      fail();
    }
  }

  static ParseExample example(String toParse, Schedule schedule, Instant expected) {
    return new AutoValue_ParameterUtilTest_ParseExample(toParse, schedule, expected);
  }

  static ParseExample example(String toParse, Schedule schedule) {
    return new AutoValue_ParameterUtilTest_ParseExample(toParse, schedule, null);
  }

  @AutoValue
  public static abstract class ParseExample {
    abstract String toParse();
    abstract Schedule schedule();
    @Nullable abstract Instant expected();
  }
}
