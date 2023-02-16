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
import static java.time.Instant.parse;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import com.google.auto.value.AutoValue;
import com.spotify.styx.model.Schedule;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class ParameterUtilTest {

  private static final Instant TIME = parse("2016-01-19T09:11:22.333Z");

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
  public void testToParameter() {
    assertThat(ParameterUtil.toParameter(HOURS, TIME), is("2016-01-19T09"));
    assertThat(ParameterUtil.toParameter(DAYS, TIME), is("2016-01-19"));
    assertThat(ParameterUtil.toParameter(WEEKS, TIME), is("2016-01-19"));
    assertThat(ParameterUtil.toParameter(MONTHS, TIME), is("2016-01"));
    assertThat(ParameterUtil.toParameter(YEARS, TIME), is("2016"));

    assertThat(ParameterUtil.toParameter(Schedule.parse("0 * * * *"), TIME),
        is("2016-01-19T09:11:00Z"));
  }

  @Test
  @Parameters({
      "2016-01-19, 2016-01-19T00:00:00Z",
      "2016-01-19T09, 2016-01-19T09:00:00Z",
      "2016-01, 2016-01-01T00:00:00Z",
      "2016, 2016-01-01T00:00:00Z",
      "2016-01-19T09:11:00Z, 2016-01-19T09:11:00Z",
      "2016-01-19T09:11:01Z, 2016-01-19T09:11:01Z",
  })
  public void testParseBest(String string, String timestamp) {
    long expected = Instant.parse(timestamp).getEpochSecond();
    long result = ParameterUtil.parseBest(string).getSeconds();
    assertEquals(result, expected);
  }

  @Test
  public void shouldParseDateHour() {
    final Instant instant = ParameterUtil.parseDateHour("2016-01-19T08");

    assertThat(instant, is(parse("2016-01-19T08:00:00.000Z")));
  }

  @Test
  public void shouldParseDateMonth() {
    final Instant instant = ParameterUtil.parseDateMonth("2016-01");

    assertThat(instant, is(parse("2016-01-01T00:00:00.000Z")));
  }

  @Test
  public void shouldParseDateYear() {
    final Instant instant = ParameterUtil.parseDateYear("2016");

    assertThat(instant, is(parse("2016-01-01T00:00:00.000Z")));
  }

  @Test
  public void shouldParseDate() {
    final Instant instant = ParameterUtil.parseDate("2016-01-19");

    assertThat(instant, is(parse("2016-01-19T00:00:00.000Z")));
  }

  @Test
  public void parseAllExamples() {
    for (ParseExample example : PARSE_EXAMPLES) {
      Instant parsed = parseAlignedInstant(example.toParse(), example.schedule());
      assertThat(
          "Parsing " + example.toParse() + " " + example.schedule(),
          parsed, is(example.expected()));
    }
  }

  @Test
  public void unparsableExamples() {
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

  private static ParseExample example(String toParse, Schedule schedule, Instant expected) {
    return new AutoValue_ParameterUtilTest_ParseExample(toParse, schedule, expected);
  }

  private static ParseExample example(String toParse, Schedule schedule) {
    return new AutoValue_ParameterUtilTest_ParseExample(toParse, schedule, null);
  }

  @AutoValue
  static abstract class ParseExample {
    abstract String toParse();
    abstract Schedule schedule();
    @Nullable abstract Instant expected();
  }
}
