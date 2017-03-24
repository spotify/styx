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

import static com.spotify.styx.util.ParameterUtil.decrementInstant;
import static com.spotify.styx.util.ParameterUtil.incrementInstant;
import static com.spotify.styx.util.ParameterUtil.rangeOfInstants;
import static com.spotify.styx.util.ParameterUtil.truncateInstant;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.styx.model.Schedule;
import com.spotify.styx.util.ParameterUtil;
import java.time.Instant;
import java.util.List;
import org.junit.Test;

public class ParameterUtilTest {

  private static final Instant TIME = Instant.parse("2016-01-19T09:11:22.333Z");

  @Test
  public void shouldFormatDate() throws Exception {
    final String date = ParameterUtil.formatDate(TIME);

    assertThat(date, is("2016-01-19"));
  }

  @Test
  public void shouldFormatDateTime() throws Exception {
    final String dateTime = ParameterUtil.formatDateTime(TIME);

    assertThat(dateTime, is("2016-01-19T09:11:22.333Z"));
  }

  @Test
  public void shouldFormatDateHour() throws Exception {
    final String dateHour = ParameterUtil.formatDateHour(TIME);

    assertThat(dateHour, is("2016-01-19T09"));
  }

  @Test
  public void shouldFormatMonth() throws Exception {
    final String month = ParameterUtil.formatMonth(TIME);

    assertThat(month, is("2016-01"));
  }

  @Test
  public void shouldParseDateHour() throws Exception {
    final Instant instant = ParameterUtil.parseDateHour("2016-01-19T08");

    assertThat(instant, is(Instant.parse("2016-01-19T08:00:00.000Z")));
  }

  @Test
  public void shouldParseDate() throws Exception {
    final Instant instant = ParameterUtil.parseDate("2016-01-19");

    assertThat(instant, is(Instant.parse("2016-01-19T00:00:00.000Z")));
  }

  @Test
  public void shouldDecrementInstant() throws Exception {
    final Instant time = Instant.parse("2016-01-19T08:11:22.333Z");
    final Instant timeMinusDay = Instant.parse("2016-01-18T09:11:22.333Z");
    final Instant timeMinusWeek = Instant.parse("2016-01-12T09:11:22.333Z");
    final Instant timeMinusMonth = Instant.parse("2015-12-19T09:11:22.333Z");

    final Instant hour = decrementInstant(TIME, Schedule.HOURS);
    assertThat(hour, is(time));

    final Instant day = decrementInstant(TIME, Schedule.DAYS);
    assertThat(day, is(timeMinusDay));

    final Instant week = decrementInstant(TIME, Schedule.WEEKS);
    assertThat(week, is(timeMinusWeek));

    final Instant months = decrementInstant(TIME, Schedule.MONTHS);
    assertThat(months, is(timeMinusMonth));
  }

  @Test
  public void shouldIncrementInstant() throws Exception {
    final Instant timePlusHour = Instant.parse("2016-01-19T10:11:22.333Z");
    final Instant timePlusDay = Instant.parse("2016-01-20T09:11:22.333Z");
    final Instant timePlusWeek = Instant.parse("2016-01-26T09:11:22.333Z");
    final Instant timePlusMonth = Instant.parse("2016-02-19T09:11:22.333Z");

    final Instant hour = incrementInstant(TIME, Schedule.HOURS);
    assertThat(hour, is(timePlusHour));

    final Instant day = incrementInstant(TIME, Schedule.DAYS);
    assertThat(day, is(timePlusDay));

    final Instant week = incrementInstant(TIME, Schedule.WEEKS);
    assertThat(week, is(timePlusWeek));

    final Instant month = incrementInstant(TIME, Schedule.MONTHS);
    assertThat(month, is(timePlusMonth));
  }

  @Test
  public void shouldTruncateInstant() throws Exception {
    final Instant truncatedTimeHours = Instant.parse("2016-01-19T09:00:00.00Z");
    final Instant truncatedTimeDays = Instant.parse("2016-01-19T00:00:00.00Z");
    final Instant truncatedTimeWeeks = Instant.parse("2016-01-18T00:00:00.00Z");
    final Instant truncatedTimeMonths = Instant.parse("2016-01-01T00:00:00.00Z");

    final Instant hour = truncateInstant(TIME, Schedule.HOURS);
    assertThat(hour, is(truncatedTimeHours));

    final Instant day = truncateInstant(TIME, Schedule.DAYS);
    assertThat(day, is(truncatedTimeDays));

    final Instant weeks = truncateInstant(TIME, Schedule.WEEKS);
    assertThat(weeks, is(truncatedTimeWeeks));

    final Instant months = truncateInstant(TIME, Schedule.MONTHS);
    assertThat(months, is(truncatedTimeMonths));
  }

  @Test
  public void shouldRangeOfInstantsHours() throws Exception {
    final Instant startInstant = Instant.parse("2016-12-31T23:00:00.00Z");
    final Instant endInstant = Instant.parse("2017-01-01T01:00:00.01Z");

    List<Instant> list = rangeOfInstants(startInstant, endInstant, Schedule.HOURS);
    assertThat(list, contains(
        Instant.parse("2016-12-31T23:00:00.00Z"),
        Instant.parse("2017-01-01T00:00:00.00Z"),
        Instant.parse("2017-01-01T01:00:00.00Z"))
    );
  }

  @Test
  public void shouldRangeOfInstantsDays() throws Exception {
    final Instant startInstant = Instant.parse("2016-12-31T00:00:00.00Z");
    final Instant endInstant = Instant.parse("2017-01-02T00:00:00.01Z");

    List<Instant> list = rangeOfInstants(startInstant, endInstant, Schedule.DAYS);
    assertThat(list, contains(
        Instant.parse("2016-12-31T00:00:00.00Z"),
        Instant.parse("2017-01-01T00:00:00.00Z"),
        Instant.parse("2017-01-02T00:00:00.00Z"))
    );
  }

  @Test
  public void shouldRangeOfInstantsWeeks() throws Exception {
    final Instant startInstant = Instant.parse("2016-12-30T00:00:00.00Z");
    final Instant endInstant = Instant.parse("2017-01-13T00:00:00.01Z");

    List<Instant> list = rangeOfInstants(startInstant, endInstant, Schedule.WEEKS);
    assertThat(list, contains(
        Instant.parse("2016-12-30T00:00:00.00Z"),
        Instant.parse("2017-01-06T00:00:00.00Z"),
        Instant.parse("2017-01-13T00:00:00.00Z"))
    );
  }

  @Test
  public void shouldRangeOfInstantsMonths() throws Exception {
    final Instant startInstant = Instant.parse("2017-01-31T00:00:00.00Z");
    final Instant endInstant = Instant.parse("2017-03-28T00:00:00.01Z");

    List<Instant> list = rangeOfInstants(startInstant, endInstant, Schedule.MONTHS);
    assertThat(list, contains(
        Instant.parse("2017-01-31T00:00:00.00Z"),
        Instant.parse("2017-02-28T00:00:00.00Z"),
        Instant.parse("2017-03-28T00:00:00.00Z"))
    );
  }

  @Test
  public void shouldReturnEmptyListStartEqualsEnd() throws Exception {
    final Instant startInstant = Instant.parse("2016-12-31T23:00:00.00Z");
    final Instant endInstant = Instant.parse("2016-12-31T23:00:00.00Z");

    List<Instant> list = rangeOfInstants(startInstant, endInstant, Schedule.HOURS);
    assertThat(list, hasSize(0));
  }

  @Test(expected=IllegalArgumentException.class)
  public void shouldRaiseRangeOfInstantsStartAfterEnd() throws Exception {
    final Instant startInstant = Instant.parse("2016-12-31T23:00:00.00Z");
    final Instant endInstant = Instant.parse("2016-01-01T01:00:00.00Z");

    rangeOfInstants(startInstant, endInstant, Schedule.HOURS);
  }
}
