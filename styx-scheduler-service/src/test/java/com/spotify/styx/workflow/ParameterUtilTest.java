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

import static com.spotify.styx.workflow.ParameterUtil.decrementInstant;
import static com.spotify.styx.workflow.ParameterUtil.incrementInstant;
import static com.spotify.styx.workflow.ParameterUtil.truncateInstant;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.styx.model.Partitioning;
import java.time.Instant;
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
  public void shouldDecrementInstant() throws Exception {
    final Instant time = Instant.parse("2016-01-19T08:11:22.333Z");
    final Instant timeMinusDay = Instant.parse("2016-01-18T09:11:22.333Z");
    final Instant timeMinusWeek = Instant.parse("2016-01-12T09:11:22.333Z");
    final Instant timeMinusMonth = Instant.parse("2015-12-19T09:11:22.333Z");

    final Instant hour = decrementInstant(TIME, Partitioning.HOURS);
    assertThat(hour, is(time));

    final Instant day = decrementInstant(TIME, Partitioning.DAYS);
    assertThat(day, is(timeMinusDay));

    final Instant week = decrementInstant(TIME, Partitioning.WEEKS);
    assertThat(week, is(timeMinusWeek));

    final Instant months = decrementInstant(TIME, Partitioning.MONTHS);
    assertThat(months, is(timeMinusMonth));
  }

  @Test
  public void shouldIncrementInstant() throws Exception {
    final Instant timePlusHour = Instant.parse("2016-01-19T10:11:22.333Z");
    final Instant timePlusDay = Instant.parse("2016-01-20T09:11:22.333Z");
    final Instant timePlusWeek = Instant.parse("2016-01-26T09:11:22.333Z");
    final Instant timePlusMonth = Instant.parse("2016-02-19T09:11:22.333Z");

    final Instant hour = incrementInstant(TIME, Partitioning.HOURS);
    assertThat(hour, is(timePlusHour));

    final Instant day = incrementInstant(TIME, Partitioning.DAYS);
    assertThat(day, is(timePlusDay));

    final Instant week = incrementInstant(TIME, Partitioning.WEEKS);
    assertThat(week, is(timePlusWeek));

    final Instant month = incrementInstant(TIME, Partitioning.MONTHS);
    assertThat(month, is(timePlusMonth));
  }

  @Test
  public void shouldTruncateInstant() throws Exception {
    final Instant truncatedTimeHours = Instant.parse("2016-01-19T09:00:00.00Z");
    final Instant truncatedTimeDays = Instant.parse("2016-01-19T00:00:00.00Z");
    final Instant truncatedTimeWeeks = Instant.parse("2016-01-18T00:00:00.00Z");
    final Instant truncatedTimeMonths = Instant.parse("2016-01-01T00:00:00.00Z");

    final Instant hour = truncateInstant(TIME, Partitioning.HOURS);
    assertThat(hour, is(truncatedTimeHours));

    final Instant day = truncateInstant(TIME, Partitioning.DAYS);
    assertThat(day, is(truncatedTimeDays));

    final Instant weeks = truncateInstant(TIME, Partitioning.WEEKS);
    assertThat(weeks, is(truncatedTimeWeeks));

    final Instant months = truncateInstant(TIME, Partitioning.MONTHS);
    assertThat(months, is(truncatedTimeMonths));
  }
}
