/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

import static com.spotify.styx.util.TimeUtil.addOffset;
import static com.spotify.styx.util.TimeUtil.isAligned;
import static com.spotify.styx.util.TimeUtil.lastInstant;
import static com.spotify.styx.util.TimeUtil.nextInstant;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.spotify.styx.model.Schedule;
import java.time.Instant;
import java.time.ZonedDateTime;
import org.junit.Test;

public class TimeUtilTest {

  private static final Instant TIME = Instant.parse("2016-01-19T09:11:22.333Z");

  @Test
  public void shouldGetLastInstant() {
    final Instant lastTimeHours = Instant.parse("2016-01-19T09:00:00.00Z");
    final Instant lastTimeDays = Instant.parse("2016-01-19T00:00:00.00Z");
    final Instant lastTimeWeeks = Instant.parse("2016-01-18T00:00:00.00Z");
    final Instant lastTimeMonths = Instant.parse("2016-01-01T00:00:00.00Z");

    final Instant hour = lastInstant(TIME, Schedule.HOURS);
    assertThat(hour, is(lastTimeHours));

    final Instant day = lastInstant(TIME, Schedule.DAYS);
    assertThat(day, is(lastTimeDays));

    final Instant weeks = lastInstant(TIME, Schedule.WEEKS);
    assertThat(weeks, is(lastTimeWeeks));

    final Instant months = lastInstant(TIME, Schedule.MONTHS);
    assertThat(months, is(lastTimeMonths));
  }

  @Test
  public void shouldWorkForComplexCron() {
    final Instant lastInstant = lastInstant(Instant.parse("2016-01-19T09:00:00.00Z"),
                                            Schedule.parse("5-59/20 * * * *"));
    assertThat(lastInstant, is(Instant.parse("2016-01-19T08:45:00.00Z")));

    final Instant nextInstance = nextInstant(Instant.parse("2016-01-19T09:00:00.00Z"),
                                             Schedule.parse("5-59/20 * * * *"));
    assertThat(nextInstance, is(Instant.parse("2016-01-19T09:05:00.00Z")));
  }

  @Test
  public void shouldReturnLastInstantUnchangedIfMatchingTime() {
    final Instant lastTimeHours = Instant.parse("2016-01-19T09:00:00.00Z");
    final Instant lastTimeDays = Instant.parse("2016-01-19T00:00:00.00Z");
    final Instant lastTimeWeeks = Instant.parse("2016-01-18T00:00:00.00Z");
    final Instant lastTimeMonths = Instant.parse("2016-01-01T00:00:00.00Z");

    final Instant hour = lastInstant(lastTimeHours, Schedule.HOURS);
    assertThat(hour, is(lastTimeHours));

    final Instant day = lastInstant(lastTimeDays, Schedule.DAYS);
    assertThat(day, is(lastTimeDays));

    final Instant weeks = lastInstant(lastTimeWeeks, Schedule.WEEKS);
    assertThat(weeks, is(lastTimeWeeks));

    final Instant months = lastInstant(lastTimeMonths, Schedule.MONTHS);
    assertThat(months, is(lastTimeMonths));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailIfNoLastInstance() {
    lastInstant(Instant.parse("2016-01-19T09:00:00.00Z"),
                Schedule.parse("* * * * * 2017"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailIfNoPreviousInstance() {
    lastInstant(Instant.parse("2016-01-19T09:00:00.00Z"),
                Schedule.parse("* * * * * 2017"));
  }

  @Test
  public void shouldGetNextInstant() {
    final Instant nextTimeHours = Instant.parse("2016-01-19T10:00:00.00Z");
    final Instant nextTimeDays = Instant.parse("2016-01-20T00:00:00.00Z");
    final Instant nextTimeWeeks = Instant.parse("2016-01-25T00:00:00.00Z");
    final Instant nextTimeMonths = Instant.parse("2016-02-01T00:00:00.00Z");

    final Instant hour = nextInstant(TIME, Schedule.HOURS);
    assertThat(hour, is(nextTimeHours));

    final Instant day = nextInstant(TIME, Schedule.DAYS);
    assertThat(day, is(nextTimeDays));

    final Instant weeks = nextInstant(TIME, Schedule.WEEKS);
    assertThat(weeks, is(nextTimeWeeks));

    final Instant months = nextInstant(TIME, Schedule.MONTHS);
    assertThat(months, is(nextTimeMonths));
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldFailIfNoNextInstance() {
    lastInstant(Instant.parse("2018-01-19T09:00:00.00Z"),
                Schedule.parse("* * * * * 2017"));
  }

  @Test
  public void shouldTestWellKnownAlignedInstants() {
    assertTrue(isAligned(Instant.parse("2017-02-06T10:00:00.00Z"), Schedule.HOURS));
    assertTrue(isAligned(Instant.parse("2017-02-06T00:00:00.00Z"), Schedule.DAYS));
    assertTrue(isAligned(Instant.parse("2017-02-06T00:00:00.00Z"), Schedule.WEEKS));
    assertTrue(isAligned(Instant.parse("2017-02-01T00:00:00.00Z"), Schedule.MONTHS));
    assertTrue(isAligned(Instant.parse("2017-01-01T00:00:00.00Z"), Schedule.YEARS));

    assertFalse(isAligned(Instant.parse("2017-02-06T10:01:00.00Z"), Schedule.HOURS));
    assertFalse(isAligned(Instant.parse("2017-02-06T01:00:00.00Z"), Schedule.DAYS));
    assertFalse(isAligned(Instant.parse("2017-02-07T00:00:00.00Z"), Schedule.WEEKS));
    assertFalse(isAligned(Instant.parse("2017-02-02T00:00:00.00Z"), Schedule.MONTHS));
    assertFalse(isAligned(Instant.parse("2017-01-02T00:00:00.00Z"), Schedule.YEARS));
  }

  @Test
  public void shouldTestCustomAlignedInstants() {
    Schedule custom = Schedule.parse("15,42 10 * * *");
    assertTrue(isAligned(Instant.parse("2017-02-06T10:15:00.00Z"), custom));
    assertTrue(isAligned(Instant.parse("2017-02-06T10:42:00.00Z"), custom));
    assertFalse(isAligned(Instant.parse("2017-02-06T10:00:00.00Z"), custom));
    assertFalse(isAligned(Instant.parse("2017-02-06T11:15:00.00Z"), custom));
  }

  @Test
  public void shouldSupportZeroOffset() {
    String offset = "PT0S";
    ZonedDateTime time = ZonedDateTime.parse("2017-01-22T08:07:11.22Z");
    ZonedDateTime offsetTime = addOffset(time, offset);

    assertThat(offsetTime, is(time));
  }

  @Test
  public void shouldAddOffset() {
    String offset = "P1M3DT1H7M5S";
    ZonedDateTime time = ZonedDateTime.parse("2017-01-22T08:07:11.22Z");
    ZonedDateTime offsetTime = addOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-02-25T09:14:16.22Z")));
  }

  @Test
  public void shouldAddOffsetWithNoPeriod() {
    String offset = "PT1H7M5S";
    ZonedDateTime time = ZonedDateTime.parse("2017-01-22T08:07:11.22Z");
    ZonedDateTime offsetTime = addOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-01-22T09:14:16.22Z")));
  }

  @Test
  public void shouldAddOffsetWithNoTime() {
    String offset = "P1M3D";
    ZonedDateTime time = ZonedDateTime.parse("2017-01-22T08:07:11.22Z");
    ZonedDateTime offsetTime = addOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-02-25T08:07:11.22Z")));
  }

  @Test
  public void shouldAddOffsetWithWeek() {
    String offset = "P2W";
    ZonedDateTime time = ZonedDateTime.parse("2017-01-22T08:07:11.22Z");
    ZonedDateTime offsetTime = addOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-02-05T08:07:11.22Z")));
  }

  @Test
  public void cronScheduleShouldNotEqualEquivalentWellKnownSchedule() {
    assertFalse(Schedule.parse("0 * * * *").equals(Schedule.HOURS));
  }
}
