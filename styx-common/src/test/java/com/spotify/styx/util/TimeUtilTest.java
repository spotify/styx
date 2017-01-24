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
import static com.spotify.styx.util.TimeUtil.lastInstant;
import static com.spotify.styx.util.TimeUtil.nextInstant;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.styx.model.Schedule;
import java.time.Instant;
import java.time.ZonedDateTime;
import org.junit.Test;

public class TimeUtilTest {

  private static final Instant TIME = Instant.parse("2016-01-19T09:11:22.333Z");

  @Test
  public void shouldGetLastInstant() throws Exception {
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
  public void shouldGetNextInstant() throws Exception {
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

  @Test
  public void shouldAddOffset() throws Exception {
    String offset = "P1M3DT1H7M5S";
    ZonedDateTime time = ZonedDateTime.parse("2017-01-22T08:07:11.22Z");
    ZonedDateTime offsetTime = addOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-02-25T09:14:16.22Z")));
  }

  @Test
  public void shouldAddOffsetWithNoPeriod() throws Exception {
    String offset = "PT1H7M5S";
    ZonedDateTime time = ZonedDateTime.parse("2017-01-22T08:07:11.22Z");
    ZonedDateTime offsetTime = addOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-01-22T09:14:16.22Z")));
  }

  @Test
  public void shouldAddOffsetWithNoTime() throws Exception {
    String offset = "P1M3D";
    ZonedDateTime time = ZonedDateTime.parse("2017-01-22T08:07:11.22Z");
    ZonedDateTime offsetTime = addOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-02-25T08:07:11.22Z")));
  }

  @Test
  public void shouldAddOffsetWithWeek() throws Exception {
    String offset = "P2W";
    ZonedDateTime time = ZonedDateTime.parse("2017-01-22T08:07:11.22Z");
    ZonedDateTime offsetTime = addOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-02-05T08:07:11.22Z")));
  }
}
