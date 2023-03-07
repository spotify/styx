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
import static com.spotify.styx.util.TimeUtil.instantsInRange;
import static com.spotify.styx.util.TimeUtil.instantsInReversedRange;
import static com.spotify.styx.util.TimeUtil.isAligned;
import static com.spotify.styx.util.TimeUtil.lastInstant;
import static com.spotify.styx.util.TimeUtil.nextInstant;
import static com.spotify.styx.util.TimeUtil.offsetInstant;
import static com.spotify.styx.util.TimeUtil.subtractOffset;
import static java.time.Instant.parse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.spotify.styx.model.Schedule;
import java.time.Instant;
import java.time.ZonedDateTime;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class TimeUtilTest {

  private static final Instant TIME = parse("2016-01-19T09:11:22.333Z");
  private static final Schedule EVERY_5_MINUTES = Schedule.parse("*/5 * * * *");

  @Rule
  public ExpectedException expect = ExpectedException.none();

  @Test
  public void shouldGetLastInstant() {
    final Instant lastTimeHours = parse("2016-01-19T09:00:00.00Z");
    final Instant lastTimeDays = parse("2016-01-19T00:00:00.00Z");
    final Instant lastTimeWeeks = parse("2016-01-18T00:00:00.00Z");
    final Instant lastTimeMonths = parse("2016-01-01T00:00:00.00Z");

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
    final Instant lastInstant = lastInstant(parse("2016-01-19T09:00:00.00Z"),
                                            Schedule.parse("5-59/20 * * * *"));
    assertThat(lastInstant, is(parse("2016-01-19T08:45:00.00Z")));

    final Instant nextInstance = nextInstant(parse("2016-01-19T09:00:00.00Z"),
                                             Schedule.parse("05-59/20 * * * *"));
    assertThat(nextInstance, is(parse("2016-01-19T09:05:00.00Z")));
  }

  @Test
  public void shouldReturnLastInstantUnchangedIfMatchingTime() {
    final Instant lastTimeHours = parse("2016-01-19T09:00:00.00Z");
    final Instant lastTimeDays = parse("2016-01-19T00:00:00.00Z");
    final Instant lastTimeWeeks = parse("2016-01-18T00:00:00.00Z");
    final Instant lastTimeMonths = parse("2016-01-01T00:00:00.00Z");

    final Instant hour = lastInstant(lastTimeHours, Schedule.HOURS);
    assertThat(hour, is(lastTimeHours));

    final Instant day = lastInstant(lastTimeDays, Schedule.DAYS);
    assertThat(day, is(lastTimeDays));

    final Instant weeks = lastInstant(lastTimeWeeks, Schedule.WEEKS);
    assertThat(weeks, is(lastTimeWeeks));

    final Instant months = lastInstant(lastTimeMonths, Schedule.MONTHS);
    assertThat(months, is(lastTimeMonths));
  }

  @Test
  public void shouldGetNextInstant() {
    final Instant nextTimeHours = parse("2016-01-19T10:00:00.00Z");
    final Instant nextTimeDays = parse("2016-01-20T00:00:00.00Z");
    final Instant nextTimeWeeks = parse("2016-01-25T00:00:00.00Z");
    final Instant nextTimeMonths = parse("2016-02-01T00:00:00.00Z");

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
    lastInstant(parse("2018-01-19T09:00:00.00Z"),
                Schedule.parse("* * * * * 2017"));
  }

  @Test
  public void shouldTestWellKnownAlignedInstants() {
    assertTrue(isAligned(parse("2017-02-06T10:00:00.00Z"), Schedule.HOURS));
    assertTrue(isAligned(parse("2017-02-06T00:00:00.00Z"), Schedule.DAYS));
    assertTrue(isAligned(parse("2017-02-06T00:00:00.00Z"), Schedule.WEEKS));
    assertTrue(isAligned(parse("2017-02-01T00:00:00.00Z"), Schedule.MONTHS));
    assertTrue(isAligned(parse("2017-01-01T00:00:00.00Z"), Schedule.YEARS));

    assertFalse(isAligned(parse("2017-02-06T10:01:00.00Z"), Schedule.HOURS));
    assertFalse(isAligned(parse("2017-02-06T01:00:00.00Z"), Schedule.DAYS));
    assertFalse(isAligned(parse("2017-02-07T00:00:00.00Z"), Schedule.WEEKS));
    assertFalse(isAligned(parse("2017-02-02T00:00:00.00Z"), Schedule.MONTHS));
    assertFalse(isAligned(parse("2017-01-02T00:00:00.00Z"), Schedule.YEARS));
  }

  @Test
  public void shouldTestCustomAlignedInstants() {
    Schedule custom = Schedule.parse("15,42 10 * * *");
    assertTrue(isAligned(parse("2017-02-06T10:15:00.00Z"), custom));
    assertTrue(isAligned(parse("2017-02-06T10:42:00.00Z"), custom));
    assertFalse(isAligned(parse("2017-02-06T10:00:00.00Z"), custom));
    assertFalse(isAligned(parse("2017-02-06T11:15:00.00Z"), custom));
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
  public void shouldAddNegativeOffset() {
    String offset = "-P1M3DT1H7M5S";
    ZonedDateTime time = ZonedDateTime.parse("2017-02-25T09:14:16.22Z");
    ZonedDateTime offsetTime = addOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-01-22T08:07:11.22Z")));
  }

  @Test
  public void shouldAddNegativeOffsetWithNoPeriod() {
    String offset = "-PT1H7M5S";
    ZonedDateTime time = ZonedDateTime.parse("2017-01-22T09:14:16.22Z");
    ZonedDateTime offsetTime = addOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-01-22T08:07:11.22Z")));
  }

  @Test
  public void shouldSubtractPositiveOffset() {
    String offset = "P1M3DT1H7M5S";
    ZonedDateTime time = ZonedDateTime.parse("2017-02-25T09:14:16.22Z");
    ZonedDateTime offsetTime = subtractOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-01-22T08:07:11.22Z")));
  }

  @Test
  public void shouldSubtractExplicitlyPositiveOffset() {
    String offset = "+P1M3DT1H7M5S";
    ZonedDateTime time = ZonedDateTime.parse("2017-02-25T09:14:16.22Z");
    ZonedDateTime offsetTime = subtractOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-01-22T08:07:11.22Z")));
  }

  @Test
  public void shouldSubtractNegativeOffset() {
    String offset = "-P1M3DT1H7M5S";
    ZonedDateTime time = ZonedDateTime.parse("2017-02-25T09:14:16.22Z");
    ZonedDateTime offsetTime = subtractOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-03-28T10:21:21.22Z")));
  }

  @Test
  public void shouldSubtractOffsetWithNoTime() {
    String offset = "P1M3D";
    ZonedDateTime time = ZonedDateTime.parse("2017-02-25T08:07:11.22Z");
    ZonedDateTime offsetTime = subtractOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-01-22T08:07:11.22Z")));
  }

  @Test
  public void shouldSubtractOffsetWithWeek() {
    String offset = "P2W";
    ZonedDateTime time = ZonedDateTime.parse("2017-02-05T08:07:11.22Z");
    ZonedDateTime offsetTime = subtractOffset(time, offset);

    assertThat(offsetTime, is(ZonedDateTime.parse("2017-01-22T08:07:11.22Z")));
  }

  @Test
  public void cronScheduleShouldNotEqualEquivalentWellKnownSchedule() {
    assertNotEquals(Schedule.parse("0 * * * *"), Schedule.HOURS);
  }

  @Test
  public void shouldGetCorrectInstants() {
    final Instant firstTimeHours = parse("2016-01-19T00:00:00.00Z");
    final Instant lastTimeHours = parse("2016-01-19T03:00:00.00Z");
    final Instant firstTimeDays = parse("2016-01-10T00:00:00.00Z");
    final Instant lastTimeDays = parse("2016-01-13T00:00:00.00Z");
    final Instant firstTimeWeeks = parse("2016-01-11T00:00:00.00Z");
    final Instant lastTimeWeeks = parse("2016-01-18T00:00:00.00Z");
    final Instant firstTimeMonths = parse("2010-01-01T00:00:00.00Z");
    final Instant lastTimeMonths = parse("2013-01-01T00:00:00.00Z");

    assertThat(instantsInRange(firstTimeHours, lastTimeHours, Schedule.HOURS),
        contains(parse("2016-01-19T00:00:00.00Z"),
            parse("2016-01-19T01:00:00.00Z"),
            parse("2016-01-19T02:00:00.00Z")));

    assertThat(instantsInRange(firstTimeDays, lastTimeDays, Schedule.DAYS),
        contains(parse("2016-01-10T00:00:00.00Z"),
            parse("2016-01-11T00:00:00.00Z"),
            parse("2016-01-12T00:00:00.00Z")));

    assertThat(instantsInRange(firstTimeWeeks, lastTimeWeeks, Schedule.WEEKS),
        contains(parse("2016-01-11T00:00:00.00Z")));

    assertThat(instantsInRange(firstTimeMonths, lastTimeMonths, Schedule.YEARS),
        contains(parse("2010-01-01T00:00:00.00Z"),
            parse("2011-01-01T00:00:00.00Z"),
            parse("2012-01-01T00:00:00.00Z")));
  }

  @Test
  public void shouldGetCorrectInstantsForCron() {
    final Instant firstTimeHours = parse("2016-01-19T00:00:00.00Z");
    final Instant lastTimeHours = parse("2016-01-19T03:00:00.00Z");

    assertThat(instantsInRange(firstTimeHours, lastTimeHours, Schedule.parse("0 * * * *")),
        contains(parse("2016-01-19T00:00:00.00Z"),
            parse("2016-01-19T01:00:00.00Z"),
            parse("2016-01-19T02:00:00.00Z")));
  }

  @Test
  public void shouldReturnEmptyList() {
    final Instant firstTimeHours = parse("2016-01-19T00:00:00.00Z");
    final Instant lastTimeHours = parse("2016-01-19T00:00:00.00Z");

    assertTrue(
        instantsInRange(firstTimeHours, lastTimeHours, Schedule.parse("0 * * * *")).isEmpty());
  }

  @Test
  public void shouldGetExceptionIfLastInstantIsBeforeFirstInstant() {
    final Instant firstTimeHours = parse("2016-01-19T10:00:00.00Z");
    final Instant lastTimeHours = parse("2016-01-19T09:00:00.00Z");

    expect.expect(IllegalArgumentException.class);
    expect.expectMessage("last instant should not be before first instant");

    instantsInRange(firstTimeHours, lastTimeHours, Schedule.HOURS);
  }

  @Test
  public void shouldGetExceptionIfLastInstantIsNotAlignedWithSchedule() {
    final Instant firstTimeHours = parse("2016-01-19T08:00:00.00Z");
    final Instant lastTimeHours = parse("2016-01-19T09:10:00.00Z");

    expect.expect(IllegalArgumentException.class);
    expect.expectMessage("unaligned instant");

    instantsInRange(firstTimeHours, lastTimeHours, Schedule.HOURS);
  }

  @Test
  public void shouldGetExceptionIfFirstInstantIsNotAlignedWithSchedule() {
    final Instant firstTimeHours = parse("2016-01-19T08:10:00.00Z");
    final Instant lastTimeHours = parse("2016-01-19T09:00:00.00Z");

    expect.expect(IllegalArgumentException.class);
    expect.expectMessage("unaligned instant");

    instantsInRange(firstTimeHours, lastTimeHours, Schedule.HOURS);
  }

  @Test
  public void shouldGetCorrectInstantsReversed() {
    final Instant firstTimeHours = parse("2016-01-19T03:00:00.00Z");
    final Instant lastTimeHours = parse("2016-01-19T00:00:00.00Z");
    final Instant firstTimeDays = parse("2016-01-13T00:00:00.00Z");
    final Instant lastTimeDays = parse("2016-01-10T00:00:00.00Z");
    final Instant firstTimeWeeks = parse("2016-01-18T00:00:00.00Z");
    final Instant lastTimeWeeks = parse("2016-01-11T00:00:00.00Z");
    final Instant firstTimeMonths = parse("2013-01-01T00:00:00.00Z");
    final Instant lastTimeMonths = parse("2010-01-01T00:00:00.00Z");

    assertThat(instantsInReversedRange(firstTimeHours, lastTimeHours, Schedule.HOURS),
        contains(parse("2016-01-19T03:00:00.00Z"),
            parse("2016-01-19T02:00:00.00Z"),
            parse("2016-01-19T01:00:00.00Z")));

    assertThat(instantsInReversedRange(firstTimeDays, lastTimeDays, Schedule.DAYS),
        contains(parse("2016-01-13T00:00:00.00Z"),
            parse("2016-01-12T00:00:00.00Z"),
            parse("2016-01-11T00:00:00.00Z")));

    assertThat(instantsInReversedRange(firstTimeWeeks, lastTimeWeeks, Schedule.WEEKS),
        contains(parse("2016-01-18T00:00:00.00Z")));

    assertThat(instantsInReversedRange(firstTimeMonths, lastTimeMonths, Schedule.YEARS),
        contains(parse("2013-01-01T00:00:00.00Z"),
            parse("2012-01-01T00:00:00.00Z"),
            parse("2011-01-01T00:00:00.00Z")));
  }

  @Test
  public void shouldGetCorrectInstantsForCronReversed() {
    final Instant firstTimeHours = parse("2016-01-19T03:00:00.00Z");
    final Instant lastTimeHours = parse("2016-01-19T00:00:00.00Z");

    assertThat(instantsInReversedRange(firstTimeHours, lastTimeHours, Schedule.parse("0 * * * *")),
        contains(parse("2016-01-19T03:00:00.00Z"),
            parse("2016-01-19T02:00:00.00Z"),
            parse("2016-01-19T01:00:00.00Z")));
  }

  @Test
  public void shouldReturnEmptyListReversed() {
    final Instant firstTimeHours = parse("2016-01-19T00:00:00.00Z");
    final Instant lastTimeHours = parse("2016-01-19T00:00:00.00Z");

    assertTrue(
        instantsInRange(firstTimeHours, lastTimeHours, Schedule.parse("0 * * * *")).isEmpty());
  }


  @Test
  public void shouldGetExceptionIfLastInstantIsBeforeFirstInstantReversed() {
    final Instant firstTimeHours = parse("2016-01-19T09:00:00.00Z");
    final Instant lastTimeHours = parse("2016-01-19T10:00:00.00Z");

    expect.expect(IllegalArgumentException.class);
    expect.expectMessage("last instant should not be after first instant");

    instantsInReversedRange(firstTimeHours, lastTimeHours, Schedule.HOURS);
  }

  @Test
  public void shouldGetExceptionIfLastInstantIsNotAlignedWithScheduleReversed() {
    final Instant firstTimeHours = parse("2016-01-19T09:00:00.00Z");
    final Instant lastTimeHours = parse("2016-01-19T08:10:00.00Z");

    expect.expect(IllegalArgumentException.class);
    expect.expectMessage("unaligned instant");

    instantsInReversedRange(firstTimeHours, lastTimeHours, Schedule.HOURS);
  }

  @Test
  public void shouldGetExceptionIfFirstInstantIsNotAlignedWithScheduleReversed() {
    final Instant firstTimeHours = parse("2016-01-19T09:10:00.00Z");
    final Instant lastTimeHours = parse("2016-01-19T08:00:00.00Z");

    expect.expect(IllegalArgumentException.class);
    expect.expectMessage("unaligned instant");

    instantsInReversedRange(firstTimeHours, lastTimeHours, Schedule.HOURS);
  }

  @Test
  @Parameters({
      "2018-01-19T09:00:00.00Z, hours, 2018-01-19T07:00:00.00Z",
      "2018-01-19T00:00:00.00Z, days, 2018-01-17T00:00:00.00Z",
      "2018-01-15T00:00:00.00Z, weeks, 2018-01-01T00:00:00Z",
      "2018-01-01T00:00:00.00Z, months, 2017-11-01T00:00:00.00Z",
      "2018-01-01T00:00:00.00Z, years, 2016-01-01T00:00:00.00Z",
  })
  public void shouldGetCorrectInstantWithNegativeOffset(String origin, String schedule,
                                                        String expected) {
    assertThat(offsetInstant(parse(origin), Schedule.parse(schedule), -2), is(parse(expected)));
  }

  @Test
  @Parameters({
      "2018-01-19T09:00:00.00Z, hours, 2018-01-19T11:00:00.00Z",
      "2018-01-19T00:00:00.00Z, days, 2018-01-21T00:00:00.00Z",
      "2018-01-15T00:00:00.00Z, weeks, 2018-01-29T00:00:00Z",
      "2018-01-01T00:00:00.00Z, months, 2018-03-01T00:00:00.00Z",
      "2018-01-01T00:00:00.00Z, years, 2020-01-01T00:00:00.00Z",
  })
  public void shouldGetCorrectInstantWithPositiveOffset(String origin, String schedule,
                                                        String expected) {
    assertThat(offsetInstant(parse(origin), Schedule.parse(schedule), 2), is(parse(expected)));
  }

  @Test
  @Parameters({
      "2018-01-19T09:00:00.00Z, hours, 2018-01-19T09:00:00.00Z",
      "2018-01-19T00:00:00.00Z, days, 2018-01-19T00:00:00.00Z",
      "2018-01-15T00:00:00.00Z, weeks, 2018-01-15T00:00:00Z",
      "2018-01-01T00:00:00.00Z, months, 2018-01-01T00:00:00.00Z",
      "2018-01-01T00:00:00.00Z, years, 2018-01-01T00:00:00.00Z",
  })
  public void shouldGetCorrectInstantWithZeroOffset(String origin, String schedule,
                                                    String expected) {
    assertThat(offsetInstant(parse(origin), Schedule.parse(schedule), 0), is(parse(expected)));
  }

  @Test
  public void shouldGetCorrectInstantWithNegativeOffsetForCronSchedule() {
    assertThat(offsetInstant(parse("2018-01-19T09:00:00.00Z"), EVERY_5_MINUTES, -2),
        is(parse("2018-01-19T08:50:00.00Z")));
  }

  @Test
  public void shouldGetCorrectInstantWithPositiveOffsetForCronSchedule() {
    assertThat(offsetInstant(parse("2018-01-19T09:00:00.00Z"), EVERY_5_MINUTES, 2),
        is(parse("2018-01-19T09:10:00.00Z")));
  }

  @Test
  public void shouldGetCorrectInstantWithZeroOffsetForCronSchedule() {
    assertThat(offsetInstant(parse("2018-01-19T09:00:00.00Z"), EVERY_5_MINUTES, 0),
        is(parse("2018-01-19T09:00:00.00Z")));
  }

  @Test
  public void shouldGetExceptionIfReferenceInstantIsNotAlignedWithSchedule() {
    expect.expect(IllegalArgumentException.class);
    expect.expectMessage("unaligned origin");

    offsetInstant(parse("2016-01-19T09:10:00.00Z"), Schedule.HOURS, 0);
  }

  @Test
  public void shouldNotBeInstantiable() throws ReflectiveOperationException {
    assertThat(ClassEnforcer.assertNotInstantiable(TimeUtil.class), is(true));
  }
}
