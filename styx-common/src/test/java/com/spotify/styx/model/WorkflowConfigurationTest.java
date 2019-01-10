/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

package com.spotify.styx.model;

import static com.spotify.styx.testdata.TestData.DAILY_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.HOURLY_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.HOURLY_WORKFLOW_CONFIGURATION_WITH_VALID_OFFSET;
import static com.spotify.styx.testdata.TestData.MONTHLY_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.WEEKLY_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.YEARLY_WORKFLOW_CONFIGURATION;
import static java.time.temporal.ChronoUnit.DAYS;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.time.temporal.ChronoUnit.MINUTES;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.time.Instant;
import org.junit.Test;

public class WorkflowConfigurationTest {

  private static final Instant NOW = Instant.now();

  @Test
  public void shouldAddOffset() {
    assertThat(HOURLY_WORKFLOW_CONFIGURATION_WITH_VALID_OFFSET.addOffset(NOW),
        is(NOW.plus(30, DAYS).plus(30, MINUTES)));
  }

  @Test
  public void shouldAddDefaultOffset() {
    assertThat(HOURLY_WORKFLOW_CONFIGURATION.addOffset(NOW),
        is(NOW.plus(1, HOURS)));
  }

  @Test
  public void shouldSubtractOffset() {
    assertThat(HOURLY_WORKFLOW_CONFIGURATION_WITH_VALID_OFFSET.subtractOffset(NOW),
        is(NOW.minus(30, DAYS).minus(30, MINUTES)));
  }

  @Test
  public void shouldSubtractDefaultOffset() {
    assertThat(HOURLY_WORKFLOW_CONFIGURATION.subtractOffset(NOW),
        is(NOW.minus(1, HOURS)));
  }

  @Test
  public void shouldReturnDefaultOffset() {
    assertThat(HOURLY_WORKFLOW_CONFIGURATION.defaultOffset(), is("PT1H"));
    assertThat(DAILY_WORKFLOW_CONFIGURATION.defaultOffset(), is("P1D"));
    assertThat(WEEKLY_WORKFLOW_CONFIGURATION.defaultOffset(), is("P1W"));
    assertThat(MONTHLY_WORKFLOW_CONFIGURATION.defaultOffset(), is("P1M"));
    assertThat(YEARLY_WORKFLOW_CONFIGURATION.defaultOffset(), is("P1Y"));
    assertThat(WorkflowConfigurationBuilder.from(HOURLY_WORKFLOW_CONFIGURATION)
            .schedule(Schedule.parse("45 23 * * 6"))
            .build()
            .defaultOffset(),
        is("PT0S"));
  }
}