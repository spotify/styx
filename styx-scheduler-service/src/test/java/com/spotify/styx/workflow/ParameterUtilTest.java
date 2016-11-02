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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

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
}
