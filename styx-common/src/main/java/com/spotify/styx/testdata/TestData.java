/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.testdata;

import static com.spotify.styx.model.Partitioning.DAYS;
import static com.spotify.styx.model.Partitioning.HOURS;
import static com.spotify.styx.model.Partitioning.MONTHS;
import static com.spotify.styx.model.Partitioning.WEEKS;
import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.spotify.styx.model.DataEndpoint;
import com.spotify.styx.model.DataEndpoint.Secret;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import java.net.URI;

public final class TestData {

  private TestData() {
  }

  public static final URI WORKFLOW_URI =
      URI.create("http://example.com/foo/bar");

  public static final WorkflowId WORKFLOW_ID =
      WorkflowId.create("styx", "styx.TestEndpoint");

  public static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(WORKFLOW_ID, "2016-09-01");

  public static final DataEndpoint HOURLY_DATA_ENDPOINT =
      DataEndpoint.create(
          "styx.TestEndpoint", HOURS, empty(), empty(), empty());

  public static final DataEndpoint DAILY_DATA_ENDPOINT =
      DataEndpoint.create(
          "styx.TestEndpoint", DAYS, empty(), empty(), empty());

  public static final DataEndpoint WEEKLY_DATA_ENDPOINT =
      DataEndpoint.create(
          "styx.TestEndpoint", WEEKS, empty(), empty(), empty());

  public static final DataEndpoint MONTHLY_DATA_ENDPOINT =
      DataEndpoint.create(
          "styx.TestEndpoint", MONTHS, empty(), empty(), empty());

  public static final DataEndpoint FULL_DATA_ENDPOINT =
      DataEndpoint.create(
          "styx.TestEndpoint", DAYS, of("busybox"), of(asList("x", "y")),
          of(Secret.create("name", "/path")));
}
