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
import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.spotify.styx.model.DataEndpoint;
import com.spotify.styx.model.DataEndpoint.Secret;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import java.net.URI;
import java.util.Arrays;
import java.util.Optional;

public final class TestData {

  private TestData() {
  }

  public static final URI WORKFLOW_URI =
      URI.create("http://example.com/foo/bar");

  public static final WorkflowId WORKFLOW_ID =
      WorkflowId.create("styx", "styx.TestEndpoint");

  public static final WorkflowId WORKFLOW_ID_2 =
      WorkflowId.create("ranic", "ranic");

  public static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(WORKFLOW_ID, "2016-09-01");

  public static final DataEndpoint HOURLY_DATA_ENDPOINT =
      DataEndpoint.create(
          "styx.TestEndpoint", HOURS, empty(), empty(), empty(), empty(), emptyList());

  public static final DataEndpoint DAILY_DATA_ENDPOINT =
      DataEndpoint.create(
          "styx.TestEndpoint", DAYS, empty(), empty(), empty(), empty(), emptyList());

  public static final DataEndpoint WEEKLY_DATA_ENDPOINT =
      DataEndpoint.create(
          "styx.TestEndpoint", WEEKS, empty(), empty(), empty(), empty(), emptyList());

  public static final DataEndpoint MONTHLY_DATA_ENDPOINT =
      DataEndpoint.create(
          "styx.TestEndpoint", MONTHS, empty(), empty(), empty(), empty(), emptyList());

  public static final DataEndpoint FULL_DATA_ENDPOINT =
      DataEndpoint.create(
          "styx.TestEndpoint", DAYS, of("busybox"), empty(), of(asList("x", "y")),
          of(Secret.create("name", "/path")), emptyList());

  public static final ExecutionDescription EXECUTION_DESCRIPTION =
      ExecutionDescription.create(
          "busybox:1.1",
          Arrays.asList("foo", "bar"),
          Optional.of(DataEndpoint.Secret.create("secret", "/dev/null")),
          Optional.of("00000ef508c1cb905e360590ce3e7e9193f6b370"));
}
