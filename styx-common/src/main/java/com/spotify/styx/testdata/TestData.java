/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2017 Spotify AB
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

import static com.spotify.styx.model.Schedule.DAYS;
import static com.spotify.styx.model.Schedule.HOURS;
import static com.spotify.styx.model.Schedule.MONTHS;
import static com.spotify.styx.model.Schedule.WEEKS;
import static java.util.Optional.empty;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfiguration.Secret;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import java.util.Arrays;
import java.util.Optional;

public final class TestData {

  private TestData() {
  }

  public static final WorkflowId WORKFLOW_ID =
      WorkflowId.create("styx", "styx.TestEndpoint");

  public static final WorkflowId WORKFLOW_ID_2 =
      WorkflowId.create("ranic", "ranic");

  public static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(WORKFLOW_ID, "2016-09-01");

  public static final WorkflowConfiguration HOURLY_WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .schedule(HOURS)
          .build();

  public static final WorkflowConfiguration DAILY_WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .schedule(DAYS)
          .build();

  public static final WorkflowConfiguration WEEKLY_WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .schedule(WEEKS)
          .build();

  public static final WorkflowConfiguration MONTHLY_WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .schedule(MONTHS)
          .build();


  public static final WorkflowConfiguration FULL_WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .schedule(DAYS)
          .dockerImage("busybox")
          .dockerArgs(ImmutableList.of("x", "y"))
          .secret(Secret.create("name", "/path"))
          .build();

  public static final ExecutionDescription EXECUTION_DESCRIPTION =
      ExecutionDescription.create(
          "busybox:1.1",
          Arrays.asList("foo", "bar"),
          false,
          Optional.of(WorkflowConfiguration.Secret.create("secret", "/dev/null")),
          empty(),
          Optional.of("00000ef508c1cb905e360590ce3e7e9193f6b370"));
}
