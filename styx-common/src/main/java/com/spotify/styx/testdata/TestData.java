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
import static com.spotify.styx.model.Schedule.YEARS;

import com.google.common.collect.ImmutableSet;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfiguration.Secret;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import java.time.Duration;
import java.util.List;
import java.util.Set;

public final class TestData {

  public static final String VALID_SHA = "00000ef508c1cb905e360590ce3e7e9193f6b370";
  public static final String INVALID_SHA = "XXXXXef508c1cb905e360590ce3e7e9193f6b370";
  public static final Set<String> RESOURCE_IDS = ImmutableSet.of("foo-resource", "bar-resource");

  public static final WorkflowId WORKFLOW_ID =
      WorkflowId.create("styx", "styx.TestEndpoint");

  public static final WorkflowId WORKFLOW_ID_2 =
      WorkflowId.create("ranic", "ranic");

  public static final WorkflowInstance WORKFLOW_INSTANCE =
      WorkflowInstance.create(WORKFLOW_ID, "2016-09-01");

  public static final WorkflowConfiguration HOURLY_WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .commitSha(VALID_SHA)
          .dockerImage("busybox")
          .schedule(HOURS)
          .build();

  public static final WorkflowConfiguration HOURLY_WORKFLOW_CONFIGURATION_WITH_RESOURCES =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .commitSha(VALID_SHA)
          .dockerImage("busybox")
          .schedule(HOURS)
          .resources(RESOURCE_IDS)
          .build();

  public static final WorkflowConfiguration HOURLY_WORKFLOW_CONFIGURATION_WITH_RESOURCES_2 =
      WorkflowConfiguration.builder()
          .id("ranic")
          .commitSha(VALID_SHA)
          .dockerImage("busybox")
          .schedule(HOURS)
          .resources(RESOURCE_IDS)
          .build();

  public static final WorkflowConfiguration HOURLY_WORKFLOW_CONFIGURATION_WITH_INVALID_OFFSET =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .commitSha(VALID_SHA)
          .dockerImage("busybox")
          .schedule(HOURS)
          .offset("P1D2H") // the correct one should be P1DT2H
          .build();

  public static final WorkflowConfiguration HOURLY_WORKFLOW_CONFIGURATION_WITH_VALID_OFFSET =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .commitSha(VALID_SHA)
          .dockerImage("busybox")
          .schedule(HOURS)
          .offset("P30DT30M")
          .build();

  public static final WorkflowConfiguration DAILY_WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .commitSha(VALID_SHA)
          .dockerImage("busybox")
          .schedule(DAYS)
          .build();

  public static final WorkflowConfiguration WEEKLY_WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .commitSha(VALID_SHA)
          .dockerImage("busybox")
          .schedule(WEEKS)
          .build();

  public static final WorkflowConfiguration MONTHLY_WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .commitSha(VALID_SHA)
          .dockerImage("busybox")
          .schedule(MONTHS)
          .build();

  public static final WorkflowConfiguration YEARLY_WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .commitSha(VALID_SHA)
          .dockerImage("busybox")
          .schedule(YEARS)
          .build();

  public static final WorkflowConfiguration FULL_WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .commitSha(VALID_SHA)
          .schedule(DAYS)
          .dockerImage("busybox")
          .dockerArgs(List.of("x", "y"))
          .secret(Secret.create("name", "/path"))
          .serviceAccount("foo@bar.baz.quu")
          .build();

  public static final WorkflowConfiguration HOURLY_WORKFLOW_CONFIGURATION_WITH_RESOURCES_RUNNING_TIMEOUT =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .commitSha(VALID_SHA)
          .dockerImage("busybox")
          .schedule(HOURS)
          .resources(RESOURCE_IDS)
          .runningTimeout(Duration.ofMillis(2L))
          .build();

  public static final ExecutionDescription EXECUTION_DESCRIPTION =
      ExecutionDescription.builder()
          .dockerImage("busybox:1.1")
          .dockerArgs("foo", "bar")
          .secret(WorkflowConfiguration.Secret.create("secret", "/dev/null"))
          .commitSha(VALID_SHA)
          .build();

  public static final Workflow WORKFLOW_WITH_RESOURCES = Workflow.create(WORKFLOW_ID.componentId(),
      HOURLY_WORKFLOW_CONFIGURATION_WITH_RESOURCES);

  public static final Workflow WORKFLOW_WITH_RESOURCES_2 = Workflow.create(WORKFLOW_ID_2.componentId(),
      HOURLY_WORKFLOW_CONFIGURATION_WITH_RESOURCES_2);

  public static final Workflow WORKFLOW_WITH_RESOURCES_RUNNING_TIMEOUT = Workflow.create(WORKFLOW_ID.componentId(),
      HOURLY_WORKFLOW_CONFIGURATION_WITH_RESOURCES_RUNNING_TIMEOUT);

  private TestData() {
    throw new UnsupportedOperationException();
  }
}
