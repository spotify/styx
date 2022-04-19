/*-
 * -\-\-
 * Spotify Styx API Service
 * --
 * Copyright (C) 2016 - 2022 Spotify AB
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

package com.spotify.styx.api.util;


import static com.spotify.styx.api.util.CreateWorkflowUtil.createWorkflowWithTime;
import static com.spotify.styx.api.util.CreateWorkflowUtil.createWorkflowWithType;
import static com.spotify.styx.api.util.CreateWorkflowUtil.createWorkflowWithTypeAndTime;
import static com.spotify.styx.api.util.QueryParams.DEPLOYMENT_TIME_AFTER;
import static com.spotify.styx.api.util.QueryParams.DEPLOYMENT_TIME_BEFORE;
import static com.spotify.styx.api.util.QueryParams.DEPLOYMENT_TYPE;
import static com.spotify.styx.api.util.WorkflowFiltering.filterWorkflows;
import static com.spotify.styx.testdata.TestData.QUERY_THRESHOLD;
import static com.spotify.styx.testdata.TestData.QUERY_THRESHOLD_AFTER;
import static com.spotify.styx.testdata.TestData.QUERY_THRESHOLD_BEFORE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

import com.spotify.styx.model.Workflow;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class WorkflowFilteringTest {

  @Test
  public void shouldReturnAllWorkflows() {
    var workflowCollection = List.of(createWorkflowWithType("id1", "remote-foo"),
        createWorkflowWithType("id2", "remote-foo"));

    Map<QueryParams, String> emptyFilters = Map.of();

    List<Workflow> result = filterWorkflows(workflowCollection, emptyFilters);

    assertThat(result, equalTo(workflowCollection));
  }

  @Test
  public void shouldNotReturnWorkflowsWithDeploymentType() {

    var workflowCollection = List.of(createWorkflowWithType("id1", "remote-foo"),
        createWorkflowWithType("id2", "remote-foo"));

    var filterParams = Map.of(DEPLOYMENT_TYPE, "wrong-type");

    List<Workflow> result = filterWorkflows(workflowCollection, filterParams);

    assertThat(result, empty());
  }

  @Test
  public void shouldReturnWorkflowsWithDeploymentType() {

    Workflow validWorkflow = createWorkflowWithType("id1", "remote-foo");
    Workflow invalidWorkflow = createWorkflowWithType("id2", ""); // Empty type

    var workflowCollection = List.of(validWorkflow, invalidWorkflow);

    var filters = Map.of(DEPLOYMENT_TYPE, "remote-foo");

    var result = filterWorkflows(workflowCollection, filters);

    assertThat(result, hasSize(1));
    assertThat(result.get(0), equalTo(validWorkflow));
  }

  @Test
  public void shouldReturnWorkflowsWithDeploymentTimeBefore() {

    Workflow invalidWorkflow = createWorkflowWithType("id1", "remote-foo");
    Workflow validWorkflow = createWorkflowWithTime("id2", QUERY_THRESHOLD_BEFORE);

    var workflowCollection = List.of(validWorkflow, invalidWorkflow);

    var filters = Map.of(DEPLOYMENT_TIME_BEFORE, QUERY_THRESHOLD.toString());

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, hasSize(1));
    assertThat(result.get(0), equalTo(validWorkflow));
  }

  @Test
  public void shouldReturnWorkflowsWithDeploymentTimeAfter() {
    Workflow validWorkflow = createWorkflowWithTime("id2", QUERY_THRESHOLD_AFTER);
    Workflow invalidWorkflow = createWorkflowWithTime("id2", QUERY_THRESHOLD_BEFORE);

    var workflowCollection = List.of(validWorkflow, invalidWorkflow);

    var filters = Map.of(DEPLOYMENT_TIME_AFTER, QUERY_THRESHOLD.toString());

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, hasSize(1));
    assertThat(result.get(0), equalTo(validWorkflow));
  }

  @Test
  public void shouldReturnWorkflowsWithDeploymentTypeDeploymentTimeBeforeAndAfter() {
    var deploymentTimeAfter = "2022-01-01T10:15:28.00Z";
    var deploymentTimeBefore = "2022-01-01T10:15:32.00Z";
    var queryThresholdOutsideWindow = "2022-01-01T10:15:33.00Z";

    Workflow validWorkflow = createWorkflowWithTypeAndTime("id1", "remote-foo", QUERY_THRESHOLD);
    Workflow invalidWorkflow = createWorkflowWithTypeAndTime("id2", "remote-foo",
        Instant.parse(queryThresholdOutsideWindow));

    var workflowCollection = List.of(validWorkflow, invalidWorkflow);

    var filters = Map.of(
        DEPLOYMENT_TYPE, "remote-foo",
        DEPLOYMENT_TIME_BEFORE, deploymentTimeBefore,
        DEPLOYMENT_TIME_AFTER, deploymentTimeAfter);

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, hasSize(1));
    assertThat(result.get(0), equalTo(validWorkflow));
  }

  @Test
  public void shouldNotReturnWorkflowWithFilterDeploymentType() {
    var workflowCollection = List.of(createWorkflowWithType("id1", ""));
    var filters = Map.of(DEPLOYMENT_TYPE, "remote-foo");

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, empty());
  }

  @Test
  public void shouldNotReturnWorkflowWithFilterDeploymentTimeBefore() {
    var workflowCollection = List.of(createWorkflowWithType("id1", "remote-foo"));

    var filters = Map.of(DEPLOYMENT_TIME_BEFORE, QUERY_THRESHOLD_BEFORE.toString());

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, empty());
  }

  @Test
  public void shouldNotReturnWorkflowWithFilterDeploymentTimeAfter() {
    var workflowCollection = List.of(createWorkflowWithType("id1", "remote-foo"));

    var filters = Map.of(DEPLOYMENT_TIME_AFTER,
        QUERY_THRESHOLD_AFTER.toString());

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, empty());
  }

  @Test
  public void shouldNotReturnWorkflowWithFilterDeploymentTimeBeforeAndAfter() {
    var workflowCollection = List.of(createWorkflowWithType("id1", "remote-foo"));

    var filters = Map.of(DEPLOYMENT_TIME_BEFORE,
        QUERY_THRESHOLD_BEFORE.toString(), DEPLOYMENT_TIME_AFTER, QUERY_THRESHOLD_AFTER.toString());

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, empty());
  }
}
