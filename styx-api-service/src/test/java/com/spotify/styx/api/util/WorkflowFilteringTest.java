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


import static com.spotify.styx.api.util.FilterParams.DEPLOYMENT_TIME_AFTER;
import static com.spotify.styx.api.util.FilterParams.DEPLOYMENT_TIME_BEFORE;
import static com.spotify.styx.api.util.FilterParams.DEPLOYMENT_TYPE;
import static com.spotify.styx.api.util.WorkflowFiltering.filterWorkflows;
import static com.spotify.styx.testdata.TestData.FLYTE_WORKFLOW_CONFIGURATION_WITHOUT_DEPLOYMENT_TIME;
import static com.spotify.styx.testdata.TestData.FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_SOURCE;
import static com.spotify.styx.testdata.TestData.FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TIME;
import static com.spotify.styx.testdata.TestData.FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TYPE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

import com.spotify.styx.model.Workflow;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class WorkflowFilteringTest {

  public static final Instant TEST_DEPLOYMENT_TIME_BEFORE = Instant.ofEpochSecond(1638709400);
  public static final Instant TEST_DEPLOYMENT_TIME_AFTER = Instant.ofEpochSecond(1638709300);


  @Test
  public void shouldReturnAllWorkflows() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(Workflow.create(
        "id-1", FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_SOURCE
    ));

    workflowCollection.add(Workflow.create(
        "id-2", FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_SOURCE
    ));

    Map<FilterParams, String> emptyFilters = Map.of(DEPLOYMENT_TYPE, "",
        DEPLOYMENT_TIME_BEFORE, "",
        DEPLOYMENT_TIME_AFTER, "");

    List<Workflow> workflows = filterWorkflows(workflowCollection, emptyFilters);

    assertThat(workflows, equalTo(new ArrayList<>(workflowCollection)));
  }

  @Test
  public void shouldNotReturnWorkflowsWithDeploymentType() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(Workflow.create(
        "id-1", FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_SOURCE
    ));

    workflowCollection.add(Workflow.create(
        "id-2", FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_SOURCE
    ));

    Map<FilterParams, String> emptyFilters = Map.of(DEPLOYMENT_TYPE, "wrong-type",
        DEPLOYMENT_TIME_BEFORE, "",
        DEPLOYMENT_TIME_AFTER, "");

    List<Workflow> workflows = filterWorkflows(workflowCollection, emptyFilters);

    assertThat(workflows, empty());
  }

  @Test
  public void shouldReturnWorkflowsWithDeploymentType() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(Workflow.create(
        "id-1", FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TYPE
    ));

    Map<FilterParams, String> filters = Map.of(DEPLOYMENT_TYPE, "remote-foo",
        DEPLOYMENT_TIME_BEFORE, "",
        DEPLOYMENT_TIME_AFTER, "");

    List<Workflow> workflows = filterWorkflows(workflowCollection, filters);

    assertThat(workflows, equalTo(new ArrayList<>(workflowCollection)));
  }

  @Test
  public void shouldReturnWorkflowsWithDeploymentTimeBefore() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(Workflow.create(
        "id-1", FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TYPE
    ));

    Map<FilterParams, String> filters = Map.of(DEPLOYMENT_TYPE, "remote-foo",
        DEPLOYMENT_TIME_BEFORE, TEST_DEPLOYMENT_TIME_BEFORE.toString(),
        DEPLOYMENT_TIME_AFTER, "");

    List<Workflow> workflows = filterWorkflows(workflowCollection, filters);

    assertThat(workflows, equalTo(new ArrayList<>(workflowCollection)));
  }

  @Test
  public void shouldReturnWorkflowsWithDeploymentTimeAfter() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(Workflow.create(
        "id-1", FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TYPE
    ));

    Map<FilterParams, String> filters = Map.of(DEPLOYMENT_TYPE, "remote-foo",
        DEPLOYMENT_TIME_BEFORE, "",
        DEPLOYMENT_TIME_AFTER, TEST_DEPLOYMENT_TIME_AFTER.toString());

    List<Workflow> workflows = filterWorkflows(workflowCollection, filters);

    assertThat(workflows, equalTo(new ArrayList<>(workflowCollection)));
  }

  @Test
  public void shouldReturnWorkflowsWithDeploymentTypeDeploymentTimeBeforeAndAfter() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(Workflow.create(
        "id-1", FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TYPE
    ));

    Map<FilterParams, String> filters = Map.of(DEPLOYMENT_TYPE, "remote-foo",
        DEPLOYMENT_TIME_BEFORE, TEST_DEPLOYMENT_TIME_BEFORE.toString(),
        DEPLOYMENT_TIME_AFTER, TEST_DEPLOYMENT_TIME_AFTER.toString());

    List<Workflow> workflows = filterWorkflows(workflowCollection, filters);

    assertThat(workflows, equalTo(new ArrayList<>(workflowCollection)));
  }

  @Test
  public void shouldNotReturnWorkflowWithFilterDeploymentType() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(Workflow.create(
        "id-1", FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TIME
    ));

    Map<FilterParams, String> filters = Map.of(DEPLOYMENT_TYPE, "remote-foo");


    List<Workflow> workflows = filterWorkflows(workflowCollection, filters);

    assertThat(workflows, empty());
  }

  @Test
  public void shouldNotReturnWorkflowWithFilterDeploymentTimeBefore() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(Workflow.create(
        "id-1", FLYTE_WORKFLOW_CONFIGURATION_WITHOUT_DEPLOYMENT_TIME
    ));

    Map<FilterParams, String> filters = Map.of(DEPLOYMENT_TIME_BEFORE, TEST_DEPLOYMENT_TIME_BEFORE.toString());


    List<Workflow> workflows = filterWorkflows(workflowCollection, filters);

    assertThat(workflows, empty());
  }

  @Test
  public void shouldNotReturnWorkflowWithFilterDeploymentTimeAfter() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(Workflow.create(
        "id-1", FLYTE_WORKFLOW_CONFIGURATION_WITHOUT_DEPLOYMENT_TIME
    ));

    Map<FilterParams, String> filters = Map.of(DEPLOYMENT_TIME_AFTER, TEST_DEPLOYMENT_TIME_AFTER.toString());


    List<Workflow> workflows = filterWorkflows(workflowCollection, filters);

    assertThat(workflows, empty());
  }

  @Test
  public void shouldNotReturnWorkflowWithFilterDeploymentTimeBeforeAndAfter() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(Workflow.create(
        "id-1", FLYTE_WORKFLOW_CONFIGURATION_WITHOUT_DEPLOYMENT_TIME
    ));

    Map<FilterParams, String> filters = Map.of(DEPLOYMENT_TIME_BEFORE, TEST_DEPLOYMENT_TIME_BEFORE.toString(),
        DEPLOYMENT_TIME_AFTER, TEST_DEPLOYMENT_TIME_AFTER.toString());


    List<Workflow> workflows = filterWorkflows(workflowCollection, filters);

    assertThat(workflows, empty());
  }
}
