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


import static com.spotify.styx.api.util.QueryParams.DEPLOYMENT_TIME_AFTER;
import static com.spotify.styx.api.util.QueryParams.DEPLOYMENT_TIME_BEFORE;
import static com.spotify.styx.api.util.QueryParams.DEPLOYMENT_TYPE;
import static com.spotify.styx.api.util.WorkflowFiltering.filterWorkflows;
import static com.spotify.styx.testdata.TestData.FLYTE_WORKFLOW_CONFIGURATION_WITHOUT_DEPLOYMENT_TIME;
import static com.spotify.styx.testdata.TestData.FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_SOURCE;
import static com.spotify.styx.testdata.TestData.FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TIME;
import static com.spotify.styx.testdata.TestData.FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TYPE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

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
    workflowCollection.add(
        Workflow.create("id-1", FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_SOURCE));

    workflowCollection.add(
        Workflow.create("id-2", FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_SOURCE));

    Map<QueryParams, String> emptyFilters = Map.of();

    List<Workflow> result = filterWorkflows(workflowCollection, emptyFilters);

    assertThat(result, equalTo(workflowCollection));
  }

  @Test
  public void shouldNotReturnWorkflowsWithDeploymentType() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(
        Workflow.create("id-1", FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_SOURCE));

    workflowCollection.add(
        Workflow.create("id-2", FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_SOURCE));

    Map<QueryParams, String> emptyFilters = Map.of(DEPLOYMENT_TYPE, "wrong-type",
        DEPLOYMENT_TIME_BEFORE, "", DEPLOYMENT_TIME_AFTER, "");

    List<Workflow> result = filterWorkflows(workflowCollection, emptyFilters);

    assertThat(result, empty());
  }

  @Test
  public void shouldReturnWorkflowsWithDeploymentType() {
    Collection<Workflow> workflowCollection = new ArrayList<>();

    Workflow validWorkflow = Workflow.create("id-1",
        FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TYPE);

    Workflow invalidWorkflow = Workflow.create("id-3",
        FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TIME); // No type set

    workflowCollection.add(validWorkflow);
    workflowCollection.add(invalidWorkflow);

    Map<QueryParams, String> filters = Map.of(DEPLOYMENT_TYPE, "remote-foo", DEPLOYMENT_TIME_BEFORE,
        "", DEPLOYMENT_TIME_AFTER, "");

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, hasSize(1));
    assertThat(result.get(0), equalTo(validWorkflow));
  }

  @Test
  public void shouldReturnWorkflowsWithDeploymentTimeBefore() {

    Workflow validWorkflow = Workflow.create("id-1",
        FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TYPE);

    Workflow invalidWorkflow = Workflow.create("id-2",
        FLYTE_WORKFLOW_CONFIGURATION_WITHOUT_DEPLOYMENT_TIME);

    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(validWorkflow);
    workflowCollection.add(invalidWorkflow);

    Map<QueryParams, String> filters = Map.of(DEPLOYMENT_TYPE, "remote-foo", DEPLOYMENT_TIME_BEFORE,
        TEST_DEPLOYMENT_TIME_BEFORE.toString(), DEPLOYMENT_TIME_AFTER, "");

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, hasSize(1));
    assertThat(result.get(0), equalTo(validWorkflow));
  }

  @Test
  public void shouldReturnWorkflowsWithDeploymentTimeAfter() {
    Workflow validWorkflow = Workflow.create("id-1",
        FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TYPE);

    Workflow invalidWorkflow = Workflow.create("id-2",
        FLYTE_WORKFLOW_CONFIGURATION_WITHOUT_DEPLOYMENT_TIME);
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(validWorkflow);
    workflowCollection.add(invalidWorkflow);

    Map<QueryParams, String> filters = Map.of(DEPLOYMENT_TYPE, "remote-foo", DEPLOYMENT_TIME_BEFORE,
        "", DEPLOYMENT_TIME_AFTER, TEST_DEPLOYMENT_TIME_AFTER.toString());

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, hasSize(1));
    assertThat(result.get(0), equalTo(validWorkflow));
  }

  @Test
  public void shouldReturnWorkflowsWithDeploymentTypeDeploymentTimeBeforeAndAfter() {
    Workflow validWorkflow = Workflow.create("id-1",
        FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TYPE);

    Workflow invalidWorkflow = Workflow.create("id-2",
        FLYTE_WORKFLOW_CONFIGURATION_WITHOUT_DEPLOYMENT_TIME);
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(validWorkflow);
    workflowCollection.add(invalidWorkflow);

    Map<QueryParams, String> filters = Map.of(DEPLOYMENT_TYPE, "remote-foo", DEPLOYMENT_TIME_BEFORE,
        TEST_DEPLOYMENT_TIME_BEFORE.toString(), DEPLOYMENT_TIME_AFTER,
        TEST_DEPLOYMENT_TIME_AFTER.toString());

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, hasSize(1));
    assertThat(result.get(0), equalTo(validWorkflow));
  }

  @Test
  public void shouldNotReturnWorkflowWithFilterDeploymentType() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(
        Workflow.create("id-1", FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_TIME));

    Map<QueryParams, String> filters = Map.of(DEPLOYMENT_TYPE, "remote-foo");

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, empty());
  }

  @Test
  public void shouldNotReturnWorkflowWithFilterDeploymentTimeBefore() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(
        Workflow.create("id-1", FLYTE_WORKFLOW_CONFIGURATION_WITHOUT_DEPLOYMENT_TIME));

    Map<QueryParams, String> filters = Map.of(DEPLOYMENT_TIME_BEFORE,
        TEST_DEPLOYMENT_TIME_BEFORE.toString());

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, empty());
  }

  @Test
  public void shouldNotReturnWorkflowWithFilterDeploymentTimeAfter() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(
        Workflow.create("id-1", FLYTE_WORKFLOW_CONFIGURATION_WITHOUT_DEPLOYMENT_TIME));

    Map<QueryParams, String> filters = Map.of(DEPLOYMENT_TIME_AFTER,
        TEST_DEPLOYMENT_TIME_AFTER.toString());

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, empty());
  }

  @Test
  public void shouldNotReturnWorkflowWithFilterDeploymentTimeBeforeAndAfter() {
    Collection<Workflow> workflowCollection = new ArrayList<>();
    workflowCollection.add(
        Workflow.create("id-1", FLYTE_WORKFLOW_CONFIGURATION_WITHOUT_DEPLOYMENT_TIME));

    Map<QueryParams, String> filters = Map.of(DEPLOYMENT_TIME_BEFORE,
        TEST_DEPLOYMENT_TIME_BEFORE.toString(), DEPLOYMENT_TIME_AFTER,
        TEST_DEPLOYMENT_TIME_AFTER.toString());

    List<Workflow> result = filterWorkflows(workflowCollection, filters);

    assertThat(result, empty());
  }
}
