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

package com.spotify.styx.storage;

import static com.spotify.styx.model.Schedule.DAYS;
import static com.spotify.styx.model.Schedule.HOURS;
import static com.spotify.styx.model.WorkflowState.patchEnabled;
import static com.spotify.styx.storage.DatastoreStorageTest.FULLY_POPULATED_RUNSTATE;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableMap;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.state.RunState;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests the InMemStorage that is used for testing.
 */
public class InMemStorageTest {

  private static final WorkflowId WORKFLOW_ID1 = WorkflowId.create("component", "endpoint1");
  private static final WorkflowId WORKFLOW_ID2 = WorkflowId.create("component", "endpoint2");
  private static final WorkflowId WORKFLOW_ID3 = WorkflowId.create("component2", "pointless");

  private Storage storage;

  @Before
  public void setUp() {
    storage = new InMemStorage();
  }

  @Test
  public void testEnableWorkflow() throws IOException {
    WorkflowId id1 = WorkflowId.create("someComponent1", "someEndpoint1");
    WorkflowId id2 = WorkflowId.create("someComponent2", "someEndpoint2");

    storage.storeWorkflow(workflow(id1));
    storage.storeWorkflow(workflow(id2));

    WorkflowState workflowState = storage.workflowState(id1);
    assertThat(workflowState.enabled().get(), is(false));
    storage.patchState(id1, patchEnabled(true));
    workflowState = storage.workflowState(id1);
    assertThat(workflowState.enabled().get(), is(true));
    storage.patchState(id2, patchEnabled(true));
    assertThat(storage.enabled(), containsInAnyOrder(id1, id2));
    storage.patchState(id1, patchEnabled(false));
    workflowState = storage.workflowState(id1);
    assertThat(workflowState.enabled().get(), is(false));
  }

  @Test
  public void testConfig() throws IOException {
    final StyxConfig expectedConfig = StyxConfig.newBuilder()
        .globalEnabled(true)
        .globalDockerRunnerId("default")
        .build();

    assertThat(storage.config(), is(expectedConfig));
  }

  @Test
  public void shouldReturnAllWorkflowsInComponent() throws Exception {
    String componentId = "component";

    Workflow workflow1 = workflow(WORKFLOW_ID1);
    Workflow workflow2 = workflow(WORKFLOW_ID2);
    Workflow workflow3 = workflow(WORKFLOW_ID3);

    assertThat(workflow1.componentId(), is(componentId));
    assertThat(workflow2.componentId(), is(componentId));
    assertThat(workflow3.componentId(), not(componentId));

    storage.storeWorkflow(workflow1);
    storage.storeWorkflow(workflow2);
    storage.storeWorkflow(workflow3);

    List<Workflow> l = storage.workflows(componentId);
    assertThat(l, hasSize(2));

    assertThat(l, hasItem(workflow1));
    assertThat(l, hasItem(workflow2));
  }

  @Test
  public void shouldReturnEmptyListIfComponentDoesNotExist() throws Exception {
    String componentId = "component";
    List<Workflow> l = storage.workflows(componentId);
    assertThat(l, hasSize(0));
  }

  private Workflow workflow(WorkflowId workflowId) {
    return Workflow.create(
        workflowId.componentId(),
        WorkflowConfiguration.builder()
            .id(workflowId.id())
            .schedule(HOURS)
            .build());
  }

  @Test
  public void shouldReturnAllActiveStatesForATriggerId() throws IOException {
    storage.writeActiveState(WORKFLOW_INSTANCE, FULLY_POPULATED_RUNSTATE);

    final Map<WorkflowInstance, RunState> activeStates =
        storage.readActiveWorkflowInstancesByTriggerId("foobar");

    assertThat(activeStates, is(ImmutableMap.of(WORKFLOW_INSTANCE, FULLY_POPULATED_RUNSTATE)));
  }

  @Test
  public void shouldStoreAndReadBackfill() throws Exception {
    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-02T00:00:00Z"))
        .workflowId(WorkflowId.create("component", "workflow2"))
        .concurrency(2)
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(DAYS)
        .build();

    storage.storeBackfill(backfill);
    assertThat(storage.backfill(backfill.id()), equalTo(Optional.of(backfill)));
  }
}
