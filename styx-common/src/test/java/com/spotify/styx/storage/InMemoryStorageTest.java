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

import static com.spotify.styx.model.Schedule.HOURS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowState;
import java.io.IOException;
import java.util.List;
import org.junit.Test;

/**
 * Tests the InMemoryStorage that is used for testing.
 */
public class InMemoryStorageTest {

  private static final WorkflowId WORKFLOW_ID1 = WorkflowId.create("component", "endpoint1");
  private static final WorkflowId WORKFLOW_ID2 = WorkflowId.create("component", "endpoint2");
  private static final WorkflowId WORKFLOW_ID3 = WorkflowId.create("component2", "pointless");

  @Test
  public void testEnableWorkflow() throws IOException {
    Storage storage = new InMemStorage();

    WorkflowId id1 = WorkflowId.create("someComponent1", "someEndpoint1");
    WorkflowId id2 = WorkflowId.create("someComponent2", "someEndpoint2");

    storage.storeWorkflow(workflow(id1));
    storage.storeWorkflow(workflow(id2));

    WorkflowState workflowState = storage.workflowState(id1);
    assertThat(workflowState.enabled().get(), is(false));
    storage.patchState(id1, WorkflowState.builder().enabled(true).build());
    workflowState = storage.workflowState(id1);
    assertThat(workflowState.enabled().get(), is(true));
    storage.patchState(id2, WorkflowState.builder().enabled(true).build());
    assertThat(storage.enabled(), containsInAnyOrder(id1, id2));
    storage.patchState(id1, WorkflowState.builder().enabled(false).build());
    workflowState = storage.workflowState(id1);
    assertThat(workflowState.enabled().get(), is(false));
  }

  @Test
  public void testEnableGlobal() throws IOException {
    Storage storage = new InMemStorage();

    assertThat(storage.globalEnabled(), is(true));
    assertThat(storage.setGlobalEnabled(false), is(true));
    assertThat(storage.globalEnabled(), is(false));
    assertThat(storage.setGlobalEnabled(true), is(false));
    assertThat(storage.globalEnabled(), is(true));
  }

  @Test
  public void shouldReturnAllWorkflowsInComponent() throws Exception {
    Storage storage = new InMemStorage();

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
    Storage storage = new InMemStorage();
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
}
