/*
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

import static com.spotify.styx.model.Partitioning.HOURS;
import static com.spotify.styx.model.WorkflowState.patchEnabled;
import static java.util.Optional.empty;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.styx.model.DataEndpoint;
import com.spotify.styx.model.ExecutionStatus;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowExecutionInfo;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.testdata.TestData;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

/**
 * Tests the InMemoryStorage that is used for testing.
 */
public class InMemoryStorageTest {

  @Test
  public void testGetExecutionInfo() throws Exception {
    Storage storage = new InMemStorage();

    Instant instant = Instant.now().truncatedTo(ChronoUnit.HOURS);
    WorkflowInstance wfi = WorkflowInstance.create(TestData.WORKFLOW_ID, instant.toString());
    WorkflowExecutionInfo first = WorkflowExecutionInfo.create(
        wfi, Instant.now(), ExecutionStatus.STARTED, Optional.empty());
    WorkflowExecutionInfo second = WorkflowExecutionInfo.create(
        wfi, Instant.MAX, ExecutionStatus.SUCCEEDED, Optional.empty());

    storage.store(second);
    storage.store(first);

    List<WorkflowExecutionInfo> workflows = storage.getExecutionInfo(wfi);
    assertThat(workflows, contains(first, second));
  }

  @Test
  public void testGetExecutionInfoWithoutParam() throws IOException {
    Storage storage = new InMemStorage();

    Instant instant1 = Instant.now();
    Instant instant2 = instant1.plus(1, ChronoUnit.HOURS);
    WorkflowInstance wfi1 = WorkflowInstance.create(TestData.WORKFLOW_ID, instant1.toString());
    WorkflowInstance wfi2 = WorkflowInstance.create(TestData.WORKFLOW_ID, instant2.toString());
    WorkflowExecutionInfo first = WorkflowExecutionInfo.create(
        wfi1, instant1, ExecutionStatus.STARTED, Optional.empty());
    WorkflowExecutionInfo second = WorkflowExecutionInfo.create(
        wfi1, instant2, ExecutionStatus.SUCCEEDED, Optional.empty());
    WorkflowExecutionInfo third = WorkflowExecutionInfo.create(
        wfi2, instant1, ExecutionStatus.STARTED, Optional.empty());
    storage.store(second);
    storage.store(first);
    storage.store(third);

    Map<WorkflowInstance, List<WorkflowExecutionInfo>> allExecutions =
        storage.getExecutionInfo(TestData.WORKFLOW_ID);
    assertThat(allExecutions.keySet(), hasItem(wfi1));
    assertThat(allExecutions.keySet(), hasItem(wfi2));

    assertThat(allExecutions.get(wfi1), contains(first, second));
    assertThat(allExecutions.get(wfi2), contains(third));
  }

  @Test
  public void testEnableWorkflow() throws IOException {
    Storage storage = new InMemStorage();

    WorkflowId id1 = WorkflowId.create("someComponent1", "someEndpoint1");
    WorkflowId id2 = WorkflowId.create("someComponent2", "someEndpoint2");

    storage.store(workflow(id1));
    storage.store(workflow(id2));

    WorkflowState workflowState = storage.workflowState(id1).get();
    assertThat(workflowState.enabled().get(), is(false));
    storage.patchState(id1, patchEnabled(true));
    workflowState = storage.workflowState(id1).get();
    assertThat(workflowState.enabled().get(), is(true));
    storage.patchState(id2, patchEnabled(true));
    assertThat(storage.enabled(), containsInAnyOrder(id1, id2));
    storage.patchState(id1, patchEnabled(false));
    workflowState = storage.workflowState(id1).get();
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

  private Workflow workflow(WorkflowId workflowId) {
    return Workflow.create(
        workflowId.componentId(),
        URI.create("http://foo"),
        DataEndpoint.create(workflowId.endpointId(), HOURS, empty(), empty(), empty()));
  }
}
