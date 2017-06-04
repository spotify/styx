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
import static com.spotify.styx.model.WorkflowState.patchEnabled;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowState;
import java.io.IOException;
import java.net.URI;
import org.junit.Test;

/**
 * Tests the InMemoryStorage that is used for testing.
 */
public class InMemoryStorageTest {

  @Test
  public void testEnableWorkflow() throws IOException {
    Storage storage = new InMemStorage();

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
        WorkflowConfiguration.builder()
            .id(workflowId.id())
            .schedule(HOURS)
            .build());
  }
}
