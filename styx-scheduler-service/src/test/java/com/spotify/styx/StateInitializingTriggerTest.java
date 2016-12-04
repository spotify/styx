/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx;

import static com.spotify.styx.model.Partitioning.DAYS;
import static com.spotify.styx.model.Partitioning.HOURS;
import static com.spotify.styx.model.Partitioning.MONTHS;
import static com.spotify.styx.model.Partitioning.WEEKS;
import static java.util.Optional.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.spotify.styx.model.DataEndpoint;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.testdata.TestData;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import org.junit.Test;

public class StateInitializingTriggerTest {

  private static final Instant TIME = Instant.parse("2016-01-18T09:11:22.333Z");

  private static final Map<Partitioning, String> PARTITIONING_ARG_EXPECTS =
      ImmutableMap.of(
          WEEKS, "2016-01-18",
          DAYS, "2016-01-18",
          HOURS, "2016-01-18T09",
          MONTHS, "2016-01"
      );

  private SyncStateManager stateManager = new SyncStateManager();
  private Storage storage = mock(Storage.class);
  private TriggerListener
      trigger = new StateInitializingTrigger(RunState::fresh, stateManager, storage);

  @Test
  public void shouldInitializeWorkflowInstance() throws Exception {
    DataEndpoint endpoint = dataEndpoint(HOURS);
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, endpoint);
    setDockerImage(workflow.id(), workflow.schedule());
    trigger.event(workflow, "trig", TIME);

    assertThat(stateManager.activeStatesSize(), is(1));
  }

  @Test
  public void shouldInjectTriggerExecutionEventWithTriggerId() throws Exception {
    DataEndpoint endpoint = dataEndpoint(HOURS);
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, endpoint);
    setDockerImage(workflow.id(), workflow.schedule());
    trigger.event(workflow, "trig", TIME);

    WorkflowInstance expectedInstance = WorkflowInstance.create(workflow.id(), "2016-01-18T09");
    RunState state = stateManager.get(expectedInstance);

    // todo: assert trigger id
    assertThat(state.state(), is(RunState.State.PREPARE));
  }

  @Test
  public void shouldDoNothingIfDockerInfoMissing() throws Exception {
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, TestData.DAILY_DATA_ENDPOINT);
    setDockerImage(workflow.id(), workflow.schedule());
    trigger.event(workflow, "trig", TIME);

    assertThat(stateManager.activeStatesSize(), is(0));
  }

  @Test
  public void shouldCreateWorkflowInstanceParameter() throws Exception {
    for (Map.Entry<Partitioning, String> partitioningCase : PARTITIONING_ARG_EXPECTS.entrySet()) {
      DataEndpoint endpoint = dataEndpoint(partitioningCase.getKey(), "--date", "{}", "--bar");
      Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, endpoint);
      setDockerImage(workflow.id(), workflow.schedule());
      trigger.event(workflow, "trig", TIME);

      RunState runState =
          stateManager.get(WorkflowInstance.create(workflow.id(), partitioningCase.getValue()));

      assertThat(runState, is(notNullValue()));
      assertThat(runState.workflowInstance().parameter(), is(partitioningCase.getValue()));
    }
  }

  @Test
  public void testsShouldCoverAllPartitioningCases() throws Exception {
    assertThat(PARTITIONING_ARG_EXPECTS.keySet(), is(Sets.newHashSet(Partitioning.values())));
  }

  private DataEndpoint dataEndpoint(Partitioning partitioning, String... args) {
    return DataEndpoint.create(
        "styx.TestEndpoint",
        partitioning,
        Optional.of("busybox"),
        Optional.of(Lists.newArrayList(args)),
        empty());
  }

  // todo: do not use deprecated getDockerImage method
  private void setDockerImage(WorkflowId workflowId, DataEndpoint schedule) throws IOException {
    when(storage.getDockerImage(workflowId)).thenReturn(schedule.dockerImage());
  }
}
