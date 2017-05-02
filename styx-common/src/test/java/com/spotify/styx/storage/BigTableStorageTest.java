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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.data.ExecStatus;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.state.Trigger;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class BigTableStorageTest {

  private static final String PARAMETER1 = "2016-01-01";
  private static final String PARAMETER2 = "2016-01-02";

  private static final WorkflowId WORKFLOW_ID1 = WorkflowId.create("component", "endpoint1");
  private static final WorkflowId WORKFLOW_ID2 = WorkflowId.create("component", "endpoint2");
  private static final WorkflowInstance WFI1 = WorkflowInstance.create(WORKFLOW_ID1, PARAMETER1);
  private static final WorkflowInstance WFI2 = WorkflowInstance.create(WORKFLOW_ID1, PARAMETER2);
  private static final WorkflowInstance WFI3 = WorkflowInstance.create(WORKFLOW_ID2, PARAMETER1);

  private static final Trigger TRIGGER = Trigger.unknown("triggerId");
  private static final Trigger TRIGGER1 = Trigger.unknown("triggerId1");
  private static final Trigger TRIGGER2 = Trigger.unknown("triggerId2");
  private static final Trigger TRIGGER3 = Trigger.unknown("triggerId3");

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private BigtableStorage storage;

  @Before
  public void setUp() throws Exception {
    Connection bigtable = setupBigTableMockTable();
    storage = new BigtableStorage(bigtable);
  }

  private Connection setupBigTableMockTable() throws IOException {
    Connection bigtable = mock(Connection.class);
    new BigtableMocker(bigtable)
        .setupTable(BigtableStorage.EVENTS_TABLE_NAME)
        .finalizeMocking();
    return bigtable;
  }

  @Test
  public void shouldReturnExecutionDataForWorkflowInstance() throws Exception {
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI1, TRIGGER), 0L, 0L));
    storage.writeEvent(SequenceEvent.create(Event.created(WFI1, "execId", "img"), 1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.started(WFI1), 2L, 2L));

    WorkflowInstanceExecutionData workflowInstanceExecutionData = storage.executionData(WFI1);
    assertThat(workflowInstanceExecutionData.triggers().get(0).triggerId(), is("triggerId"));
    assertThat(workflowInstanceExecutionData.triggers().get(0).executions().get(0).executionId(), is("execId"));
    assertThat(workflowInstanceExecutionData.triggers().get(0).executions().get(0).dockerImage(), is("img"));
    assertThat(workflowInstanceExecutionData.triggers().get(0).executions().get(0).statuses().get(0), is(
        ExecStatus.create(Instant.ofEpochMilli(1L), "SUBMITTED")));
    assertThat(workflowInstanceExecutionData.triggers().get(0).executions().get(0).statuses().get(1), is(
        ExecStatus.create(Instant.ofEpochMilli(2L), "STARTED")));
  }

  @Test
  public void shouldReturnExecutionDataForWorkflow() throws Exception {
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI1, TRIGGER1), 0L, 0L));
    storage.writeEvent(SequenceEvent.create(Event.created(WFI1, "execId1", "img1"), 1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.started(WFI1), 2L, 2L));

    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI2, TRIGGER2), 0L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.created(WFI2, "execId2", "img2"), 1L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(WFI2), 2L, 5L));

    List<WorkflowInstanceExecutionData> workflowInstanceExecutionData =
        storage.executionData(WORKFLOW_ID1, "", 100);

    assertThat(workflowInstanceExecutionData.size(), is(2));

    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).triggerId(), is("triggerId1"));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).executionId(), is("execId1"));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).dockerImage(), is("img1"));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).statuses()
                   .get(0), is(ExecStatus.create(Instant.ofEpochMilli(1L), "SUBMITTED")));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).statuses()
                   .get(1), is(ExecStatus.create(Instant.ofEpochMilli(2L), "STARTED")));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).triggerId(), is("triggerId2"));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).executions().get(0).executionId(), is("execId2"));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).executions().get(0).dockerImage(), is("img2"));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).executions().get(0).statuses()
                   .get(0), is(ExecStatus.create(Instant.ofEpochMilli(4L), "SUBMITTED")));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).executions().get(0).statuses()
                   .get(1), is(ExecStatus.create(Instant.ofEpochMilli(5L), "STARTED")));
  }

  @Test
  public void shouldPaginateExecutionDataForWorkflow() throws Exception {
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI1, TRIGGER1), 0L, 0L));
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI2, TRIGGER2), 0L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI3, TRIGGER3), 0L, 3L));

    List<WorkflowInstanceExecutionData> workflowInstanceExecutionData =
        storage.executionData(WORKFLOW_ID1, WFI2.parameter(), 100);

    assertThat(workflowInstanceExecutionData.size(), is(1));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).triggerId(), is("triggerId2"));
  }

  @Test
  public void shouldLimitExecutionDataForWorkflow() throws Exception {
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI1, TRIGGER1), 0L, 0L));
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI2, TRIGGER2), 0L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI3, TRIGGER3), 0L, 3L));

    List<WorkflowInstanceExecutionData> workflowInstanceExecutionData =
        storage.executionData(WORKFLOW_ID1, WFI1.parameter(), 1);

    assertThat(workflowInstanceExecutionData.size(), is(1));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).triggerId(), is("triggerId1"));
  }
}
