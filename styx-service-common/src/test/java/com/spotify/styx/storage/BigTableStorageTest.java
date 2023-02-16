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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.data.ExecStatus;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.util.ResourceNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.hbase.client.Connection;
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

  private static final int MAX_BIGTABLE_RETRIES = 3;

  private static final TriggerParameters TRIGGER_PARAMETERS = TriggerParameters.builder()
      .env("FOO", "foo",
          "BAR", "bar")
      .build();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private Connection bigtable;
  private BigtableStorage storage;

  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  public void setUp(int numFailures) throws Exception {
    bigtable = setupBigTableMockTable(numFailures);
    storage =
        new BigtableStorage(bigtable, executor, WaitStrategies.noWait(),
            StopStrategies.stopAfterAttempt(MAX_BIGTABLE_RETRIES));
  }

  private Connection setupBigTableMockTable(int numFailures) throws IOException {
    Connection bigtable = mock(Connection.class);
    new BigtableMocker(bigtable)
        .setNumFailures(numFailures)
        .setupTable(BigtableStorage.EVENTS_TABLE_NAME)
        .finalizeMocking();
    return bigtable;
  }

  @Test
  public void shouldReturnExecutionDataForWorkflowInstance() throws Exception {
    setUp(0);
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI1, TRIGGER, TRIGGER_PARAMETERS), 0L, 0L));
    storage.writeEvent(SequenceEvent.create(Event.created(WFI1, "execId", "img"), 1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.started(WFI1), 2L, 2L));

    WorkflowInstanceExecutionData workflowInstanceExecutionData = storage.executionData(WFI1);
    assertThat(workflowInstanceExecutionData.triggers().get(0).triggerId(), is("triggerId"));
    assertThat(workflowInstanceExecutionData.triggers().get(0).executions().get(0).executionId(), is(Optional.of("execId")));
    assertThat(workflowInstanceExecutionData.triggers().get(0).executions().get(0).dockerImage(), is(Optional.of("img")));
    assertThat(workflowInstanceExecutionData.triggers().get(0).executions().get(0).statuses().get(0), is(
        ExecStatus.create(Instant.ofEpochMilli(1L), "SUBMITTED", Optional.empty())));
    assertThat(workflowInstanceExecutionData.triggers().get(0).executions().get(0).statuses().get(1), is(
        ExecStatus.create(Instant.ofEpochMilli(2L), "STARTED", Optional.empty())));
  }

  @Test
  public void shouldReturnExecutionDataForWorkflow() throws Exception {
    setUp(0);
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI1, TRIGGER1, TRIGGER_PARAMETERS), 0L, 0L));
    storage.writeEvent(SequenceEvent.create(Event.created(WFI1, "execId1", "img1"), 1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.started(WFI1), 2L, 2L));

    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI2, TRIGGER2, TRIGGER_PARAMETERS), 0L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.created(WFI2, "execId2", "img2"), 1L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(WFI2), 2L, 5L));

    List<WorkflowInstanceExecutionData> workflowInstanceExecutionData =
        storage.executionData(WORKFLOW_ID1, "", 100);

    assertThat(workflowInstanceExecutionData.size(), is(2));

    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).triggerId(), is("triggerId1"));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).executionId(), is(Optional.of("execId1")));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).dockerImage(), is(Optional.of("img1")));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).statuses()
                   .get(0), is(ExecStatus.create(Instant.ofEpochMilli(1L), "SUBMITTED", Optional.empty())));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).statuses()
                   .get(1), is(ExecStatus.create(Instant.ofEpochMilli(2L), "STARTED", Optional.empty())));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).triggerId(), is("triggerId2"));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).executions().get(0).executionId(), is(Optional.of("execId2")));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).executions().get(0).dockerImage(), is(Optional.of("img2")));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).executions().get(0).statuses()
                   .get(0), is(ExecStatus.create(Instant.ofEpochMilli(4L), "SUBMITTED", Optional.empty())));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).executions().get(0).statuses()
                   .get(1), is(ExecStatus.create(Instant.ofEpochMilli(5L), "STARTED", Optional.empty())));
  }

  @Test
  public void shouldPaginateExecutionDataForWorkflow() throws Exception {
    setUp(0);
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI1, TRIGGER1, TRIGGER_PARAMETERS), 0L, 0L));
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI2, TRIGGER2, TRIGGER_PARAMETERS), 0L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI3, TRIGGER3, TRIGGER_PARAMETERS), 0L, 3L));

    List<WorkflowInstanceExecutionData> workflowInstanceExecutionData =
        storage.executionData(WORKFLOW_ID1, WFI2.parameter(), 100);

    assertThat(workflowInstanceExecutionData.size(), is(1));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).triggerId(), is("triggerId2"));
  }

  @Test
  public void shouldLimitExecutionDataForWorkflow() throws Exception {
    setUp(0);
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI1, TRIGGER1, TRIGGER_PARAMETERS), 0L, 0L));
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI2, TRIGGER2, TRIGGER_PARAMETERS), 0L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI3, TRIGGER3, TRIGGER_PARAMETERS), 0L, 3L));

    List<WorkflowInstanceExecutionData> workflowInstanceExecutionData =
        storage.executionData(WORKFLOW_ID1, WFI1.parameter(), 1);

    assertThat(workflowInstanceExecutionData.size(), is(1));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).triggerId(), is("triggerId1"));
  }

  @Test
  public void shouldReturnRangeOfExecutionDataForWorkflow() throws Exception {
    setUp(0);
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI1, TRIGGER1, TRIGGER_PARAMETERS), 0L, 0L));
    storage.writeEvent(SequenceEvent.create(Event.created(WFI1, "execId1", "img1"), 1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.started(WFI1), 2L, 2L));

    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI2, TRIGGER2, TRIGGER_PARAMETERS), 0L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.created(WFI2, "execId2", "img2"), 1L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(WFI2), 2L, 5L));

    List<WorkflowInstanceExecutionData> workflowInstanceExecutionData =
        storage.executionData(WORKFLOW_ID1, WFI1.parameter(), "");

    assertThat(workflowInstanceExecutionData.size(), is(2));

    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).triggerId(), is("triggerId1"));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).executionId(), is(Optional.of("execId1")));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).dockerImage(), is(Optional.of("img1")));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).statuses()
                   .get(0), is(ExecStatus.create(Instant.ofEpochMilli(1L), "SUBMITTED", Optional.empty())));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).statuses()
                   .get(1), is(ExecStatus.create(Instant.ofEpochMilli(2L), "STARTED", Optional.empty())));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).triggerId(), is("triggerId2"));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).executions().get(0).executionId(), is(Optional.of("execId2")));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).executions().get(0).dockerImage(), is(Optional.of("img2")));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).executions().get(0).statuses()
                   .get(0), is(ExecStatus.create(Instant.ofEpochMilli(4L), "SUBMITTED", Optional.empty())));
    assertThat(workflowInstanceExecutionData.get(1).triggers().get(0).executions().get(0).statuses()
                   .get(1), is(ExecStatus.create(Instant.ofEpochMilli(5L), "STARTED", Optional.empty())));
  }

  @Test
  public void shouldReturnExecutionDataForOneWorkflow() throws Exception {
    setUp(0);
    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI1, TRIGGER1, TRIGGER_PARAMETERS), 0L, 0L));
    storage.writeEvent(SequenceEvent.create(Event.created(WFI1, "execId1", "img1"), 1L, 1L));
    storage.writeEvent(SequenceEvent.create(Event.started(WFI1), 2L, 2L));

    storage.writeEvent(SequenceEvent.create(Event.triggerExecution(WFI2, TRIGGER2, TRIGGER_PARAMETERS), 0L, 3L));
    storage.writeEvent(SequenceEvent.create(Event.created(WFI2, "execId2", "img2"), 1L, 4L));
    storage.writeEvent(SequenceEvent.create(Event.started(WFI2), 2L, 5L));

    List<WorkflowInstanceExecutionData> workflowInstanceExecutionData =
        storage.executionData(WORKFLOW_ID1, WFI1.parameter(), WFI2.parameter());

    assertThat(workflowInstanceExecutionData.size(), is(1));

    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).triggerId(), is("triggerId1"));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).executionId(), is(Optional.of("execId1")));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).dockerImage(), is(Optional.of("img1")));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).statuses()
                   .get(0), is(ExecStatus.create(Instant.ofEpochMilli(1L), "SUBMITTED", Optional.empty())));
    assertThat(workflowInstanceExecutionData.get(0).triggers().get(0).executions().get(0).statuses()
                   .get(1), is(ExecStatus.create(Instant.ofEpochMilli(2L), "STARTED", Optional.empty())));
  }

  @Test
  public void shouldProduceIOExceptionIfTooManyPutRetries() throws Exception {
    setUp(MAX_BIGTABLE_RETRIES);

    thrown.expect(IOException.class);
    thrown.expectMessage(containsString("Something went wrong in performing put operation"));

    storage.writeEvent(SequenceEvent.create(Event.success(WFI1), 1, 0));
  }

  @Test
  public void shouldNotProduceIOExceptionIfPutRetrySucceeds() throws Exception {
    setUp(MAX_BIGTABLE_RETRIES - 1);

    storage.writeEvent(SequenceEvent.create(Event.success(WFI1), 1, 0));
  }

  @Test
  public void shouldProduceRuntimeExceptionWrappingRuntimeException() throws Exception {
    setUp(0);

    var e = new RuntimeException();
    doThrow(e).when(bigtable).getTable(any());

    thrown.expect(RuntimeException.class);
    thrown.expectCause(is(e));

    storage.writeEvent(SequenceEvent.create(Event.success(WFI1), 1, 0));
  }

  @Test
  public void shouldProduceResourceNotFoundExceptionWhenNoExecutionData() throws Exception {
    setUp(0);

    thrown.expect(ResourceNotFoundException.class);
    thrown.expectMessage(containsString("Workflow instance not found"));

    storage.executionData(WFI1);
  }
}
