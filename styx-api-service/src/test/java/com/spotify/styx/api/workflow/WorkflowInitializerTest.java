/*-
 * -\-\-
 * Spotify styx
 * --
 * Copyright (C) 2017 Spotify AB
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

package com.spotify.styx.api.workflow;

import static com.spotify.styx.util.TimeUtil.lastInstant;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import com.spotify.styx.storage.TransactionFunction;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.TriggerInstantSpec;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WorkflowInitializerTest {

  public static final Instant NOW = Instant.parse("2015-12-31T23:59:10.000Z");
  private final Workflow HOURLY_WORKFLOW = Workflow.create("styx",
      TestData.HOURLY_WORKFLOW_CONFIGURATION);
  private final Workflow HOURLY_WORKFLOW_WITH_INVALID_OFFSET =
      Workflow.create("styx", TestData.HOURLY_WORKFLOW_CONFIGURATION_WITH_INVALID_OFFSET);

  private final Workflow HOURLY_WORKFLOW_WITH_VALID_OFFSET =
      Workflow.create("styx", TestData.HOURLY_WORKFLOW_CONFIGURATION_WITH_VALID_OFFSET);

  private WorkflowInitializer workflowInitializer;

  @Mock private Storage storage;
  @Mock private StorageTransaction transaction;

  @Before
  public void setUp() throws IOException {
    workflowInitializer = new WorkflowInitializer(storage, () -> NOW);
    when(storage.runInTransaction(any())).then(a ->
        a.getArgumentAt(0, TransactionFunction.class).apply(transaction));
  }

  @Test
  public void shouldStoreNewWorkflowAndUpdateNextNaturalTrigger() throws IOException, WorkflowInitializationException {
    when(transaction.workflow(HOURLY_WORKFLOW.id())).thenReturn(Optional.empty());
    when(transaction.store(HOURLY_WORKFLOW)).thenReturn(HOURLY_WORKFLOW.id());
    workflowInitializer.store(HOURLY_WORKFLOW);
    verify(transaction).store(HOURLY_WORKFLOW);

    final Instant nextTrigger = lastInstant(NOW, Schedule.HOURS);
    final Instant nextWithOffset = HOURLY_WORKFLOW.configuration().addOffset(nextTrigger);
    TriggerInstantSpec expectedTriggerInstantSpec = TriggerInstantSpec.create(nextTrigger, nextWithOffset);

    verify(transaction).updateNextNaturalTrigger(HOURLY_WORKFLOW.id(), expectedTriggerInstantSpec);
  }

  @Test
  public void shouldUpdateExistingWorkflowAndNotUpdateNextNaturalTrigger()
      throws IOException, WorkflowInitializationException {
    when(transaction.workflow(HOURLY_WORKFLOW.id())).thenReturn(Optional.of(HOURLY_WORKFLOW));
    when(transaction.store(HOURLY_WORKFLOW)).thenReturn(HOURLY_WORKFLOW.id());
    workflowInitializer.store(HOURLY_WORKFLOW);
    verify(transaction).store(HOURLY_WORKFLOW);
    verify(transaction, never()).updateNextNaturalTrigger(any(), any());
  }

  @Test
  public void shouldFailToReadWorkflow() throws Exception {
    when(transaction.workflow(HOURLY_WORKFLOW.id())).thenThrow(new IOException("read error"));

    try {
      workflowInitializer.store(HOURLY_WORKFLOW);
      fail();
    } catch (RuntimeException e) {
      assertEquals("read error", e.getCause().getMessage());
    }
  }

  @Test
  public void shouldFailToStoreWorkflow() throws Exception {
    doThrow(new IOException("write error")).when(transaction).store(HOURLY_WORKFLOW);
    when(transaction.workflow(HOURLY_WORKFLOW.id())).thenReturn(Optional.of(HOURLY_WORKFLOW));

    try {
      workflowInitializer.store(HOURLY_WORKFLOW);
      fail();
    } catch (RuntimeException e) {
      assertEquals("write error", e.getCause().getMessage());
    }
  }

  @Test(expected = WorkflowInitializationException.class)
  public void shouldFailComputeNextTrigger() throws Exception {
    when(transaction.workflow(HOURLY_WORKFLOW.id()))
        .thenReturn(Optional.of(HOURLY_WORKFLOW));
    workflowInitializer.store(HOURLY_WORKFLOW_WITH_INVALID_OFFSET);
  }

  @Test
  public void shouldFailToUpdateNextNaturalTrigger() throws Exception {
    doThrow(new IOException("update error")).when(transaction)
        .updateNextNaturalTrigger(eq(HOURLY_WORKFLOW.id()), any());
    when(transaction.workflow(HOURLY_WORKFLOW.id()))
        .thenReturn(Optional.of(HOURLY_WORKFLOW));

    try {
      workflowInitializer.store(HOURLY_WORKFLOW_WITH_VALID_OFFSET);
      fail();
    } catch (RuntimeException e) {
      assertEquals("update error", e.getCause().getMessage());
    }
  }
}
