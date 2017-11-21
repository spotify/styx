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

package com.spotify.styx.workflow;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import com.spotify.styx.model.Workflow;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.testdata.TestData;
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

  private final Workflow HOURLY_WORKFLOW = Workflow.create("styx",
                                                           TestData.HOURLY_WORKFLOW_CONFIGURATION);
  private final Workflow HOURLY_WORKFLOW_WITH_INVALID_OFFSET =
      Workflow.create("styx", TestData.HOURLY_WORKFLOW_CONFIGURATION_WITH_INVALID_OFFSET);

  private final Workflow HOURLY_WORKFLOW_WITH_VALID_OFFSET =
      Workflow.create("styx", TestData.HOURLY_WORKFLOW_CONFIGURATION_WITH_VALID_OFFSET);

  private WorkflowInitializer workflowInitializer;

  @Mock
  private Storage storage;

  @Before
  public void setUp() {
    workflowInitializer = new WorkflowInitializer(storage,
                                                  () -> Instant.parse("2015-12-31T23:59:10.000Z"));
  }

  @Test
  public void shouldFailToReadWorkflow() throws Exception {
    when(storage.workflow(HOURLY_WORKFLOW.id())).thenThrow(new IOException("read error"));

    try {
      workflowInitializer.inspectChange(HOURLY_WORKFLOW);
      fail();
    } catch (RuntimeException e) {
      assertEquals("read error", e.getCause().getMessage());
    }
  }

  @Test
  public void shouldFailToStoreWorkflow() throws Exception {
    doThrow(new IOException("write error")).when(storage).storeWorkflow(HOURLY_WORKFLOW);
    when(storage.workflow(HOURLY_WORKFLOW.id())).thenReturn(Optional.of(HOURLY_WORKFLOW));

    try {
      workflowInitializer.inspectChange(HOURLY_WORKFLOW);
      fail();
    } catch (RuntimeException e) {
      assertEquals("write error", e.getCause().getMessage());
    }
  }

  @Test(expected = WorkflowInitializationException.class)
  public void shouldFailComputeNextTrigger() throws Exception {
    when(storage.workflow(HOURLY_WORKFLOW.id()))
        .thenReturn(Optional.of(HOURLY_WORKFLOW));
    workflowInitializer.inspectChange(HOURLY_WORKFLOW_WITH_INVALID_OFFSET);
  }

  @Test
  public void shouldFailToUpdateNextNaturalTrigger() throws Exception {
    doThrow(new IOException("update error")).when(storage)
        .updateNextNaturalTrigger(eq(HOURLY_WORKFLOW.id()), any());
    when(storage.workflow(HOURLY_WORKFLOW.id()))
        .thenReturn(Optional.of(HOURLY_WORKFLOW));

    try {
      workflowInitializer.inspectChange(HOURLY_WORKFLOW_WITH_VALID_OFFSET);
      fail();
    } catch (RuntimeException e) {
      assertEquals("update error", e.getCause().getMessage());
    }
  }
}
