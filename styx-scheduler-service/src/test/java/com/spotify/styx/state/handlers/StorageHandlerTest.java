/*
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
package com.spotify.styx.state.handlers;

import com.spotify.styx.model.ExecutionStatus;
import com.spotify.styx.model.WorkflowExecutionInfo;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.storage.Storage;

import org.junit.Before;
import org.junit.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class StorageHandlerTest {

  private static final WorkflowInstance WORKFLOW_INSTANCE = WorkflowInstance.create(
      WorkflowId.create("component", "endpoint"), "2016-03-24T16");

  private Storage storage;
  private OutputHandler outputHandler;

  private static final Clock FIXED_CLOCK = Clock.fixed(Instant.now(), ZoneId.of("UTC"));;
  private static final String TEST_EXECUTION_ID = "execution_1";

  @Before
  public void setUp() throws Exception {
    storage = mock(Storage.class);
    outputHandler = new StorageHandler(storage, FIXED_CLOCK);
  }

  @Test
  public void shouldStoreStartedOnStart() throws Exception {
    RunState started = RunState.create(WORKFLOW_INSTANCE, TEST_EXECUTION_ID, RunState.State.RUNNING);
    outputHandler.transitionInto(started);

    WorkflowExecutionInfo expectedWorkflowExecutionInfoToStore =
        WorkflowExecutionInfo.create(
            WORKFLOW_INSTANCE, Instant.now(FIXED_CLOCK), ExecutionStatus.STARTED, Optional.of(TEST_EXECUTION_ID));
    verify(storage, times(1)).store(expectedWorkflowExecutionInfoToStore);
  }

  @Test
  public void shouldStoreFailedFail() throws Exception {
    RunState failed = RunState.create(WORKFLOW_INSTANCE, RunState.State.FAILED);
    outputHandler.transitionInto(failed);

    WorkflowExecutionInfo expectedWorkflowExecutionInfoToStore =
        WorkflowExecutionInfo.create(
            WORKFLOW_INSTANCE, Instant.now(FIXED_CLOCK), ExecutionStatus.FAILED, Optional.empty());
    verify(storage, times(1)).store(expectedWorkflowExecutionInfoToStore);
  }

  @Test
  public void shouldStoreSucceededOnZeroTerm() throws Exception {
    RunState zeroTerm = RunState.create(WORKFLOW_INSTANCE, RunState.State.TERMINATED, 1, 0);
    outputHandler.transitionInto(zeroTerm);

    WorkflowExecutionInfo expectedWorkflowExecutionInfoToStore =
        WorkflowExecutionInfo.create(
            WORKFLOW_INSTANCE, Instant.now(FIXED_CLOCK), ExecutionStatus.SUCCEEDED, Optional.empty());
    verify(storage, times(1)).store(expectedWorkflowExecutionInfoToStore);
  }

  @Test
  public void shouldStoreMissingDepsOn20Term() throws Exception {
    RunState twentyTerm = RunState.create(WORKFLOW_INSTANCE, RunState.State.TERMINATED, 1, 20);
    outputHandler.transitionInto(twentyTerm);

    WorkflowExecutionInfo expectedWorkflowExecutionInfoToStore =
        WorkflowExecutionInfo.create(
            WORKFLOW_INSTANCE, Instant.now(FIXED_CLOCK), ExecutionStatus.MISSING_DEPS, Optional.empty());
    verify(storage, times(1)).store(expectedWorkflowExecutionInfoToStore);
  }

  @Test
  public void shouldStoreFailedOnOtherTerm() throws Exception {
    RunState otherTerm = RunState.create(WORKFLOW_INSTANCE, RunState.State.TERMINATED, 1, 1);
    outputHandler.transitionInto(otherTerm);

    WorkflowExecutionInfo expectedWorkflowExecutionInfoToStore =
        WorkflowExecutionInfo.create(
            WORKFLOW_INSTANCE, Instant.now(FIXED_CLOCK), ExecutionStatus.FAILED, Optional.empty());
    verify(storage, times(1)).store(expectedWorkflowExecutionInfoToStore);
  }

  @Test
  public void shouldStoreFatalOnFatal() throws Exception {
    RunState error = RunState.create(WORKFLOW_INSTANCE, RunState.State.ERROR);
    outputHandler.transitionInto(error);

    WorkflowExecutionInfo expectedWorkflowExecutionInfoToStore =
        WorkflowExecutionInfo.create(
            WORKFLOW_INSTANCE, Instant.now(FIXED_CLOCK), ExecutionStatus.FATAL, Optional.empty());
    verify(storage, times(1)).store(expectedWorkflowExecutionInfoToStore);
  }
}
