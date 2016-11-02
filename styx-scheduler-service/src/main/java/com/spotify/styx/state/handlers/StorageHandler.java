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

package com.spotify.styx.state.handlers;

import com.google.common.base.Throwables;
import com.spotify.styx.model.ExecutionStatus;
import com.spotify.styx.model.WorkflowExecutionInfo;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link OutputHandler} handler that stores execution states with the {@link Storage} interface.
 */
public class StorageHandler implements OutputHandler {

  private static final int MISSING_DEPS_STATUS_CODE = 20;

  private final Storage storage;
  private final Clock clock;

  public StorageHandler(Storage storage, Clock clock) {
    this.storage = Objects.requireNonNull(storage);
    this.clock = Objects.requireNonNull(clock);
  }

  @Override
  public void transitionInto(RunState state) {

    switch (state.state()) {
      case RUNNING:
        storeStatus(state.workflowInstance(), ExecutionStatus.STARTED, state.executionId());
        break;

      case TERMINATED:
        final int lastExit = state.lastExit();

        if (lastExit == 0) {
          storeStatus(state.workflowInstance(), ExecutionStatus.SUCCEEDED, state.executionId());
        } else if (lastExit == MISSING_DEPS_STATUS_CODE) {
          storeStatus(state.workflowInstance(), ExecutionStatus.MISSING_DEPS, state.executionId());
        } else {
          // todo: differentiate failure status?
          storeStatus(state.workflowInstance(), ExecutionStatus.FAILED, state.executionId());
        }
        break;

      case FAILED:
        storeStatus(state.workflowInstance(), ExecutionStatus.FAILED, state.executionId());
        break;

      case ERROR:
        storeStatus(state.workflowInstance(), ExecutionStatus.FATAL, state.executionId());

      default:
        // do nothing
    }
  }

  private void storeStatus(
      WorkflowInstance workflowInstance,
      ExecutionStatus executionStatus,
      Optional<String> executionId) {
    final WorkflowExecutionInfo executionInfo = WorkflowExecutionInfo.create(
        workflowInstance, Instant.now(clock), executionStatus, executionId);
    try {
      storage.store(executionInfo);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }
}
