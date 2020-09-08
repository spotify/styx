/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2020 Spotify AB
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

package com.spotify.styx.flyte;

import com.google.common.collect.ImmutableMap;
import flyteidl.core.Execution;
import java.util.Map;
import java.util.NoSuchElementException;

public enum FlytePhase {
  QUEUED,
  RUNNING,
  SUCCEEDING,
  SUCCEEDED,
  FAILING,
  FAILED,
  ABORTED,
  TIMED_OUT;

  private final static Map<Execution.WorkflowExecution.Phase, FlytePhase> phaseMapper =
    ImmutableMap.<Execution.WorkflowExecution.Phase, FlytePhase>builder()
        .put(Execution.WorkflowExecution.Phase.QUEUED, FlytePhase.QUEUED)
        .put(Execution.WorkflowExecution.Phase.RUNNING, FlytePhase.RUNNING)
        .put(Execution.WorkflowExecution.Phase.SUCCEEDING, FlytePhase.SUCCEEDING)
        .put(Execution.WorkflowExecution.Phase.SUCCEEDED, FlytePhase.SUCCEEDED)
        .put(Execution.WorkflowExecution.Phase.FAILING, FlytePhase.FAILING)
        .put(Execution.WorkflowExecution.Phase.FAILED, FlytePhase.FAILED)
        .put(Execution.WorkflowExecution.Phase.ABORTED, FlytePhase.ABORTED)
        .put(Execution.WorkflowExecution.Phase.TIMED_OUT, FlytePhase.TIMED_OUT)
      .build();

  public static FlytePhase fromProto(Execution.WorkflowExecution.Phase phase) {
    if (!phaseMapper.containsKey(phase)) {
      throw new NoSuchElementException();
    }
    return phaseMapper.get(phase);
  }
}

