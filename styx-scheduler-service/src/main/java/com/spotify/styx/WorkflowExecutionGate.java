/*
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx;


import com.spotify.styx.model.WorkflowInstance;
import io.norberg.automatter.AutoMatter;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * A gating mechanism that can block workflow instance execution. Can be implemented in order to
 * look up execution preconditions and e.g. delay workflow execution when input dependencies are
 * missing.
 */
public interface WorkflowExecutionGate {

  CompletableFuture<Optional<ExecutionBlocker>> NO_BLOCKER =
      CompletableFuture.completedFuture(Optional.empty());

  /**
   * A nop {@link WorkflowExecutionGate} that never returns any missing dependencies. I.e., the
   * execution can always proceed.
   */
  WorkflowExecutionGate NOOP = wfi -> NO_BLOCKER;

  /**
   * Check if there is a blocker for the execution of a workflow instance. This method is called
   * before the workflow instance is dequeued. If an {@link ExecutionBlocker} is returned, execution
   * of the workflow instance is delayed. Implementations of this method should be careful to
   * consider whether information gathered e.g. from previous executions has been invalidated by
   * docker image and/or args changes, etc.
   *
   * <p>This method must not block.
   *
   * @param instance The workflow instance to check.
   * @return A future with an optional blocker.
   */
  CompletionStage<Optional<ExecutionBlocker>> executionBlocker(WorkflowInstance instance);

  @AutoMatter
  interface ExecutionBlocker {

    /**
     * The reason for this execution to be blocked.
     */
    String reason();

    /**
     * How long to wait before attempting to execute the worklow instance again.
     */
    Duration delay();

    static ExecutionBlocker of(String reason, Duration delay) {
      return new ExecutionBlockerBuilder()
          .reason(reason)
          .delay(delay)
          .build();
    }
  }
}
