/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

package com.spotify.styx.model;

import com.spotify.styx.state.Message;
import com.spotify.styx.state.Trigger;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

public class EventVisitorAdapter<R> implements EventVisitor<R> {

  @Override
  public R triggerExecution(WorkflowInstance workflowInstance, Trigger trigger, TriggerParameters parameters) {
    return null;
  }

  @Override
  public R info(WorkflowInstance workflowInstance, Message message) {
    return null;
  }

  @Override
  public R dequeue(WorkflowInstance workflowInstance, Set<String> resourceIds) {
    return null;
  }

  @Override
  public R submit(WorkflowInstance workflowInstance, ExecutionDescription executionDescription,
                  @Nullable String executionId) {
    return null;
  }

  @Override
  public R submitted(WorkflowInstance workflowInstance, @Nullable String executionId) {
    return null;
  }

  @Override
  public R started(WorkflowInstance workflowInstance) {
    return null;
  }

  @Override
  public R terminate(WorkflowInstance workflowInstance, Optional<Integer> exitCode) {
    return null;
  }

  @Override
  public R runError(WorkflowInstance workflowInstance, String message) {
    return null;
  }

  @Override
  public R success(WorkflowInstance workflowInstance) {
    return null;
  }

  @Override
  public R retryAfter(WorkflowInstance workflowInstance, long delayMillis) {
    return null;
  }

  @Override
  public R stop(WorkflowInstance workflowInstance) {
    return null;
  }

  @Override
  public R timeout(WorkflowInstance workflowInstance) {
    return null;
  }

  @Override
  public R halt(WorkflowInstance workflowInstance) {
    return null;
  }

  @Override
  public R timeTrigger(WorkflowInstance workflowInstance) {
    return null;
  }

  @Override
  public R created(WorkflowInstance workflowInstance, String executionId, String dockerImage) {
    return null;
  }

  @Override
  public R retry(WorkflowInstance workflowInstance) {
    return null;
  }
}
