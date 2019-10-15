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

package com.spotify.styx.model;

import com.github.sviperll.adt4j.GenerateValueClassForVisitor;
import com.github.sviperll.adt4j.Getter;
import com.github.sviperll.adt4j.Visitor;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.Trigger;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Generated {@link Event} ADT for all events that can be received by RunState
 */
@GenerateValueClassForVisitor(isPublic = true)
@Visitor(resultVariableName = "R")
public interface EventVisitor<R> {

  R triggerExecution(@Getter WorkflowInstance workflowInstance, Trigger trigger, TriggerParameters parameters);
  R info(@Getter WorkflowInstance workflowInstance, Message message);
  R dequeue(@Getter WorkflowInstance workflowInstance, Set<String> resourceIds);
  R submit(@Getter WorkflowInstance workflowInstance, ExecutionDescription executionDescription,
      @Nullable String executionId);
  R submitted(@Getter WorkflowInstance workflowInstance, @Nullable String executionId, @Nullable String runnerId);
  R started(@Getter WorkflowInstance workflowInstance);
  R terminate(@Getter WorkflowInstance workflowInstance, Optional<Integer> exitCode);
  R runError(@Getter WorkflowInstance workflowInstance, String message);
  R success(@Getter WorkflowInstance workflowInstance);
  R retryAfter(@Getter WorkflowInstance workflowInstance, long delayMillis);
  R stop(@Getter WorkflowInstance workflowInstance);
  R timeout(@Getter WorkflowInstance workflowInstance);
  R halt(@Getter WorkflowInstance workflowInstance);

  // Note: Do not make changes to these deprecated event method signatures
  @Deprecated
  R timeTrigger(@Getter WorkflowInstance workflowInstance);
  @Deprecated
  R created(@Getter WorkflowInstance workflowInstance, String executionId, String dockerImage);
  @Deprecated
  R retry(@Getter WorkflowInstance workflowInstance);
}
