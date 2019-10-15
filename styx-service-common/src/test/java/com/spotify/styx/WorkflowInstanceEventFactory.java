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

package com.spotify.styx;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.Trigger;
import java.util.Optional;
import java.util.Set;

public class WorkflowInstanceEventFactory {

  public static final String TEST_RUNNER_ID = "test";

  private final WorkflowInstance workflowInstance;

  public WorkflowInstanceEventFactory(WorkflowInstance workflowInstance) {
    this.workflowInstance = workflowInstance;
  }

  public Event timeTrigger() {
    return Event.timeTrigger(workflowInstance);
  }

  public Event triggerExecution(Trigger trigger) {
    return triggerExecution(trigger, TriggerParameters.zero());
  }

  public Event triggerExecution(Trigger trigger, TriggerParameters parameters) {
    return Event.triggerExecution(workflowInstance, trigger, parameters);
  }

  public Event info(Message message) {
    return Event.info(workflowInstance, message);
  }

  public Event created(String executionId, String dockerImage) {
    return Event.created(workflowInstance, executionId, dockerImage);
  }

  public Event dequeue(Set<String> resourceIds) {
    return Event.dequeue(workflowInstance, resourceIds);
  }

  public Event submit(ExecutionDescription executionDescription, String executionId) {
    return Event.submit(workflowInstance, executionDescription, executionId);
  }

  public Event submitted(String executionId) {
    return Event.submitted(workflowInstance, executionId, TEST_RUNNER_ID);
  }

  public Event started() {
    return Event.started(workflowInstance);
  }

  public Event terminate(Optional<Integer> exitCode) {
    return Event.terminate(workflowInstance, exitCode);
  }

  public Event terminate(int exitCode) {
    return Event.terminate(workflowInstance, Optional.of(exitCode));
  }

  public Event runError(String message) {
    return Event.runError(workflowInstance, message);
  }

  public Event success() {
    return Event.success(workflowInstance);
  }

  public Event retryAfter(int delayMillis) {
    return Event.retryAfter(workflowInstance, delayMillis);
  }

  public Event retry() {
    return Event.retry(workflowInstance);
  }

  public Event stop() {
    return Event.stop(workflowInstance);
  }

  public Event halt() {
    return Event.halt(workflowInstance);
  }

  public Event timeout() {
    return Event.timeout(workflowInstance);
  }
}
