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

package com.spotify.styx.serialization;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.google.common.collect.ImmutableSet;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventVisitor;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.Trigger;
import java.util.Optional;
import java.util.Set;

@JsonTypeInfo(use = Id.NAME, visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = PersistentEvent.class, name = "timeTrigger"),
    @JsonSubTypes.Type(value = PersistentEvent.TriggerExecution.class, name = "triggerExecution"),
    @JsonSubTypes.Type(value = PersistentEvent.Info.class, name = "info"),
    @JsonSubTypes.Type(value = PersistentEvent.Created.class, name = "created"),
    @JsonSubTypes.Type(value = PersistentEvent.Dequeue.class, name = "dequeue"),
    @JsonSubTypes.Type(value = PersistentEvent.Started.class, name = "started"),
    @JsonSubTypes.Type(value = PersistentEvent.Terminate.class, name = "terminate"),
    @JsonSubTypes.Type(value = PersistentEvent.RunError.class, name = "runError"),
    @JsonSubTypes.Type(value = PersistentEvent.RetryAfter.class, name = "retryAfter"),
    @JsonSubTypes.Type(value = PersistentEvent.class, name = "success"),
    @JsonSubTypes.Type(value = PersistentEvent.class, name = "retry"),
    @JsonSubTypes.Type(value = PersistentEvent.class, name = "stop"),
    @JsonSubTypes.Type(value = PersistentEvent.class, name = "timeout"),
    @JsonSubTypes.Type(value = PersistentEvent.class, name = "halt"),
    @JsonSubTypes.Type(value = PersistentEvent.Submit.class, name = "submit"),
    @JsonSubTypes.Type(value = PersistentEvent.Submitted.class, name = "submitted")
    })
@JsonInclude(Include.NON_ABSENT)
class PersistentEvent {

  private static final SerializerVisitor SERIALIZER_VISITOR = new SerializerVisitor();

  public static PersistentEvent wrap(Event event) {
    return event.accept(SERIALIZER_VISITOR);
  }

  public static class SerializerVisitor implements EventVisitor<PersistentEvent> {

    @Override
    public PersistentEvent timeTrigger(WorkflowInstance workflowInstance) {
      return new PersistentEvent("timeTrigger", workflowInstance.toKey());
    }

    @Override
    public PersistentEvent triggerExecution(WorkflowInstance workflowInstance, Trigger trigger) {
      return new TriggerExecution(workflowInstance.toKey(), Optional.of(trigger));
    }

    @Override
    public PersistentEvent info(WorkflowInstance workflowInstance, Message message) {
      return new Info(workflowInstance.toKey(), message);
    }

    @Override
    public PersistentEvent created(WorkflowInstance workflowInstance, String executionId, String dockerImage) {
      return new Created(workflowInstance.toKey(), executionId, Optional.of(dockerImage));
    }

    @Override
    public PersistentEvent dequeue(WorkflowInstance workflowInstance, Set<String> resources) {
      return new Dequeue(workflowInstance.toKey(), Optional.of(resources));
    }

    @Override
    public PersistentEvent submit(WorkflowInstance workflowInstance, ExecutionDescription executionDescription,
        String executionId) {
      return new Submit(workflowInstance.toKey(), executionDescription, executionId);
    }

    @Override
    public PersistentEvent submitted(WorkflowInstance workflowInstance, String executionId) {
      return new Submitted(workflowInstance.toKey(), executionId);
    }

    @Override
    public PersistentEvent started(WorkflowInstance workflowInstance) {
      return new Started(workflowInstance.toKey());
    }

    @Override
    public PersistentEvent terminate(WorkflowInstance workflowInstance, Optional<Integer> exitCode) {
      return new Terminate(workflowInstance.toKey(), exitCode);
    }

    @Override
    public PersistentEvent runError(WorkflowInstance workflowInstance, String message) {
      return new RunError(workflowInstance.toKey(), message);
    }

    @Override
    public PersistentEvent success(WorkflowInstance workflowInstance) {
      return new PersistentEvent("success", workflowInstance.toKey());
    }

    @Override
    public PersistentEvent retryAfter(WorkflowInstance workflowInstance, long delayMillis) {
      return new RetryAfter(workflowInstance.toKey(), delayMillis);
    }

    @Override
    public PersistentEvent retry(WorkflowInstance workflowInstance) {
      return new PersistentEvent("retry", workflowInstance.toKey());
    }

    @Override
    public PersistentEvent stop(WorkflowInstance workflowInstance) {
      return new PersistentEvent("stop", workflowInstance.toKey());
    }

    @Override
    public PersistentEvent timeout(WorkflowInstance workflowInstance) {
      return new PersistentEvent("timeout", workflowInstance.toKey());
    }

    @Override
    public PersistentEvent halt(WorkflowInstance workflowInstance) {
      return new PersistentEvent("halt", workflowInstance.toKey());
    }
  }

  @JsonTypeId
  @JsonProperty("@type") // from Id.NAME
  public final String type;
  public final String workflowInstance;

  @JsonCreator
  PersistentEvent(
      @JsonProperty("@type") String type,
      @JsonProperty("workflow_instance") String workflowInstance) {
    this.type = type;
    this.workflowInstance = workflowInstance;
  }

  public Event toEvent() {
    final WorkflowInstance workflowInstance = WorkflowInstance.parseKey(this.workflowInstance);
    switch (type) {
      case "timeTrigger":
        return Event.timeTrigger(workflowInstance);
      case "success":
        return Event.success(workflowInstance);
      case "retry":
        return Event.retry(workflowInstance);
      case "stop":
        return Event.stop(workflowInstance);
      case "timeout":
        return Event.timeout(workflowInstance);
      case "halt":
        return Event.halt(workflowInstance);

      default:
        throw new IllegalStateException("Event type " + type + " not covered by base PersistentEvent class");
    }
  }

  public static class TriggerExecution extends PersistentEvent {

    public final Optional<String> triggerId; //for backwards compatibility
    public final Optional<Trigger> trigger;

    @JsonCreator
    public TriggerExecution(
        @JsonProperty("workflow_instance") String workflowInstance,
        @JsonProperty("trigger") Optional<Trigger> trigger) {
      super("triggerExecution", workflowInstance);
      this.triggerId = Optional.empty();
      this.trigger = trigger;
    }

    @Override
    public Event toEvent() {
      if (trigger.isPresent()) {
        return Event.triggerExecution(WorkflowInstance.parseKey(workflowInstance), trigger.get());
      } else if (triggerId.isPresent()) {
        return Event.triggerExecution(WorkflowInstance.parseKey(workflowInstance), Trigger.unknown(triggerId.get()));
      } else {
        return Event.triggerExecution(WorkflowInstance.parseKey(workflowInstance), Trigger.unknown("UNKNOWN"));
      }
    }
  }

  public static class Info extends PersistentEvent {

    public final Message message;

    @JsonCreator
    public Info(
        @JsonProperty("workflow_instance") String workflowInstance,
        @JsonProperty("message") Message message) {
      super("info", workflowInstance);
      this.message = message;
    }

    @Override
    public Event toEvent() {
      return Event.info(WorkflowInstance.parseKey(workflowInstance), message);
    }
  }


  public static class Created extends PersistentEvent {

    public final String executionId;
    public final String dockerImage;

    @JsonCreator
    public Created(
        @JsonProperty("workflow_instance") String workflowInstance,
        @JsonProperty("execution_id") String executionId,
        @JsonProperty("docker_image") Optional<String> dockerImage) {
      super("created", workflowInstance);
      this.executionId = executionId;
      this.dockerImage = dockerImage.orElse("UNKNOWN");
    }

    @Override
    public Event toEvent() {
      return Event.created(WorkflowInstance.parseKey(workflowInstance), executionId, dockerImage);
    }
  }

  public static class Dequeue extends PersistentEvent {

    public final Set<String> resources;

    @JsonCreator
    public Dequeue(
        @JsonProperty("workflow_instance") String workflowInstance,
        @JsonProperty("resources") Optional<Set<String>> resources) {
      super("dequeue", workflowInstance);
      this.resources = resources.orElse(ImmutableSet.of());
    }

    @Override
    public Event toEvent() {
      return Event.dequeue(WorkflowInstance.parseKey(workflowInstance), resources);
    }
  }

  public static class Submitted extends PersistentEvent {

    public final String executionId;

    @JsonCreator
    public Submitted(
        @JsonProperty("workflow_instance") String workflowInstance,
        @JsonProperty("execution_id") String executionId) {
      super("submitted", workflowInstance);
      this.executionId = executionId;
    }

    @Override
    public Event toEvent() {
      return Event.submitted(WorkflowInstance.parseKey(workflowInstance), executionId);
    }
  }

  public static class Started extends PersistentEvent {

    public final Optional<String> podName; // for backwards compatibility

    @JsonCreator
    public Started(
        @JsonProperty("workflow_instance") String workflowInstance) {
      super("started", workflowInstance);
      this.podName = Optional.empty();
    }

    @Override
    public Event toEvent() {
      return Event.started(WorkflowInstance.parseKey(workflowInstance));
    }
  }

  public static class Terminate extends PersistentEvent {

    public final Optional<Integer> exitCode;

    @JsonCreator
    public Terminate(
        @JsonProperty("workflow_instance") String workflowInstance,
        @JsonProperty("exit_code") Optional<Integer> exitCode) {
      super("terminate", workflowInstance);
      this.exitCode = exitCode;
    }

    @Override
    public Event toEvent() {
      return Event.terminate(WorkflowInstance.parseKey(workflowInstance), exitCode);
    }
  }

  public static class RunError extends PersistentEvent {

    public final String message;

    @JsonCreator
    public RunError(
        @JsonProperty("workflow_instance") String workflowInstance,
        @JsonProperty("message") String message) {
      super("runError", workflowInstance);
      this.message = message;
    }

    @Override
    public Event toEvent() {
      return Event.runError(WorkflowInstance.parseKey(workflowInstance), message);
    }
  }

  public static class RetryAfter extends PersistentEvent {

    public final long delayMillis;

    @JsonCreator
    public RetryAfter(
        @JsonProperty("workflow_instance") String workflowInstance,
        @JsonProperty("delay_millis") long delayMillis) {
      super("retryAfter", workflowInstance);
      this.delayMillis = delayMillis;
    }

    @Override
    public Event toEvent() {
      return Event.retryAfter(WorkflowInstance.parseKey(workflowInstance), delayMillis);
    }
  }

  public static class Submit extends PersistentEvent {

    public final ExecutionDescription executionDescription;
    public final String executionId;

    @JsonCreator
    public Submit(
        @JsonProperty("workflow_instance") String workflowInstance,
        @JsonProperty("execution_description") ExecutionDescription executionDescription,
        @JsonProperty("execution_id") String executionId) {
      super("submit", workflowInstance);
      this.executionDescription = executionDescription;
      this.executionId = executionId;
    }

    @Override
    public Event toEvent() {
      return Event.submit(WorkflowInstance.parseKey(workflowInstance), executionDescription, executionId);
    }
  }
}
