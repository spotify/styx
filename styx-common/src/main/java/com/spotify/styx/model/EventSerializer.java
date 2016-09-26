/*
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

import com.google.common.base.Throwables;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;

import java.io.IOException;
import java.util.Optional;

import okio.ByteString;

public final class EventSerializer {

  private static final SerializerVisitor SERIALIZER_VISITOR = new SerializerVisitor();
  private static final ObjectMapper MAPPER = new ObjectMapper()
      .setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE)
      .registerModule(new Jdk8Module());

  public static PersistentEvent convertEventToPersistentEvent(Event event) {
    return event.accept(SERIALIZER_VISITOR);
  }

  public ByteString convert(Event event) {
    return serialize(convertEventToPersistentEvent(event));
  }

  public Event convert(ByteString event) {
    try {
      PersistentEvent persistentEvent = MAPPER.readValue(event.toByteArray(), PersistentEvent.class);
      return persistentEvent.toEvent();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private ByteString serialize(PersistentEvent persistentEvent) {
    try {
      return ByteString.of(MAPPER.writeValueAsBytes(persistentEvent));
    } catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  public static class SerializerVisitor implements EventVisitor<PersistentEvent> {

    @Override
    public PersistentEvent timeTrigger(WorkflowInstance workflowInstance) {
      return new PersistentEvent("timeTrigger", workflowInstance.toKey());
    }

    @Override
    public PersistentEvent triggerExecution(WorkflowInstance workflowInstance, String triggerId) {
      return new TriggerExecution(workflowInstance.toKey(), Optional.of(triggerId));
    }

    @Override
    public PersistentEvent created(WorkflowInstance workflowInstance, String executionId, String dockerImage) {
      return new Created(workflowInstance.toKey(), executionId, Optional.of(dockerImage));
    }

    @Override
    public PersistentEvent submit(WorkflowInstance workflowInstance, ExecutionDescription executionDescription) {
      return new Submit(workflowInstance.toKey(), executionDescription);
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
    public PersistentEvent terminate(WorkflowInstance workflowInstance, int exitCode) {
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

  @JsonTypeInfo(use = Id.NAME, visible = true)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = PersistentEvent.class, name = "timeTrigger"),
      @JsonSubTypes.Type(value = TriggerExecution.class, name = "triggerExecution"),
      @JsonSubTypes.Type(value = Created.class, name = "created"),
      @JsonSubTypes.Type(value = Started.class, name = "started"),
      @JsonSubTypes.Type(value = Terminate.class, name = "terminate"),
      @JsonSubTypes.Type(value = RunError.class, name = "runError"),
      @JsonSubTypes.Type(value = RetryAfter.class, name = "retryAfter"),
      @JsonSubTypes.Type(value = PersistentEvent.class, name = "success"),
      @JsonSubTypes.Type(value = PersistentEvent.class, name = "retry"),
      @JsonSubTypes.Type(value = PersistentEvent.class, name = "stop"),
      @JsonSubTypes.Type(value = PersistentEvent.class, name = "timeout"),
      @JsonSubTypes.Type(value = PersistentEvent.class, name = "halt"),
      @JsonSubTypes.Type(value = Submit.class, name = "submit"),
      @JsonSubTypes.Type(value = Submitted.class, name = "submitted")
  })
  @JsonInclude(Include.NON_ABSENT)
  public static class PersistentEvent {

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
  }

  public static class TriggerExecution extends PersistentEvent {

    public final String triggerId;

    @JsonCreator
    public TriggerExecution(
        @JsonProperty("workflow_instance") String workflowInstance,
        @JsonProperty("trigger_id") Optional<String> triggerId) {
      super("triggerExecution", workflowInstance);
      this.triggerId = triggerId.orElse("UNKNOWN");
    }

    @Override
    public Event toEvent() {
      return Event.triggerExecution(WorkflowInstance.parseKey(workflowInstance), triggerId);
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

    public final int exitCode;

    @JsonCreator
    public Terminate(
        @JsonProperty("workflow_instance") String workflowInstance,
        @JsonProperty("exit_code") int exitCode) {
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

    @JsonCreator
    public Submit(
        @JsonProperty("workflow_instance") String workflowInstance,
        @JsonProperty("execution_description") ExecutionDescription executionDescription) {
      super("submit", workflowInstance);
      this.executionDescription = executionDescription;
    }

    @Override
    public Event toEvent() {
      return Event.submit(WorkflowInstance.parseKey(workflowInstance), executionDescription);
    }
  }
}
