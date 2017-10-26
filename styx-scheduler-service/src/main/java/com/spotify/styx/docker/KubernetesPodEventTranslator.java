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

package com.spotify.styx.docker;

import static com.spotify.styx.docker.DockerRunner.LOG;
import static com.spotify.styx.docker.KubernetesDockerRunner.DOCKER_TERMINATION_LOGGING_ANNOTATION;
import static java.util.Collections.emptyList;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.RunState;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.Watcher.Action;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import okio.ByteString;

public final class KubernetesPodEventTranslator {

  private KubernetesPodEventTranslator() {
  }

  private static final Predicate<ContainerStatus> IS_STYX_CONTAINER =
      (cs) -> KubernetesDockerRunner.STYX_RUN.equals(cs.getName());

  @JsonIgnoreProperties(ignoreUnknown = true)
  private static class TerminationLogMessage {
    int exitCode;

    @JsonCreator
    public TerminationLogMessage(
        @JsonProperty(value = "exit_code", required = true) int exitCode
    ) {
      this.exitCode = exitCode;
    }
  }

  private static Optional<Integer> getExitCodeIfValid(String workflowInstance,
                                                      Pod pod,
                                                      ContainerStatus status,
                                                      Stats stats) {
    final ContainerStateTerminated terminated = status.getState().getTerminated();

    // Check termination log exit code, if available
    if ("true".equals(pod.getMetadata().getAnnotations().get(DOCKER_TERMINATION_LOGGING_ANNOTATION))) {
      if (terminated.getMessage() == null) {
        LOG.warn("Missing termination log message for workflow instance {} container {}",
                 workflowInstance, status.getContainerID());
        stats.recordTerminationLogMissing();
      } else {
        try {
          // TODO: handle multiple termination log messages
          final TerminationLogMessage message = Json.deserialize(
              ByteString.encodeUtf8(terminated.getMessage()), TerminationLogMessage.class);

          if (!Objects.equals(message.exitCode, terminated.getExitCode())) {
            LOG.warn("Exit code mismatch for workflow instance {} container {}. Container exit code: {}. "
                + "Termination log exit code: {}",
                workflowInstance, status.getContainerID(), terminated.getExitCode(),
                message.exitCode);
            stats.recordExitCodeMismatch();
          }

          if (terminated.getExitCode() != null && message.exitCode == 0) {
            // If we have a non-zero container exit code but a zero termination log exit code,
            // return the container exit code to indicate failure. This guards against jobs that
            // incorrectly write a successful exit code to the termination log _before_ running
            // the actual job, which then fails. We could then still incorrectly get a zero exit
            // code from docker, but there is not a lot we can do about that.
            return Optional.of(terminated.getExitCode());
          } else {
            return Optional.of(message.exitCode);
          }
        } catch (IOException e) {
          stats.recordTerminationLogInvalid();
          LOG.warn("Unexpected termination log message for workflow instance {} container {}",
              workflowInstance, status.getContainerID(), e);
        }
      }

      // If there's no termination log exit code, fall back to k8s exit code if it is not zero.
      // Rationale: It is important for users to be able to get the exit code of the container to be
      // able to debug failures, but at the same time we must be careful about using the use the k8s
      // exit code when checking whether the execution was successful as dockerd some times returns
      // incorrect exit codes.
      // TODO: consider separating execution status and debugging info in the "terminate" event.
      if (terminated.getExitCode() != null && terminated.getExitCode() != 0) {
        return Optional.of(terminated.getExitCode());
      } else {
        return Optional.empty();
      }
    }

    // No termination log expected, use k8s exit code
    if (terminated.getExitCode() == null) {
      LOG.warn("Missing exit code for workflow instance {} container {}", workflowInstance,
               status.getContainerID());
      return Optional.empty();
    } else {
      return Optional.of(terminated.getExitCode());
    }
  }

  static List<Event> translate(
      WorkflowInstance workflowInstance,
      RunState state,
      Action action,
      Pod pod,
      Stats stats) {

    if (action == Watcher.Action.DELETED) {
      return emptyList();
    }

    final List<Event> generatedEvents = Lists.newArrayList();
    final Optional<Event> hasError = isInErrorState(workflowInstance, pod);

    if (hasError.isPresent()) {
      switch (state.state()) {
        case PREPARE:
        case SUBMITTED:
        case RUNNING:
          generatedEvents.add(hasError.get());

        default:
          // no event
      }

      return generatedEvents;
    }

    final PodStatus status = pod.getStatus();
    final String phase = status.getPhase();

    boolean exited = false;
    boolean started = false;
    Optional<Integer> exitCode = Optional.empty();

    switch (phase) {
      case "Running":
        // check that the styx container is ready
        started = status.getContainerStatuses().stream()
            .anyMatch(IS_STYX_CONTAINER.and(ContainerStatus::getReady));
        break;

      case "Succeeded":
      case "Failed":
        exited = true;
        exitCode = pod.getStatus().getContainerStatuses().stream()
            .filter(IS_STYX_CONTAINER)
            .map(cs -> getExitCodeIfValid(
                workflowInstance.toKey(),
                pod,
                cs, stats))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .findFirst();
        break;

      default:
        // do nothing
    }

    if (started) {
      switch (state.state()) {
        case PREPARE:
        case SUBMITTED:
          generatedEvents.add(Event.started(workflowInstance));

        default:
          // no event
      }
    }

    if (exited) {
      switch (state.state()) {
        case PREPARE:
        case SUBMITTED:
          generatedEvents.add(Event.started(workflowInstance));
          // intentional fall-through

        case RUNNING:
          generatedEvents.add(Event.terminate(workflowInstance, exitCode));
          break;

        default:
          // no event
      }
    }

    return generatedEvents;
  }

  private static Optional<Event> isInErrorState(WorkflowInstance workflowInstance, Pod pod) {
    final PodStatus status = pod.getStatus();
    final String phase = status.getPhase();
    final String reason = status.getReason();

    if ("NodeLost".equals(reason)) {
      LOG.warn("Kubernetes node {} became unresponsive", pod.getSpec().getNodeName());
      return Optional.of(Event.runError(workflowInstance, "Kubernetes node became unresponsive"));
    }

    switch (phase) {
      case "Pending":
        // check if one or more docker contains failed to pull their image, a possible silent error
        return status.getContainerStatuses().stream()
            .filter(IS_STYX_CONTAINER.and(KubernetesPodEventTranslator::hasPullImageError))
            .findAny()
            .map((x) -> Event.runError(workflowInstance, "One or more containers failed to pull their image"));

      case "Succeeded":
      case "Failed":
        final Optional<ContainerStatus> containerStatusOpt =
            pod.getStatus().getContainerStatuses().stream()
                .filter(IS_STYX_CONTAINER)
                .findFirst();

        if (!containerStatusOpt.isPresent()) {
          return Optional.of(Event.runError(workflowInstance, "Could not find our container in pod"));
        }

        final ContainerStatus containerStatus = containerStatusOpt.get();
        final ContainerStateTerminated terminated = containerStatus.getState().getTerminated();
        if (terminated == null) {
          return Optional.of(Event.runError(workflowInstance, "Unexpected null terminated status"));
        }
        return Optional.empty();

      case "Unknown":
        return Optional.of(Event.runError(workflowInstance, "Pod entered Unknown phase"));

      default:
        return Optional.empty();
    }
  }

  static boolean hasPullImageError(ContainerStatus cs) {
    ContainerStateWaiting waiting = cs.getState().getWaiting();
    return waiting != null && (
        "PullImageError".equals(waiting.getReason())
        || "ErrImagePull".equals(waiting.getReason())
        || "ImagePullBackOff".equals(waiting.getReason()));
  }
}
