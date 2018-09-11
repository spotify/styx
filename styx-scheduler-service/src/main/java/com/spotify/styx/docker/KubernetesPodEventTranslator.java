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
import static com.spotify.styx.docker.KubernetesDockerRunner.getMainContainerStatus;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.RunState;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import okio.ByteString;

final class KubernetesPodEventTranslator {

  private KubernetesPodEventTranslator() {
    throw new UnsupportedOperationException();
  }

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
      // there are cases k8s marks the pod failed but with exitCode 0
      if ("Failed".equals(pod.getStatus().getPhase()) && terminated.getExitCode() == 0) {
        return Optional.empty();
      }
      return Optional.of(terminated.getExitCode());
    }
  }

  // TODO: fix NPath complexity
  static List<Event> translate(
      WorkflowInstance workflowInstance,
      RunState state,
      Pod pod,
      Stats stats) {

    final List<Event> generatedEvents = Lists.newArrayList();
    final Optional<Event> hasError = isInErrorState(workflowInstance, pod);

    if (hasError.isPresent()) {
      switch (state.state()) {
        case PREPARE:
        case SUBMITTED:
        case RUNNING:
          generatedEvents.add(hasError.get());
          break;

        default:
          // no event
          break;
      }

      return generatedEvents;
    }

    final PodStatus status = pod.getStatus();
    final String phase = status.getPhase();

    boolean exited = false;
    boolean started = false;

    final Optional<ContainerStatus> containerStatus = getMainContainerStatus(pod);
    switch (phase) {
      case "Running":
        // Check if the main container has exited
        exited = containerStatus.map(ContainerStatus::getState)
                                .map(ContainerState::getTerminated)
                                .isPresent();
        if (exited) {
          break;
        }

        // check that the main container is ready
        // TODO: is checking for "ready" meaningful without a readiness probe configured?
        started = containerStatus.map(ContainerStatus::getReady)
                                 .orElse(false);
        break;

      case "Succeeded":
      case "Failed":
        exited = true;
        break;

      default:
        // do nothing
        break;
    }

    if (started) {
      switch (state.state()) {
        case PREPARE:
        case SUBMITTED:
          generatedEvents.add(Event.started(workflowInstance));
          break;

        default:
          // no event
          break;
      }
    }

    if (exited) {
      switch (state.state()) {
        case PREPARE:
        case SUBMITTED:
          generatedEvents.add(Event.started(workflowInstance));
          // intentional fall-through

        case RUNNING:
          final Optional<Integer> exitCode = containerStatus.flatMap(cs ->
              getExitCodeIfValid(workflowInstance.toKey(), pod, cs, stats));
          generatedEvents.add(Event.terminate(workflowInstance, exitCode));
          break;

        default:
          // no event
          break;
      }
    }

    return generatedEvents;
  }

  private static Optional<Event> isInErrorState(WorkflowInstance workflowInstance, Pod pod) {
    final PodStatus status = pod.getStatus();
    final String phase = status.getPhase();

    if ("NodeLost".equals(pod.getStatus().getReason())) {
      return Optional.of(Event.runError(workflowInstance, "Lost node running pod"));
    }

    switch (phase) {
      case "Pending":
        // check if one or more docker contains failed to pull their image, a possible silent error
        return getMainContainerStatus(pod)
            .filter(KubernetesPodEventTranslator::hasPullImageError)
            .map(x -> Event.runError(workflowInstance, "One or more containers failed to pull their image"));

      case "Succeeded":
      case "Failed":
        final Optional<ContainerStatus> containerStatusOpt = getMainContainerStatus(pod);
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

  static boolean isTerminated(ContainerStatus cs) {
    return cs.getState().getTerminated() != null;
  }

  static boolean isTerminated(Pod pod) {
    return getMainContainerStatus(pod)
        .map(KubernetesPodEventTranslator::isTerminated)
        .orElse(false);
  }
}
