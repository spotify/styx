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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.RunState;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
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
    if (Optional.ofNullable(pod.getMetadata().getAnnotations())
        .map(annotations -> "true".equals(annotations.get(DOCKER_TERMINATION_LOGGING_ANNOTATION)))
        .orElse(false)) {
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

  static List<Event> translate(
      WorkflowInstance workflowInstance,
      RunState state,
      Pod pod,
      Stats stats) {

    final Optional<ContainerStatus> mainContainerStatusOpt = getMainContainerStatus(pod);

    final Optional<Event> hasError = isInErrorState(workflowInstance, pod, mainContainerStatusOpt);
    if (hasError.isPresent()) {
      return handleError(state, hasError.get());
    }

    if (isExited(pod, mainContainerStatusOpt)) {
      return handleExited(workflowInstance, state, pod, mainContainerStatusOpt, stats);
    }

    if (isStarted(pod, mainContainerStatusOpt)) {
      return handleStarted(workflowInstance, state);
    }

    return List.of();
  }

  private static List<Event> handleExited(WorkflowInstance workflowInstance, RunState state,
                                                Pod pod,
                                                Optional<ContainerStatus> mainContainerStatusOpt,
                                                Stats stats) {
    final List<Event> generatedEvents = Lists.newArrayList();

    switch (state.state()) {
      case PREPARE:
      case SUBMITTED:
        generatedEvents.add(Event.started(workflowInstance));
        // intentional fall-through

      case RUNNING:
        final Optional<Integer> exitCode = mainContainerStatusOpt.flatMap(cs ->
            getExitCodeIfValid(workflowInstance.toKey(), pod, cs, stats));
        generatedEvents.add(Event.terminate(workflowInstance, exitCode));
        break;

      default:
        // no event
        break;
    }

    return ImmutableList.copyOf(generatedEvents);
  }

  private static List<Event> handleStarted(WorkflowInstance workflowInstance, RunState state) {
    switch (state.state()) {
      case PREPARE:
      case SUBMITTED:
        return List.of(Event.started(workflowInstance));

      default:
        return List.of();
    }
  }

  private static List<Event> handleError(RunState state, Event event) {
    switch (state.state()) {
      case PREPARE:
      case SUBMITTED:
      case RUNNING:
        return List.of(event);

      default:
        return List.of();
    }
  }

  private static boolean isExited(Pod pod, Optional<ContainerStatus> mainContainerStatusOpt) {
    switch (pod.getStatus().getPhase()) {
      case "Running":
        // Check if the main container has exited
        if (mainContainerStatusOpt.map(ContainerStatus::getState)
            .map(ContainerState::getTerminated)
            .isPresent()) {
          return true;
        }

        break;

      case "Succeeded":
      case "Failed":
        return true;

      default:
        // do nothing
        break;
    }

    return false;
  }

  private static boolean isStarted(Pod pod, Optional<ContainerStatus> mainContainerStatusOpt) {
    return "Running".equals(pod.getStatus().getPhase()) && mainContainerStatusOpt
        .map(ContainerStatus::getReady).orElse(false);
  }

  private static Optional<Event> isInErrorState(WorkflowInstance workflowInstance, Pod pod,
                                                Optional<ContainerStatus> mainContainerStatusOpt) {
    final PodStatus status = pod.getStatus();
    final String phase = status.getPhase();

    if ("NodeLost".equals(pod.getStatus().getReason())) {
      return Optional.of(Event.runError(workflowInstance, "Lost node running pod"));
    }

    switch (phase) {
      case "Pending":
        // check if one or more docker contains failed to pull their image, a possible silent error
        return mainContainerStatusOpt
            .flatMap(KubernetesPodEventTranslator::imageError)
            .map(msg -> Event.runError(workflowInstance, msg));

      case "Succeeded":
      case "Failed":
        if (!mainContainerStatusOpt.isPresent()) {
          return Optional.of(Event.runError(workflowInstance, "Could not find our container in pod"));
        }

        final ContainerStatus containerStatus = mainContainerStatusOpt.get();
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

  static Optional<String> imageError(ContainerStatus cs) {
    return Optional.ofNullable(cs.getState().getWaiting()).flatMap(waiting ->
        Optional.ofNullable(waiting.getReason()).flatMap(reason -> {
          var message = Optional.ofNullable(waiting.getMessage()).orElse("");
          switch (reason) {
            // https://github.com/kubernetes/kubernetes/blob/8327e433590f9e867b1e31a4dc32316685695729/pkg/kubelet/images/types.go#L26
            case "ImageInspectError":
            case "PullImageError":
            case "ErrImagePull":
            case "ErrImageNeverPull":
            case "ImagePullBackOff":
            case "RegistryUnavailable":
              // TODO: Provide more descriptive error messages here
              return Optional.of("One or more containers failed to pull their image: " + reason + ": " + message);
            case "InvalidImageName":
              return Optional.of("One or more container image names were invalid: " + reason + ": " + message);
            default:
              return Optional.empty();
          }
        }));
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
