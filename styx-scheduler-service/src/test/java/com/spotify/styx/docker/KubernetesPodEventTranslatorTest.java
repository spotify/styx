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

import static com.spotify.styx.docker.KubernetesDockerRunner.KEEPALIVE_CONTAINER_NAME;
import static com.spotify.styx.docker.KubernetesDockerRunner.MAIN_CONTAINER_NAME;
import static com.spotify.styx.docker.KubernetesPodEventTranslator.translate;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import com.spotify.styx.docker.KubernetesDockerRunner.KubernetesSecretSpec;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.testdata.TestData;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateRunning;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

public class KubernetesPodEventTranslatorTest {

  private static final WorkflowInstance WFI =
      WorkflowInstance.create(TestData.WORKFLOW_ID, "foo");
  private static final DockerRunner.RunSpec RUN_SPEC =
      DockerRunner.RunSpec.simple("eid", "busybox");
  private static final String MESSAGE_FORMAT = "{\"rfu\":{\"dum\":\"my\"},\"component_id\":\"dummy\",\"workflow_id\":\"dummy\",\"parameter\":\"dummy\",\"execution_id\":\"dummy\",\"event\":\"dummy\",\"exit_code\":%d}\n";
  private static final KubernetesSecretSpec SECRET_SPEC = KubernetesSecretSpec.builder().build();

  private static final String STYX_ENVIRONMENT = "testing";

  private Pod pod = KubernetesDockerRunner.createPod(WFI, RUN_SPEC, SECRET_SPEC, STYX_ENVIRONMENT);

  @Test
  public void terminateOnSuccessfulTermination() {
    setTerminated(pod, "Succeeded", 20, null);

    assertGeneratesEventsAndTransitions(
        RunState.State.RUNNING, pod,
        Event.terminate(WFI, Optional.of(20)));
  }

  @Test
  public void startedAndTerminatedOnFromSubmitted() {
    setTerminated(pod, "Succeeded", 0, null);

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(0)));
  }

  @Test
  public void shouldNotGenerateStartedWhenContainerIsNotReady() {
    setRunning(pod, /* ready= */ false);

    assertGeneratesNoEvents(
        RunState.State.SUBMITTED, pod);
  }

  @Test
  public void shouldGenerateStartedWhenContainerIsReady() {
    setRunning(pod, /* ready= */ true);

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI));
  }

  @Test
  public void runErrorOnErrImagePull() {
    setWaiting(pod, "Pending", "ErrImagePull");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.runError(WFI, "One or more containers failed to pull their image"));
  }

  @Test
  public void runErrorOnUnknownPhaseEntered() {
    pod.setStatus(podStatusNoContainer("Unknown"));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.runError(WFI, "Pod entered Unknown phase"));
  }

  @Test
  public void runErrorOnMissingContainer() {
    pod.setStatus(podStatusNoContainer("Succeeded"));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.runError(WFI, "Could not find our container in pod"));
  }

  @Test
  public void runErrorOnUnexpectedTerminatedStatus() {
    setWaiting(pod, "Failed", "");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.runError(WFI, "Unexpected null terminated status"));
  }
  @Test
  public void runErrorOnNodeLost() {
    setRunning(pod, true);
    pod.getStatus().setReason("NodeLost");

    assertGeneratesEventsAndTransitions(
        State.RUNNING, pod,
        Event.runError(WFI, "Lost node running pod"));
  }

  @Test
  public void errorExitCodeOnTerminationLoggingButNoMessage() {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Succeeded", 0, null);

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void errorExitCodeOnTerminationLoggingButInvalidJson() {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Succeeded", 0, "SUCCESS");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void errorExitCodeOnTerminationLoggingButPartialJson() {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Succeeded", 0, "{\"workflow_id\":\"dummy\"}");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void errorExitCodeOnTerminationLoggingButK8sFallback() {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Failed", 17, "{\"workflow_id\":\"dummy\"}");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(17)));
  }

  @Test
  public void errorContainerExitCodeAndUnparsableTerminationLog() {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Failed", 17, "{\"workf");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(17)));
  }

  @Test
  public void zeroContainerExitCodeAndInvalidTerminationLog() {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Failed", 0, "{\"workflow_id\":\"dummy\"}");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void zeroContainerExitCodeAndUnparsableTerminationLog() {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Failed", 0, "{\"workflo");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void exitCodeFromMessageOnTerminationLoggingAndZeroExitCode() {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Succeeded", 0, String.format(MESSAGE_FORMAT, 1));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(1)));
  }

  @Test
  public void noExitCodeFromEitherMessageOnTerminationLoggingNorDocker() {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Succeeded", null, null);

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void exitCodeFromMessageOnTerminationLoggingAndNonzeroExitCode() {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Failed", 2, String.format(MESSAGE_FORMAT, 3));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(3)));
  }

  @Test
  public void zeroExitCodeFromTerminationLogAndNonZeroContainerExitCode() {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Failed", 2, String.format(MESSAGE_FORMAT, 0));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(2)));
  }

  @Test
  public void zeroExitCodeFailedPhaseWithoutTerminationLog() {
    pod.setStatus(terminated("Failed", 0, null));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }
  
  @Test
  public void nonZeroExitCodeFailedPhaseWithoutTerminationLog() {
    pod.setStatus(terminated("Failed", 2, null));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(2)));
  }

  @Test
  public void noEventsWhenStateInTerminated() {
    pod.setStatus(podStatusNoContainer("Unknown"));

    assertGeneratesNoEvents(RunState.State.TERMINATED, pod);
  }

  @Test
  public void noEventsWhenStateInFailed() {
    pod.setStatus(podStatusNoContainer("Unknown"));

    assertGeneratesNoEvents(RunState.State.FAILED, pod);
  }

  private void assertGeneratesEventsAndTransitions(
      RunState.State initialState,
      Pod pod,
      Event... expectedEvents) {

    RunState state = RunState.create(WFI, initialState);
    List<Event> events = translate(WFI, state, pod, Stats.NOOP);
    assertThat(events, contains(expectedEvents));

    // ensure no exceptions are thrown when transitioning
    for (Event event : events) {
      state = state.transition(event, Instant::now);
    }
  }

  private void assertGeneratesNoEvents(
      RunState.State initialState,
      Pod pod) {

    RunState state = RunState.create(WFI, initialState);
    List<Event> events = translate(WFI, state, pod, Stats.NOOP);

    assertThat(events, empty());
  }

  static void setRunning(Pod pod, boolean ready) {
    pod.setStatus(running(ready));
  }

  static PodStatus running(boolean ready) {
    return podStatus("Running", ready, runningContainerState());
  }

  private static ContainerState runningContainerState() {
    return new ContainerState(new ContainerStateRunning("2016-05-30T09:46:48Z"), null, null);
  }

  static void setTerminated(Pod pod, String phase, Integer exitCode, String message) {
    pod.setStatus(terminated(phase, exitCode, message));
  }

  static PodStatus terminated(String phase, Integer exitCode, String message) {
    return podStatus(phase, true, terminatedContainerState(exitCode, message));
  }

  static ContainerState terminatedContainerState(Integer exitCode, String message) {
    return new ContainerState(null, new ContainerStateTerminated("", exitCode, "", message, "", 0, ""), null);
  }

  static void setWaiting(Pod pod, String phase, String reason) {
    pod.setStatus(waiting(phase, reason));
  }

  static PodStatus waiting(String phase, String reason) {
    return podStatus(phase, true, waitingContainerState(reason));
  }

  private static ContainerState waitingContainerState(String reason) {
    return new ContainerState(null, null, new ContainerStateWaiting("", reason));
  }

  static PodStatus podStatus(String phase, boolean ready, ContainerState containerState) {
    PodStatus podStatus = podStatusNoContainer(phase);
    podStatus.getContainerStatuses()
        .add(new ContainerStatus("foo", "", "", containerState,
                                 MAIN_CONTAINER_NAME, ready, 0, containerState));
    podStatus.getContainerStatuses()
        .add(new ContainerStatusBuilder().withName(KEEPALIVE_CONTAINER_NAME)
            .withNewState().withNewRunning().endRunning().endState()
            .build());
    return podStatus;
  }

  static PodStatus podStatusNoContainer(String phase) {
    return new PodStatus(emptyList(), Lists.newArrayList(), "", "", phase, "", "", "");
  }

  private Pod podWithTerminationLogging() {
    return KubernetesDockerRunner.createPod(
        WFI,
        DockerRunner.RunSpec.builder()
            .executionId("eid")
            .imageName("busybox")
            .terminationLogging(true).build(),
        SECRET_SPEC,
        STYX_ENVIRONMENT);
  }
}
