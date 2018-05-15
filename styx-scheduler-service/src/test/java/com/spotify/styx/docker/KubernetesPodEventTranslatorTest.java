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
import com.spotify.styx.testdata.TestData;
import io.fabric8.kubernetes.api.model.ContainerState;
import io.fabric8.kubernetes.api.model.ContainerStateRunning;
import io.fabric8.kubernetes.api.model.ContainerStateTerminated;
import io.fabric8.kubernetes.api.model.ContainerStateWaiting;
import io.fabric8.kubernetes.api.model.ContainerStatus;
import io.fabric8.kubernetes.api.model.ContainerStatusBuilder;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.Watcher;
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

  Pod pod = KubernetesDockerRunner.createPod(WFI, RUN_SPEC, SECRET_SPEC);

  @Test
  public void terminateOnSuccessfulTermination() throws Exception {
    setTerminated(pod, "Succeeded", 20, null);

    assertGeneratesEventsAndTransitions(
        RunState.State.RUNNING, pod,
        Event.terminate(WFI, Optional.of(20)));
  }

  @Test
  public void startedAndTerminatedOnFromSubmitted() throws Exception {
    setTerminated(pod, "Succeeded", 0, null);

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(0)));
  }

  @Test
  public void shouldNotGenerateStartedWhenContainerIsNotReady() throws Exception {
    setRunning(pod, /* ready= */ false);

    assertGeneratesNoEvents(
        RunState.State.SUBMITTED, pod);
  }

  @Test
  public void shouldGenerateStartedWhenContainerIsReady() throws Exception {
    setRunning(pod, /* ready= */ true);

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI));
  }

  @Test
  public void runErrorOnErrImagePull() throws Exception {
    setWaiting(pod, "Pending", "ErrImagePull");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.runError(WFI, "One or more containers failed to pull their image"));
  }

  @Test
  public void runErrorOnUnknownPhaseEntered() throws Exception {
    pod.setStatus(podStatusNoContainer("Unknown"));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.runError(WFI, "Pod entered Unknown phase"));
  }

  @Test
  public void runErrorOnMissingContainer() throws Exception {
    pod.setStatus(podStatusNoContainer("Succeeded"));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.runError(WFI, "Could not find our container in pod"));
  }

  @Test
  public void runErrorOnUnexpectedTerminatedStatus() throws Exception {
    setWaiting(pod, "Failed", "");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.runError(WFI, "Unexpected null terminated status"));
  }

  @Test
  public void errorExitCodeOnTerminationLoggingButNoMessage() throws Exception {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Succeeded", 0, null);

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void errorExitCodeOnTerminationLoggingButInvalidJson() throws Exception {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Succeeded", 0, "SUCCESS");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void errorExitCodeOnTerminationLoggingButPartialJson() throws Exception {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Succeeded", 0, "{\"workflow_id\":\"dummy\"}");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void errorExitCodeOnTerminationLoggingButK8sFallback() throws Exception {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Failed", 17, "{\"workflow_id\":\"dummy\"}");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(17)));
  }

  @Test
  public void errorContainerExitCodeAndUnparsableTerminationLog() throws Exception {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Failed", 17, "{\"workf");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(17)));
  }

  @Test
  public void zeroContainerExitCodeAndInvalidTerminationLog() throws Exception {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Failed", 0, "{\"workflow_id\":\"dummy\"}");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void zeroContainerExitCodeAndUnparsableTerminationLog() throws Exception {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Failed", 0, "{\"workflo");

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void exitCodeFromMessageOnTerminationLoggingAndZeroExitCode() throws Exception {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Succeeded", 0, String.format(MESSAGE_FORMAT, 1));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(1)));
  }

  @Test
  public void noExitCodeFromEitherMessageOnTerminationLoggingNorDocker() throws Exception {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Succeeded", null, null);

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.empty()));
  }

  @Test
  public void exitCodeFromMessageOnTerminationLoggingAndNonzeroExitCode() throws Exception {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Failed", 2, String.format(MESSAGE_FORMAT, 3));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(3)));
  }

  @Test
  public void zeroExitCodeFromTerminationLogAndNonZeroContainerExitCode() throws Exception {
    Pod pod = podWithTerminationLogging();
    setTerminated(pod, "Failed", 2, String.format(MESSAGE_FORMAT, 0));

    assertGeneratesEventsAndTransitions(
        RunState.State.SUBMITTED, pod,
        Event.started(WFI),
        Event.terminate(WFI, Optional.of(2)));
  }

  @Test
  public void noEventsWhenStateInTerminated() throws Exception {
    pod.setStatus(podStatusNoContainer("Unknown"));

    assertGeneratesNoEvents(RunState.State.TERMINATED, pod);
  }

  @Test
  public void noEventsWhenStateInFailed() throws Exception {
    pod.setStatus(podStatusNoContainer("Unknown"));

    assertGeneratesNoEvents(RunState.State.FAILED, pod);
  }

  @Test
  public void shouldIgnoreDeletedEvents() throws Exception {
    setTerminated(pod, "Succeeded", 0, null);
    RunState state = RunState.create(WFI, RunState.State.TERMINATED);

    List<Event> events = translate(WFI, state, Watcher.Action.DELETED, pod, Stats.NOOP);
    assertThat(events, empty());
  }

  private void assertGeneratesEventsAndTransitions(
      RunState.State initialState,
      Pod pod,
      Event... expectedEvents) {

    RunState state = RunState.create(WFI, initialState);
    List<Event> events = translate(WFI, state, Watcher.Action.MODIFIED, pod, Stats.NOOP);
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
    List<Event> events = translate(WFI, state, Watcher.Action.MODIFIED, pod, Stats.NOOP);

    assertThat(events, empty());
  }

  static void setRunning(Pod pod, boolean ready) {
    pod.setStatus(running(ready, pod.getMetadata().getName()));
  }

  static PodStatus running(boolean ready, String containerName) {
    return podStatus("Running", ready, containerName, runningContainerState());
  }

  private static ContainerState runningContainerState() {
    return new ContainerState(new ContainerStateRunning("2016-05-30T09:46:48Z"), null, null);
  }

  static void setTerminated(Pod pod, String phase, Integer exitCode, String message) {
    pod.setStatus(terminated(phase, exitCode, message, pod.getMetadata().getName()));
  }

  static PodStatus terminated(String phase, Integer exitCode, String message, String containerName) {
    return podStatus(phase, true, containerName, terminatedContainerState(exitCode, message));
  }

  static ContainerState terminatedContainerState(Integer exitCode, String message) {
    return new ContainerState(null, new ContainerStateTerminated("", exitCode, "", message, "", 0, ""), null);
  }

  static void setWaiting(Pod pod, String phase, String reason) {
    pod.setStatus(waiting(phase, reason, pod.getMetadata().getName()));
  }

  static PodStatus waiting(String phase, String reason, String containerName) {
    return podStatus(phase, true, containerName, waitingContainerState(reason));
  }

  private static ContainerState waitingContainerState(String reason) {
    return new ContainerState(null, null, new ContainerStateWaiting("", reason));
  }

  static PodStatus podStatus(String phase, boolean ready, String containerName, ContainerState containerState) {
    PodStatus podStatus = podStatusNoContainer(phase);
    podStatus.getContainerStatuses()
        .add(new ContainerStatus("foo", "", "", containerState,
                                 containerName, ready, 0, containerState));
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
        SECRET_SPEC);
  }
}
