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

package com.spotify.styx;

import static com.spotify.styx.model.WorkflowInstance.create;
import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.model.DataEndpoint;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.handlers.TerminationHandler;
import com.spotify.styx.testdata.TestData;
import java.time.Instant;
import org.junit.Test;

public class SystemTest extends StyxSchedulerServiceFixture {

  private static final DataEndpoint DATA_ENDPOINT = DataEndpoint.create(
      "styx.TestEndpoint", Partitioning.HOURS, of("busybox"), of(asList("--hour", "{}")),
      empty());
  private static final String TEST_EXECUTION_ID_1 = "execution_1";
  private static final String TEST_DOCKER_IMAGE = "busybox:1.1";
  private static final Workflow HOURLY_WORKFLOW = Workflow.create(
      "styx",
      TestData.WORKFLOW_URI,
      DATA_ENDPOINT);
  private static final Instant NEXT_EXECUTION = Instant.parse("2016-03-14T16:00:00Z");

  @Test
  public void shouldCatchUpWithNaturalTriggers() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    final Instant nextExecution = Instant.parse("2016-03-14T13:00:00Z");
    givenNextNaturalTrigger(HOURLY_WORKFLOW.id(), nextExecution);

    styxStarts();
    timePasses(1, SECONDS);
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance = dockerRuns.get(0)._1;
    RunSpec runSpec = dockerRuns.get(0)._2;
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T12")));

    timePasses(1, SECONDS);
    awaitNumberOfDockerRuns(2);

    workflowInstance = dockerRuns.get(1)._1;
    runSpec = dockerRuns.get(1)._2;
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T13")));

    timePasses(1, SECONDS);
    awaitNumberOfDockerRuns(3);

    workflowInstance = dockerRuns.get(2)._1;
    runSpec = dockerRuns.get(2)._2;
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T14")));
  }

  @Test
  public void removedEnabledWorkflowWontGetScheduled() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);

    final Instant nextExecution = Instant.parse("2016-03-14T14:00:00Z");
    givenNextNaturalTrigger(HOURLY_WORKFLOW.id(), nextExecution);

    WorkflowInstance instance1 = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-03-14T13");
    WorkflowInstance instance2 = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-03-14T14");

    styxStarts();
    timePasses(1, SECONDS);
    awaitNumberOfDockerRuns(1);

    assertThat(dockerRuns.get(0)._1, is(instance1));

    workflowDeleted(HOURLY_WORKFLOW);

    timePasses(1, SECONDS);

    assertThat(getState(instance2), is(nullValue()));
  }

  @Test
  public void runsDockerImageWithArgsTemplate() throws Exception {
    givenTheTimeIs("2016-03-14T15:59:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW.id(), NEXT_EXECUTION);

    styxStarts();
    timePasses(1, MINUTES);
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance = dockerRuns.get(0)._1;
    RunSpec runSpec = dockerRuns.get(0)._2;
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T15")));
  }

  @Test
  public void retriesUseLatestWorkflowSpecification() throws Exception {
    givenTheTimeIs("2016-03-14T15:59:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW.id(), NEXT_EXECUTION);

    styxStarts();
    timePasses(1, MINUTES);
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance = dockerRuns.get(0)._1;
    RunSpec runSpec = dockerRuns.get(0)._2;
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T15")));

    injectEvent(Event.started(workflowInstance));
    injectEvent(Event.terminate(workflowInstance, 20));
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);

    DataEndpoint changedDataEndpoint = DataEndpoint.create(
        DATA_ENDPOINT.id(), Partitioning.HOURS, of("busybox:v777"), of(asList("other", "args")),
        empty());

    Workflow changedWorkflow = Workflow.create(
        HOURLY_WORKFLOW.componentId(),
        TestData.WORKFLOW_URI,
        changedDataEndpoint);

    workflowChanges(changedWorkflow);

    // we must stagger the time progression here in order not to no trigger tons of retries
    timePasses(TerminationHandler.MISSING_DEPS_RETRY_DELAY_MINUTES - 1, MINUTES);
    timePasses(59, SECONDS);
    timePasses(StyxScheduler.STATE_RETRY_CHECK_INTERVAL_SECONDS, SECONDS);

    awaitNumberOfDockerRuns(2);

    WorkflowInstance workflowInstance2 = dockerRuns.get(1)._1;
    RunSpec runSpec2 = dockerRuns.get(1)._2;
    assertThat(workflowInstance2.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec2, is(RunSpec.simple("busybox:v777", "other", "args")));
  }

  @Test
  public void cleansUpDockerRunsWhenTerminating() throws Exception {
    givenTheTimeIs("2016-03-14T15:59:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW.id(), NEXT_EXECUTION);

    styxStarts();
    timePasses(1, MINUTES);
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance = dockerRuns.get(0)._1;

    injectEvent(Event.started(workflowInstance));
    injectEvent(Event.terminate(workflowInstance, 20));
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);

    assertThat(dockerCleans, contains(TEST_EXECUTION_ID));
  }

  @Test
  public void cleansUpDockerRunsWhenFailing() throws Exception {
    givenTheTimeIs("2016-03-14T15:59:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW.id(), NEXT_EXECUTION);

    styxStarts();
    timePasses(1, MINUTES);
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance = dockerRuns.get(0)._1;

    injectEvent(Event.runError(workflowInstance, "Something failed"));
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);

    assertThat(dockerCleans, contains(TEST_EXECUTION_ID));
  }

  @Test
  public void restoredStatesUseOriginalTimestamps() throws Exception {
    WorkflowInstance workflowInstance = create(HOURLY_WORKFLOW.id(), "2016-03-14T10");

    givenTheTimeIs("2016-03-14T15:17:45Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW.id(), NEXT_EXECUTION);

    givenStoredEventAtTime(Event.timeTrigger(workflowInstance),       0L, timeOffsetSeconds(1));
    givenStoredEventAtTime(Event.started(workflowInstance),           1L, timeOffsetSeconds(2));
    givenStoredEventAtTime(Event.terminate(workflowInstance, 20),     2L, timeOffsetSeconds(3));
    givenStoredEventAtTime(Event.retryAfter(workflowInstance, 30000), 3L, timeOffsetSeconds(4));
    givenActiveStateAtSequenceCount(workflowInstance,                 3L);

    givenTheTimeIs("2016-03-14T16:01:00Z");
    styxStarts();

    RunState state = getState(workflowInstance);
    Instant stateTime = Instant.ofEpochMilli(state.timestamp());

    assertThat(stateTime, is(Instant.parse("2016-03-14T15:17:49Z")));
  }

  @Test
  public void restoresActiveStatesFromBigtable() throws Exception {
    WorkflowInstance workflowInstance = create(HOURLY_WORKFLOW.id(), "2016-03-14T14");

    givenTheTimeIs("2016-03-14T15:17:45Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);

    givenStoredEvent(Event.timeTrigger(workflowInstance),       0L);
    givenStoredEvent(Event.started(workflowInstance),           1L);
    givenStoredEvent(Event.terminate(workflowInstance, 20),     2L);
    givenStoredEvent(Event.retry(workflowInstance),             3L);
    givenStoredEvent(Event.started(workflowInstance),           4L);
    givenStoredEvent(Event.terminate(workflowInstance, 20),     5L);
    givenStoredEvent(Event.retryAfter(workflowInstance, 30000), 6L);
    givenActiveStateAtSequenceCount(workflowInstance,           6L);

    styxStarts();

    timePasses(1, MINUTES);
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance2 = dockerRuns.get(0)._1;
    RunSpec runSpec = dockerRuns.get(0)._2;
    assertThat(workflowInstance2.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T14")));
  }

  @Test
  public void restoresActiveStatesFromBigtableRetriggered() throws Exception {
    WorkflowInstance workflowInstance = create(HOURLY_WORKFLOW.id(), "2016-03-14T14");

    givenTheTimeIs("2016-03-14T15:17:45Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);

    givenStoredEvent(Event.triggerExecution(workflowInstance, "trig1"),                        0L);
    givenStoredEvent(Event.created(workflowInstance, TEST_EXECUTION_ID_1, TEST_DOCKER_IMAGE),  1L);
    givenStoredEvent(Event.started(workflowInstance),                                          2L);
    givenStoredEvent(Event.terminate(workflowInstance, 30),                                    3L);
    givenStoredEvent(Event.retry(workflowInstance),                                            4L);
    givenStoredEvent(Event.started(workflowInstance),                                          5L);
    givenStoredEvent(Event.terminate(workflowInstance, 30),                                    6L);
    givenStoredEvent(Event.retryAfter(workflowInstance, 30000),                                7L);
    givenStoredEvent(Event.halt(workflowInstance),                                             8L);
    givenStoredEvent(Event.triggerExecution(workflowInstance, "trig2"),                        9L);
    givenStoredEvent(Event.created(workflowInstance, TEST_EXECUTION_ID_1, TEST_DOCKER_IMAGE), 10L);
    givenStoredEvent(Event.started(workflowInstance),                                         11L);
    givenStoredEvent(Event.terminate(workflowInstance, 30),                                   12L);
    givenStoredEvent(Event.retryAfter(workflowInstance, 30000),                               13L);
    givenActiveStateAtSequenceCount(workflowInstance,                                         13L);

    styxStarts();

    timePasses(1, MINUTES);
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance2 = dockerRuns.get(0)._1;
    RunSpec runSpec = dockerRuns.get(0)._2;
    assertThat(workflowInstance2.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T14")));
  }
}
