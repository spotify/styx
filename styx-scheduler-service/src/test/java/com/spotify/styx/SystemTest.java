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
import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.state.handlers.TerminationHandler;
import com.spotify.styx.testdata.TestData;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Optional;
import org.junit.Test;

public class SystemTest extends StyxSchedulerServiceFixture {

  private static final Schedule SCHEDULE_HOURLY = Schedule.create(
      "styx.TestEndpoint", Partitioning.HOURS, of("busybox"), of(asList("--hour", "{}")),
      empty(), empty(), emptyList());
  private static final Schedule SCHEDULE_DAILY = Schedule.create(
      "styx.TestEndpoint", Partitioning.DAYS, of("busybox"), of(asList("--hour", "{}")),
      empty(), empty(), emptyList());
  private static final String TEST_EXECUTION_ID_1 = "execution_1";
  private static final String TEST_DOCKER_IMAGE = "busybox:1.1";
  private static final Workflow HOURLY_WORKFLOW = Workflow.create(
      "styx",
      TestData.WORKFLOW_URI,
      SCHEDULE_HOURLY);
  private static final ExecutionDescription TEST_EXECUTION_DESCRIPTION =
      ExecutionDescription.create(
          TEST_DOCKER_IMAGE, Arrays.asList("--date", "{}", "--bar"), false, empty(), empty());
  private static final Workflow DAILY_WORKFLOW = Workflow.create(
      "styx",
      TestData.WORKFLOW_URI,
      SCHEDULE_DAILY);
  private static final Trigger TRIGGER1 = Trigger.unknown("trig1");
  private static final Trigger TRIGGER2 = Trigger.unknown("trig2");
  private static final Backfill ONE_DAY_HOURLY_BACKFILL = Backfill.newBuilder()
      .id("backfill-1")
      .start(Instant.parse("2015-01-01T00:00:00Z"))
      .end(Instant.parse("2015-01-02T00:00:00Z"))
      .workflowId(WorkflowId.create("styx", "styx.TestEndpoint"))
      .concurrency(2)
      .nextTrigger(Instant.parse("2015-01-01T00:00:00Z"))
      .partitioning(Partitioning.HOURS)
      .build();

  @Test
  public void shouldCatchUpWithNaturalTriggers() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T13:00:00Z");

    styxStarts();
    tickTriggerManager();
    awaitWorkflowInstanceState(
        WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-03-14T12"),
        RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance = dockerRuns.get(0)._1;
    RunSpec runSpec = dockerRuns.get(0)._2;
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T12")));

    tickTriggerManager();
    awaitWorkflowInstanceState(
        WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-03-14T13"),
        RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(2);

    workflowInstance = dockerRuns.get(1)._1;
    runSpec = dockerRuns.get(1)._2;
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T13")));

    tickTriggerManager();
    awaitWorkflowInstanceState(
        WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-03-14T14"),
        RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(3);

    workflowInstance = dockerRuns.get(2)._1;
    runSpec = dockerRuns.get(2)._2;
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T14")));
  }

  @Test
  public void testTriggerBackfillsWithinResourceLimit() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, false);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");
    givenBackfill(ONE_DAY_HOURLY_BACKFILL);

    styxStarts();
    tickScheduler();
    awaitWorkflowInstanceState(
        WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2015-01-01T00"),
        RunState.State.QUEUED);
    awaitWorkflowInstanceState(
        WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2015-01-01T01"),
        RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(2);
  }

  @Test
  public void testTriggerBackfillsWillComplete() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, false);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");
    final Backfill singleHourBackfill = ONE_DAY_HOURLY_BACKFILL.builder()
        .end(ONE_DAY_HOURLY_BACKFILL.start().plus(1, ChronoUnit.HOURS)).build();
    givenBackfill(singleHourBackfill);

    styxStarts();
    tickScheduler();
    awaitWorkflowInstanceState(
        WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2015-01-01T00"),
        RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(1);
    WorkflowInstance workflowInstance = dockerRuns.get(0)._1;

    injectEvent(Event.started(workflowInstance));
    injectEvent(Event.terminate(workflowInstance, Optional.of(0)));
    awaitWorkflowInstanceCompletion(workflowInstance);
    awaitBackfillCompleted(singleHourBackfill.id());
    tickScheduler();
    assertThat(getState(workflowInstance), is(nullValue()));
  }

  @Test
  public void removedEnabledWorkflowWontGetScheduled() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T14:00:00Z");

    WorkflowInstance instance1 = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-03-14T13");
    WorkflowInstance instance2 = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-03-14T14");

    styxStarts();
    tickTriggerManager();
    awaitWorkflowInstanceState(instance1, RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(1);

    assertThat(dockerRuns.get(0)._1, is(instance1));

    workflowDeleted(HOURLY_WORKFLOW);
    tickTriggerManager();

    assertThat(getState(instance2), is(nullValue()));
  }

  @Test
  public void updatesNextNaturalTriggerWhenWFPartitioningChangesFromFinerToCoarser() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    WorkflowInstance workflowInstance =
        WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-03-14T14");

    styxStarts();
    timePasses(1, SECONDS);
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);
    timePasses(1, SECONDS);
    awaitNumberOfDockerRuns(1);

    workflowInstance = dockerRuns.get(0)._1;
    RunSpec runSpec = dockerRuns.get(0)._2;
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T14")));

    workflowInstance = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-03-14");
    // this should store a new value for nextNaturalTrigger, 2016-03-15.
    workflowChanges(DAILY_WORKFLOW);
    timeJumps(1, DAYS);
    timePasses(1, SECONDS);
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);
    timePasses(1, SECONDS);
    awaitNumberOfDockerRuns(2);

    workflowInstance = dockerRuns.get(1)._1;
    runSpec = dockerRuns.get(1)._2;
    assertThat(workflowInstance.workflowId(), is(DAILY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14")));
  }

  @Test
  public void updatesNextNaturalTriggerWhenWFPartitioningChangesFromCoarserToFiner() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(DAILY_WORKFLOW);
    givenWorkflowEnabledStateIs(DAILY_WORKFLOW, true);
    WorkflowInstance workflowInstance = WorkflowInstance.create(DAILY_WORKFLOW.id(), "2016-03-13");

    styxStarts();
    timePasses(1, SECONDS);
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);
    timePasses(1, SECONDS);
    awaitNumberOfDockerRuns(1);

    workflowInstance = dockerRuns.get(0)._1;
    RunSpec runSpec = dockerRuns.get(0)._2;
    assertThat(workflowInstance.workflowId(), is(DAILY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-13")));

    workflowInstance = WorkflowInstance.create(HOURLY_WORKFLOW.id(), "2016-03-14T15");
    // this should store a new value for nextNaturalTrigger, 2016-03-14T16.
    workflowChanges(HOURLY_WORKFLOW);
    timeJumps(1, HOURS);
    timePasses(1, SECONDS);
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);
    timePasses(1, SECONDS);

    awaitNumberOfDockerRuns(2);
    workflowInstance = dockerRuns.get(1)._1;
    runSpec = dockerRuns.get(1)._2;
    assertThat(workflowInstance.workflowId(), is(DAILY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T15")));
  }

  @Test
  public void doesntUpdateNextNaturalTriggerWhenPartitioningDoesntChange() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(DAILY_WORKFLOW);
    givenWorkflowEnabledStateIs(DAILY_WORKFLOW, true);
    WorkflowInstance workflowInstance =
        WorkflowInstance.create(DAILY_WORKFLOW.id(), "2016-03-13");

    styxStarts();
    timePasses(1, SECONDS);
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);
    timePasses(1, SECONDS);
    awaitNumberOfDockerRuns(1);

    workflowInstance = dockerRuns.get(0)._1;
    RunSpec runSpec = dockerRuns.get(0)._2;
    assertThat(workflowInstance.workflowId(), is(DAILY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-13")));

    injectEvent(Event.started(workflowInstance));
    injectEvent(Event.terminate(workflowInstance, Optional.of(0)));
    awaitWorkflowInstanceCompletion(workflowInstance);

    workflowChanges(Workflow.create(DAILY_WORKFLOW.componentId(),
                                    DAILY_WORKFLOW.componentUri(),
                                    Schedule.create(SCHEDULE_DAILY.id(), SCHEDULE_DAILY.partitioning(),
                                                    Optional.of("freebox"), SCHEDULE_DAILY.dockerArgs(), empty(),
                                                    SCHEDULE_DAILY.secret(), emptyList())));
    timePasses(StyxScheduler.SCHEDULER_TICK_INTERVAL_SECONDS, SECONDS);
    awaitNumberOfDockerRunsWontChange(1);

    assertThat(dockerRuns.size(), is(1));
  }

  @Test
  public void runsDockerImageWithArgsTemplate() throws Exception {
    givenTheTimeIs("2016-03-14T15:59:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");

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
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");

    styxStarts();
    timePasses(1, MINUTES);
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance = dockerRuns.get(0)._1;
    RunSpec runSpec = dockerRuns.get(0)._2;
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T15")));

    injectEvent(Event.started(workflowInstance));
    injectEvent(Event.terminate(workflowInstance, Optional.of(20)));
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);

    Schedule changedSchedule = Schedule.create(
        SCHEDULE_HOURLY.id(), Partitioning.HOURS, of("busybox:v777"),
        of(asList("other", "args")), empty(), empty(), emptyList());

    Workflow changedWorkflow = Workflow.create(
        HOURLY_WORKFLOW.componentId(),
        TestData.WORKFLOW_URI,
        changedSchedule);

    workflowChanges(changedWorkflow);

    // we must stagger the time progression here in order not to no trigger tons of retries
    timePasses(TerminationHandler.MISSING_DEPS_RETRY_DELAY_MINUTES - 1, MINUTES);
    timePasses(59, SECONDS);
    timePasses(StyxScheduler.SCHEDULER_TICK_INTERVAL_SECONDS, SECONDS);

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
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");

    styxStarts();
    timePasses(1, MINUTES);
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance = dockerRuns.get(0)._1;

    injectEvent(Event.started(workflowInstance));
    injectEvent(Event.terminate(workflowInstance, Optional.of(20)));
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);

    assertThat(dockerCleans, contains(TEST_EXECUTION_ID));
  }

  @Test
  public void cleansUpDockerRunsWhenFailing() throws Exception {
    givenTheTimeIs("2016-03-14T15:59:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");

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
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");

    givenStoredEventAtTime(Event.timeTrigger(workflowInstance),                0L, timeOffsetSeconds(1));
    givenStoredEventAtTime(Event.started(workflowInstance),                    1L, timeOffsetSeconds(2));
    givenStoredEventAtTime(Event.terminate(workflowInstance, Optional.of(20)), 2L, timeOffsetSeconds(3));
    givenStoredEventAtTime(Event.retryAfter(workflowInstance, 30000),          3L, timeOffsetSeconds(4));
    givenActiveStateAtSequenceCount(workflowInstance,                          3L);

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
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");

    givenStoredEvent(Event.timeTrigger(workflowInstance),               0L);
    givenStoredEvent(Event.started(workflowInstance),                   1L);
    givenStoredEvent(Event.terminate(workflowInstance, Optional.of(20)),2L);
    givenStoredEvent(Event.retry(workflowInstance),                     3L);
    givenStoredEvent(Event.started(workflowInstance),                   4L);
    givenStoredEvent(Event.terminate(workflowInstance, Optional.of(20)),5L);
    givenStoredEvent(Event.retryAfter(workflowInstance, 30000),         6L);
    givenActiveStateAtSequenceCount(workflowInstance,                   6L);

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
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");

    givenStoredEvent(Event.triggerExecution(workflowInstance, TRIGGER1),                       0L);
    givenStoredEvent(Event.created(workflowInstance, TEST_EXECUTION_ID_1, TEST_DOCKER_IMAGE),  1L);
    givenStoredEvent(Event.started(workflowInstance),                                          2L);
    givenStoredEvent(Event.terminate(workflowInstance, Optional.of(30)),                       3L);
    givenStoredEvent(Event.retry(workflowInstance),                                            4L);
    givenStoredEvent(Event.started(workflowInstance),                                          5L);
    givenStoredEvent(Event.terminate(workflowInstance, Optional.of(30)),                       6L);
    givenStoredEvent(Event.retryAfter(workflowInstance, 30000),                                7L);
    givenStoredEvent(Event.halt(workflowInstance),                                             8L);
    givenStoredEvent(Event.triggerExecution(workflowInstance, TRIGGER2),                       9L);
    givenStoredEvent(Event.created(workflowInstance, TEST_EXECUTION_ID_1, TEST_DOCKER_IMAGE), 10L);
    givenStoredEvent(Event.started(workflowInstance),                                         11L);
    givenStoredEvent(Event.terminate(workflowInstance, Optional.of(30)),                      12L);
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

  @Test
  public void restoresActiveStatesFromBigtableRetriggered2() throws Exception {
    WorkflowInstance workflowInstance = create(HOURLY_WORKFLOW.id(), "2016-03-14T14");

    givenTheTimeIs("2016-03-14T15:17:45Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");

    givenStoredEvent(Event.triggerExecution(workflowInstance, TRIGGER1),          0L);
    givenStoredEvent(Event.dequeue(workflowInstance),                             1L);
    givenStoredEvent(Event.submit(workflowInstance, TEST_EXECUTION_DESCRIPTION),  2L);
    givenStoredEvent(Event.submitted(workflowInstance, "exec1"),                  3L);
    givenStoredEvent(Event.started(workflowInstance),                             4L);
    givenStoredEvent(Event.terminate(workflowInstance, Optional.of(30)),          5L);
    givenStoredEvent(Event.retryAfter(workflowInstance, 30000),                   6L);
    givenStoredEvent(Event.dequeue(workflowInstance),                             7L);
    givenStoredEvent(Event.submit(workflowInstance, TEST_EXECUTION_DESCRIPTION),  8L);
    givenStoredEvent(Event.submitted(workflowInstance, "exec2"),                  9L);
    givenStoredEvent(Event.started(workflowInstance),                            10L);
    givenStoredEvent(Event.terminate(workflowInstance, Optional.of(30)),         11L);
    givenStoredEvent(Event.retryAfter(workflowInstance, 30000),                  12L);
    givenStoredEvent(Event.halt(workflowInstance),                               13L);
    givenStoredEvent(Event.triggerExecution(workflowInstance, TRIGGER2),         14L);
    givenStoredEvent(Event.dequeue(workflowInstance),                            15L);
    givenStoredEvent(Event.submit(workflowInstance, TEST_EXECUTION_DESCRIPTION), 16L);
    givenStoredEvent(Event.submitted(workflowInstance, "exec3"),                 17L);
    givenStoredEvent(Event.started(workflowInstance),                            18L);
    givenStoredEvent(Event.terminate(workflowInstance, Optional.of(30)),         19L);
    givenStoredEvent(Event.retryAfter(workflowInstance, 30000),                  20L);
    givenActiveStateAtSequenceCount(workflowInstance,                            20L);

    styxStarts();

    timePasses(1, MINUTES);
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance2 = dockerRuns.get(0)._1;
    RunSpec runSpec = dockerRuns.get(0)._2;
    assertThat(workflowInstance2.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T14")));
  }
}
