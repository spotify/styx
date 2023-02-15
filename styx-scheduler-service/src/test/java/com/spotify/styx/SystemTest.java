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
import static com.spotify.styx.testdata.TestData.FLYTE_EXEC_CONF;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;;

import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.state.handlers.TerminationHandler;
import com.spotify.styx.util.TriggerInstantSpec;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import junitparams.JUnitParamsRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class SystemTest extends StyxSchedulerServiceFixture {

  private static final WorkflowConfiguration WORKFLOW_CONFIGURATION_HOURLY =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .schedule(Schedule.HOURS)
          .dockerImage("busybox")
          .dockerArgs(asList("--hour", "{}"))
          .env("FOO", "foo", "BAR", "bar")
          .build();
  private static final WorkflowConfiguration WORKFLOW_CONFIGURATION_HOURLY_WITH_ZERO_OFFSET =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .schedule(Schedule.HOURS)
          .offset("PT0S")
          .dockerImage("busybox")
          .dockerArgs(asList("--hour", "{}"))
          .build();
  private static final WorkflowConfiguration WORKFLOW_CONFIGURATION_HOURLY_WITH_TWO_HOURS_OFFSET =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .schedule(Schedule.HOURS)
          .offset("PT2H")
          .dockerImage("busybox")
          .dockerArgs(asList("--hour", "{}"))
          .build();
  private static final WorkflowConfiguration WORKFLOW_CONFIGURATION_DAILY =
      WorkflowConfiguration.builder()
          .id("styx.TestEndpoint")
          .schedule(Schedule.DAYS)
          .dockerImage("busybox")
          .dockerArgs(asList("--hour", "{}"))
          .build();
  private static final Workflow HOURLY_WORKFLOW = Workflow.create(
      "styx",
      WORKFLOW_CONFIGURATION_HOURLY);
  private static final Workflow HOURLY_WORKFLOW_WITH_ZERO_OFFSET = Workflow.create(
      "styx",
      WORKFLOW_CONFIGURATION_HOURLY_WITH_ZERO_OFFSET);
  private static final Workflow HOURLY_WORKFLOW_WITH_TWO_HOURS_OFFSET = Workflow.create(
      "styx",
      WORKFLOW_CONFIGURATION_HOURLY_WITH_TWO_HOURS_OFFSET);
  private static final Workflow DAILY_WORKFLOW = Workflow.create(
      "styx",
      WORKFLOW_CONFIGURATION_DAILY);
  private static final Trigger TRIGGER1 = Trigger.unknown("trig1");
  private static final Backfill ONE_DAY_HOURLY_BACKFILL = Backfill.newBuilder()
      .id("backfill-1")
      .start(Instant.parse("2015-01-01T00:00:00Z"))
      .end(Instant.parse("2015-01-02T00:00:00Z"))
      .workflowId(WorkflowId.create("styx", "styx.TestEndpoint"))
      .concurrency(2)
      .nextTrigger(Instant.parse("2015-01-01T00:00:00Z"))
      .schedule(Schedule.HOURS)
      .build();

  private static RunSpec naturalRunSpec(String executionId, String imageName, List<String> args,
      Map<String, String> env) {
    return RunSpec.builder()
        .executionId(executionId)
        .imageName(imageName)
        .args(args)
        .trigger(Trigger.natural())
        .env(env)
        .build();
  }

  private static RunSpec unknownRunSpec(String executionId, String imageName, List<String> args, String triggerId,
      Map<String, String> env) {
    return RunSpec.builder()
        .executionId(executionId)
        .imageName(imageName)
        .args(args)
        .trigger(Trigger.unknown(triggerId))
        .env(env)
        .build();
  }

  @RunWith(JUnitParamsRunner.class)
  public static class ShouldRunCustomWorkflowScheduleTest extends SystemTest {
  @Test
  public void shouldRunCustomWorkflowSchedule() throws Exception {
    Workflow customWorkflow = Workflow.create(
        "styx",
        WorkflowConfiguration.builder()
            .id("styx.TestEndpoint")
            .schedule(Schedule.parse("15,45 12,15 * * *"))
            .dockerImage("busybox")
            .dockerArgs(List.of())
            .build());

    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenWorkflow(customWorkflow);
    givenWorkflowEnabledStateIs(customWorkflow, true);
    givenNextNaturalTrigger(customWorkflow, "2016-03-14T12:45:00Z");

    styxStarts();
    tickTriggerManager();
    awaitWorkflowInstanceState(
        create(customWorkflow.id(), "2016-03-14T12:45:00Z"),
        RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance = getDockerRuns().get(0)._1.workflowInstance();
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(workflowInstance.parameter(), is("2016-03-14T12:45:00Z"));

    tickTriggerManager();
    awaitWorkflowInstanceState(
        create(customWorkflow.id(), "2016-03-14T15:15:00Z"),
        RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(2);

    workflowInstance = getDockerRuns().get(1)._1.workflowInstance();
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(workflowInstance.parameter(), is("2016-03-14T15:15:00Z"));
  }
  }

  public static class ShouldRunFlyteWorkflowToCompletion extends SystemTest {
    @Test
    public void shouldRunFlyteWorkflowToCompletion() throws Exception {
      Workflow wf = Workflow.create(
          "comp",
          WorkflowConfiguration.builder()
              .id("flyte-wf")
              .schedule(Schedule.parse("45 12 * * *"))
              .flyteExecConf(FLYTE_EXEC_CONF)
              .build());

      givenTheTimeIs("2016-03-14T15:30:00Z");
      givenWorkflow(wf);
      givenWorkflowEnabledStateIs(wf, true);
      givenNextNaturalTrigger(wf, "2016-03-14T12:45:00Z");

      WorkflowInstance wfi =
          create(wf.id(), "2016-03-14T12:45:00Z");
      styxStarts();
      tickTriggerManager();

      final SequenceEvent expectedEvent =
          SequenceEvent.create(
              Event.triggerExecution(wfi, Trigger.natural(), TriggerParameters.zero()),
              0,
              Instant.parse("2016-03-14T15:30:00Z").toEpochMilli());

      awaitUntilConsumedEvent(expectedEvent, RunState.State.QUEUED);
      tickScheduler();
      awaitWorkflowInstanceCompletion(wfi);
      assertThat(flyteExecCreations.size(), is(1));
      assertThat(getState(wfi), is(Optional.empty()));
    }
  }

  @RunWith(JUnitParamsRunner.class)
  public static class ShouldConsumeEventTest extends SystemTest {
  @Test
  public void shouldConsumeEvent() throws Exception {
    Workflow customWorkflow = Workflow.create(
        "styx",
        WorkflowConfiguration.builder()
            .id("styx.TestEndpoint")
            .schedule(Schedule.parse("15,45 12,15 * * *"))
            .dockerImage("busybox")
            .dockerArgs(List.of())
            .build());

    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenWorkflow(customWorkflow);
    givenWorkflowEnabledStateIs(customWorkflow, true);
    givenNextNaturalTrigger(customWorkflow, "2016-03-14T12:45:00Z");

    WorkflowInstance wfi =
        create(customWorkflow.id(), "2016-03-14T12:45:00Z");
    styxStarts();
    tickTriggerManager();

    final SequenceEvent expectedEvent =
        SequenceEvent.create(
            Event.triggerExecution(wfi, Trigger.natural(), TriggerParameters.zero()),
            0,
            Instant.parse("2016-03-14T15:30:00Z").toEpochMilli());

    awaitUntilConsumedEvent(expectedEvent, RunState.State.QUEUED);
  }
  }

  @RunWith(JUnitParamsRunner.class)
  public static class ShouldCatchUpWithNaturalTriggersTest extends SystemTest {
  @Test
  public void shouldCatchUpWithNaturalTriggers() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T12:00:00Z");

    styxStarts();
    tickTriggerManager();
    awaitWorkflowInstanceState(
        create(HOURLY_WORKFLOW.id(), "2016-03-14T12"),
        RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance = getDockerRuns().get(0)._1.workflowInstance();
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(workflowInstance.parameter(), is("2016-03-14T12"));

    tickTriggerManager();
    awaitWorkflowInstanceState(
        create(HOURLY_WORKFLOW.id(), "2016-03-14T13"),
        RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(2);

    workflowInstance = getDockerRuns().get(1)._1.workflowInstance();
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(workflowInstance.parameter(), is("2016-03-14T13"));

    tickTriggerManager();
    awaitWorkflowInstanceState(
        create(HOURLY_WORKFLOW.id(), "2016-03-14T14"),
        RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(3);

    workflowInstance = getDockerRuns().get(2)._1.workflowInstance();
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(workflowInstance.parameter(), is("2016-03-14T14"));
  }
  }

  @RunWith(JUnitParamsRunner.class)
  public static class TestTriggerBackfillsWithinResourceLimitTest extends SystemTest {
  @Test
  public void testTriggerBackfillsWithinResourceLimit() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, false);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");
    givenBackfill(ONE_DAY_HOURLY_BACKFILL);

    styxStarts();
    tickBackfillTriggerManager();
    awaitWorkflowInstanceState(
        create(HOURLY_WORKFLOW.id(), "2015-01-01T00"),
        RunState.State.QUEUED);
    awaitWorkflowInstanceState(
        create(HOURLY_WORKFLOW.id(), "2015-01-01T01"),
        RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(2);
  }
  }

  @RunWith(JUnitParamsRunner.class)
  public static class TestTriggerBackfillsWillCompleteTest extends SystemTest {
  @Test
  public void testTriggerBackfillsWillComplete() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, false);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");
    final Backfill singleHourBackfill = ONE_DAY_HOURLY_BACKFILL.builder()
        .end(ONE_DAY_HOURLY_BACKFILL.start().plus(1, ChronoUnit.HOURS))
        .triggerParameters(TriggerParameters.builder()
            .env("BAR", "overridden-bar",
                "BAZ", "baz")
            .build())
        .build();
    givenBackfill(singleHourBackfill);

    styxStarts();
    tickBackfillTriggerManager();
    awaitWorkflowInstanceState(
        create(HOURLY_WORKFLOW.id(), "2015-01-01T00"),
        RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(1);
    WorkflowInstance workflowInstance = getDockerRuns().get(0)._1.workflowInstance();
    RunSpec runSpec = getDockerRuns().get(0)._2;

    assertThat(runSpec.env(), is(Map.of(
        "FOO", "foo",
        "BAR", "overridden-bar",
        "BAZ", "baz")));

    injectEvent(Event.started(workflowInstance));
    injectEvent(Event.terminate(workflowInstance, Optional.of(0)));
    awaitWorkflowInstanceCompletion(workflowInstance);
    awaitBackfillCompleted(singleHourBackfill.id());
    tickScheduler();
    assertThat(getState(workflowInstance), is(Optional.empty()));
  }
  }

  @RunWith(JUnitParamsRunner.class)
  public static class RemovedEnabledWorkflowWontGetScheduledTest extends SystemTest {
  @Test
  public void removedEnabledWorkflowWontGetScheduled() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T13:00:00Z");

    WorkflowInstance instance1 = create(HOURLY_WORKFLOW.id(), "2016-03-14T13");
    WorkflowInstance instance2 = create(HOURLY_WORKFLOW.id(), "2016-03-14T14");

    styxStarts();
    tickTriggerManager();
    awaitWorkflowInstanceState(instance1, RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(1);

    assertThat(getDockerRuns().get(0)._1.workflowInstance(), is(instance1));

    workflowDeleted(HOURLY_WORKFLOW);
    tickTriggerManager();

    assertThat(getState(instance2), is(Optional.empty()));
  }
  }

  @RunWith(JUnitParamsRunner.class)
  public static class UpdatesNextNaturalTriggerWhenWFScheduleChangesFromFinerToCoarserTest extends SystemTest {
  @Test
  public void updatesNextNaturalTriggerWhenWFScheduleChangesFromFinerToCoarser() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T14:00:00Z");
    WorkflowInstance workflowInstance =
        create(HOURLY_WORKFLOW.id(), "2016-03-14T14");

    styxStarts();
    tickTriggerManager();
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(1);

    workflowInstance = getDockerRuns().get(0)._1.workflowInstance();
    assertThat(workflowInstance.parameter(), is("2016-03-14T14"));

    workflowInstance = create(HOURLY_WORKFLOW.id(), "2016-03-14");
    // this should store a new value for nextNaturalTrigger, 2016-03-14
    workflowChanges(DAILY_WORKFLOW);
    timeJumps(1, DAYS); // to 2016-03-15
    tickTriggerManager();
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(2);

    workflowInstance = getDockerRuns().get(1)._1.workflowInstance();
    assertThat(workflowInstance.parameter(), is("2016-03-14"));
  }
  }

  @RunWith(JUnitParamsRunner.class)
  public static class UpdatesNextNaturalTriggerWhenWFOffsetChangesTest extends SystemTest {
  @Test
  public void updatesNextNaturalTriggerWhenWFOffsetChanges() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T14:00:00Z");
    WorkflowInstance workflowInstance =
        create(HOURLY_WORKFLOW.id(), "2016-03-14T14");

    Workflow workflow;
    TriggerInstantSpec triggerInstantSpec;

    workflow = storage.workflow(workflowInstance.workflowId()).orElseThrow();
    assertThat(workflow.configuration().offset(), is(Optional.empty()));

    triggerInstantSpec = storage.workflowsWithNextNaturalTrigger().get(workflow);
    assertThat(triggerInstantSpec.instant(),
               is(Instant.parse("2016-03-14T14:00:00Z")));
    assertThat(triggerInstantSpec.offsetInstant(),
               is(Instant.parse("2016-03-14T15:00:00Z")));

    styxStarts();
    tickTriggerManager();
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(1);

    workflowInstance = getDockerRuns().get(0)._1.workflowInstance();
    assertThat(workflowInstance.parameter(), is("2016-03-14T14"));

    triggerInstantSpec = storage.workflowsWithNextNaturalTrigger().get(workflow);
    assertThat(triggerInstantSpec.instant(),
               is(Instant.parse("2016-03-14T15:00:00Z")));
    assertThat(triggerInstantSpec.offsetInstant(),
               is(Instant.parse("2016-03-14T16:00:00Z")));

    workflowInstance = create(HOURLY_WORKFLOW.id(), "2016-03-14T16");
    // this should store a new value for nextNaturalTrigger, 2016-03-14T16
    workflowChanges(HOURLY_WORKFLOW_WITH_ZERO_OFFSET);
    workflow = storage.workflow(workflowInstance.workflowId()).orElseThrow();

    triggerInstantSpec = storage.workflowsWithNextNaturalTrigger().get(workflow);
    assertThat(triggerInstantSpec.instant(),
               is(Instant.parse("2016-03-14T16:00:00Z")));
    assertThat(triggerInstantSpec.offsetInstant(),
               is(Instant.parse("2016-03-14T16:00:00Z")));

    givenTheTimeIs("2016-03-14T16:30:00Z");

    tickTriggerManager();
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(2);

    workflowInstance = getDockerRuns().get(1)._1.workflowInstance();
    assertThat(workflowInstance.parameter(), is("2016-03-14T16"));

    triggerInstantSpec = storage.workflowsWithNextNaturalTrigger().get(workflow);
    assertThat(triggerInstantSpec.instant(),
               is(Instant.parse("2016-03-14T17:00:00Z")));
    assertThat(triggerInstantSpec.offsetInstant(),
               is(Instant.parse("2016-03-14T17:00:00Z")));

    givenTheTimeIs("2016-03-14T17:30:00Z");

    workflowInstance = create(HOURLY_WORKFLOW.id(), "2016-03-14T16");
    // this should store a the same value for nextNaturalTrigger, 2016-03-14T16
    workflowChanges(HOURLY_WORKFLOW_WITH_TWO_HOURS_OFFSET);
    workflow = storage.workflow(workflowInstance.workflowId()).orElseThrow();

    triggerInstantSpec = storage.workflowsWithNextNaturalTrigger().get(workflow);
    assertThat(triggerInstantSpec.instant(),
        is(Instant.parse("2016-03-14T16:00:00Z")));
    assertThat(triggerInstantSpec.offsetInstant(),
        is(Instant.parse("2016-03-14T18:00:00Z")));

    givenTheTimeIs("2016-03-14T18:30:00Z");

    tickTriggerManager();
    // the instance 2016-03-14T16 is still active and should reach SUBMITTED state
    awaitWorkflowInstanceState(workflowInstance, RunState.State.SUBMITTED);

    workflowInstance = getDockerRuns().get(1)._1.workflowInstance();
    assertThat(workflowInstance.parameter(), is("2016-03-14T16"));

    triggerInstantSpec = storage.workflowsWithNextNaturalTrigger().get(workflow);
    assertThat(triggerInstantSpec.instant(),
        is(Instant.parse("2016-03-14T17:00:00Z")));
    assertThat(triggerInstantSpec.offsetInstant(),
        is(Instant.parse("2016-03-14T19:00:00Z")));
  }
  }

  @RunWith(JUnitParamsRunner.class)
  public static class UpdatesNextNaturalTriggerWhenWFScheduleChangesFromCoarserToFinerTest extends SystemTest {
  @Test
  public void updatesNextNaturalTriggerWhenWFScheduleChangesFromCoarserToFiner() throws Exception {
    givenTheTimeIs("2016-03-14T15:30:00Z");
    givenWorkflow(DAILY_WORKFLOW);
    givenWorkflowEnabledStateIs(DAILY_WORKFLOW, true);
    givenNextNaturalTrigger(DAILY_WORKFLOW, "2016-03-13T00:00:00Z");
    WorkflowInstance workflowInstance =
        create(DAILY_WORKFLOW.id(), "2016-03-13");

    styxStarts();
    tickTriggerManager();
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);
    tickScheduler();
    awaitNumberOfDockerRuns(1);

    workflowInstance = getDockerRuns().get(0)._1.workflowInstance();
    assertThat(workflowInstance.parameter(), is("2016-03-13"));

    workflowInstance = create(HOURLY_WORKFLOW.id(), "2016-03-14T15");
    // this should store a new value for nextNaturalTrigger, 2016-03-14T15
    workflowChanges(HOURLY_WORKFLOW);
    timeJumps(1, HOURS); // to 16:30
    tickTriggerManager();
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);
    tickScheduler();

    awaitNumberOfDockerRuns(2);
    workflowInstance = getDockerRuns().get(1)._1.workflowInstance();
    assertThat(workflowInstance.parameter(), is("2016-03-14T15"));
  }
  }

  @RunWith(JUnitParamsRunner.class)
  public static class RunsDockerImageWithArgsTemplateTest extends SystemTest {
  @Test
  public void runsDockerImageWithArgsTemplateAndEnv() throws Exception {
    givenTheTimeIs("2016-03-14T15:59:01Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T15:00:00Z");

    styxStarts();
    timePasses(1, MINUTES);
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance = getDockerRuns().get(0)._1.workflowInstance();
    RunSpec runSpec = getDockerRuns().get(0)._2;
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(naturalRunSpec(runSpec.executionId(), "busybox", List.of("--hour", "2016-03-14T15"),
        Map.of("FOO", "foo", "BAR", "bar"))));
  }
  }

  @RunWith(JUnitParamsRunner.class)
  public static class RetriesUseLatestWorkflowSpecificationTest extends SystemTest {
  @Test
  public void retriesUseLatestWorkflowSpecification() throws Exception {
    givenTheTimeIs("2016-03-14T15:59:01Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T15:00:00Z");

    styxStarts();
    timePasses(1, MINUTES);
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance = getDockerRuns().get(0)._1.workflowInstance();
    RunSpec runSpec = getDockerRuns().get(0)._2;
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(naturalRunSpec(runSpec.executionId(), "busybox", List.of("--hour", "2016-03-14T15"),
        Map.of("FOO", "foo", "BAR", "bar"))));

    injectEvent(Event.started(workflowInstance));
    injectEvent(Event.terminate(workflowInstance, Optional.of(20)));
    awaitWorkflowInstanceState(workflowInstance, RunState.State.QUEUED);

    WorkflowConfiguration changedWorkflowConfiguration = WorkflowConfiguration.builder()
        .id(WORKFLOW_CONFIGURATION_HOURLY.id())
        .schedule(Schedule.HOURS)
        .dockerImage("busybox:v777")
        .dockerArgs(asList("other", "args"))
        .env("QUUX", "quux")
        .build();

    Workflow changedWorkflow = Workflow.create(
        HOURLY_WORKFLOW.componentId(),
        changedWorkflowConfiguration);

    workflowChanges(changedWorkflow);

    // we must stagger the time progression here in order not to no trigger tons of retries
    timePasses(TerminationHandler.MISSING_DEPS_RETRY_DELAY_MINUTES - 1, MINUTES);
    timePasses(59, SECONDS);
    timePasses(StyxScheduler.DEFAULT_SCHEDULER_TICK_INTERVAL.getSeconds() * 2, SECONDS);

    awaitNumberOfDockerRunsAtLeast(2);

    WorkflowInstance workflowInstance2 = getDockerRuns().get(1)._1.workflowInstance();
    RunSpec runSpec2 = getDockerRuns().get(1)._2;
    assertThat(workflowInstance2.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec2, is(naturalRunSpec(runSpec2.executionId(), "busybox:v777", List.of("other", "args"),
        Map.of("QUUX", "quux"))));
  }
  }

  @RunWith(JUnitParamsRunner.class)
  public static class RestoredStatesUseOriginalTimestampsTest extends SystemTest {
  @Test
  public void restoredStatesUseOriginalTimestamps() throws Exception {
    WorkflowInstance workflowInstance = create(HOURLY_WORKFLOW.id(), "2016-03-14T10");

    givenTheTimeIs("2016-03-14T15:17:45Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");

    givenActiveState(workflowInstance, RunState.create(workflowInstance,
        RunState.State.QUEUED, StateData.zero(),
        Instant.ofEpochMilli(timeOffsetSeconds(4)), 3L));

    givenTheTimeIs("2016-03-14T16:01:00Z");
    styxStarts();

    RunState state = getState(workflowInstance).orElseThrow();
    Instant stateTime = Instant.ofEpochMilli(state.timestamp());

    assertThat(stateTime, is(Instant.parse("2016-03-14T15:17:49Z")));
  }
  }

  @RunWith(JUnitParamsRunner.class)
  public static class RestoresActiveStatesTest extends SystemTest {
  @Test
  public void restoresActiveStates() throws Exception {
    WorkflowInstance workflowInstance = create(HOURLY_WORKFLOW.id(), "2016-03-14T14");

    givenTheTimeIs("2016-03-14T15:17:45Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");

    givenActiveState(workflowInstance, RunState.create(workflowInstance,
        RunState.State.QUEUED, StateData.newBuilder().trigger(TRIGGER1).build(),
        Instant.parse("2016-03-14T15:17:45Z"), 13L));

    styxStarts();

    timePasses(1, MINUTES);
    awaitNumberOfDockerRuns(1);

    WorkflowInstance workflowInstance2 = getDockerRuns().get(0)._1.workflowInstance();
    RunSpec runSpec = getDockerRuns().get(0)._2;
    assertThat(workflowInstance2.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(
        unknownRunSpec(runSpec.executionId(), "busybox", List.of("--hour", "2016-03-14T14"), "trig1",
            Map.of("FOO", "foo", "BAR", "bar"))));
  }
  }

  // TODO: rewrite
  @RunWith(JUnitParamsRunner.class)
  public static class PollsPodStateWhenStartingTest extends SystemTest {
  @Test
  public void pollsPodStateWhenStarting() throws Exception {
    WorkflowInstance workflowInstance = create(HOURLY_WORKFLOW.id(), "2016-03-14T14");

    givenTheTimeIs("2016-03-14T15:17:45Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
    givenNextNaturalTrigger(HOURLY_WORKFLOW, "2016-03-14T16:00:00Z");

    givenActiveState(workflowInstance, RunState.create(workflowInstance,
        RunState.State.RUNNING,
        StateData.newBuilder().executionId("id").executionDescription(ExecutionDescription.forImage(
            "image")).trigger(TRIGGER1).build(),
        Instant.parse("2016-03-14T15:17:45Z"), 2L));

    styxStarts();

    timePasses(1, MINUTES);

    // Verify that styx polled for execution status
    assertThat(dockerPolls.size(), is(greaterThanOrEqualTo(1)));

    // Simulate the runner emitting a successful termination event
    injectEvent(Event.terminate(workflowInstance, Optional.of(0)));

    awaitWorkflowInstanceCompletion(workflowInstance);
    tickScheduler();
    assertThat(getState(workflowInstance), is(Optional.empty()));
  }
  }
}
