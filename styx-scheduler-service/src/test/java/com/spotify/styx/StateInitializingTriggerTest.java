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

import static com.github.npathai.hamcrestopt.OptionalMatchers.hasValue;
import static com.spotify.styx.model.Schedule.DAYS;
import static com.spotify.styx.model.Schedule.HOURS;
import static com.spotify.styx.model.Schedule.MONTHS;
import static com.spotify.styx.model.Schedule.WEEKS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.SyncStateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.TriggerUtil;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import org.junit.Test;

public class StateInitializingTriggerTest {

  private static final Instant TIME = Instant.parse("2016-01-18T09:11:22.333Z");
  private static final Trigger NATURAL_TRIGGER = Trigger.natural();
  private static final Trigger BACKFILL_TRIGGER = Trigger.backfill("trig");

  private static final Map<Schedule, String> SCHEDULE_ARG_EXPECTS =
      ImmutableMap.of(
          WEEKS, "2016-01-18",
          DAYS, "2016-01-18",
          HOURS, "2016-01-18T09",
          MONTHS, "2016-01"
      );

  private SyncStateManager stateManager = new SyncStateManager();
  private Storage storage = mock(Storage.class);
  private TriggerListener
      trigger = new StateInitializingTrigger(RunState::fresh, stateManager, storage);

  @Test
  public void shouldInitializeWorkflowInstance() throws Exception {
    WorkflowConfiguration workflowConfiguration = schedule(HOURS);
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, workflowConfiguration);
    setDockerImage(workflow.id(), workflow.configuration());
    trigger.event(workflow, NATURAL_TRIGGER, TIME);

    assertThat(stateManager.activeStatesSize(), is(1));
  }

  @Test
  public void shouldInjectTriggerExecutionEventWithNaturalTrigger() throws Exception {
    WorkflowConfiguration workflowConfiguration = schedule(HOURS);
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, workflowConfiguration);
    setDockerImage(workflow.id(), workflow.configuration());
    trigger.event(workflow, NATURAL_TRIGGER, TIME);

    WorkflowInstance expectedInstance = WorkflowInstance.create(workflow.id(), "2016-01-18T09");
    RunState state = stateManager.get(expectedInstance);

    assertThat(state.state(), is(RunState.State.QUEUED));
    assertThat(state.data().triggerId(), hasValue(TriggerUtil.NATURAL_TRIGGER_ID));
    assertThat(state.data().trigger(), hasValue(Trigger.natural()));
  }

  @Test
  public void shouldInjectTriggerExecutionEventWithBackfillTrigger() throws Exception {
    WorkflowConfiguration workflowConfiguration = schedule(HOURS);
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, workflowConfiguration);
    setDockerImage(workflow.id(), workflow.configuration());
    trigger.event(workflow, BACKFILL_TRIGGER, TIME);

    WorkflowInstance expectedInstance = WorkflowInstance.create(workflow.id(), "2016-01-18T09");
    RunState state = stateManager.get(expectedInstance);

    assertThat(state.state(), is(RunState.State.QUEUED));
    assertThat(state.data().triggerId(), hasValue("trig"));
    assertThat(state.data().trigger(), hasValue(Trigger.backfill("trig")));
  }

  @Test
  public void shouldDoNothingIfDockerInfoMissing() throws Exception {
    Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, TestData.DAILY_WORKFLOW_CONFIGURATION);
    setDockerImage(workflow.id(), workflow.configuration());
    trigger.event(workflow, NATURAL_TRIGGER, TIME);

    assertThat(stateManager.activeStatesSize(), is(0));
  }

  @Test
  public void shouldCreateWorkflowInstanceParameter() throws Exception {
    for (Map.Entry<Schedule, String> scheduleCase : SCHEDULE_ARG_EXPECTS.entrySet()) {
      WorkflowConfiguration
          workflowConfiguration = schedule(scheduleCase.getKey(), "--date", "{}", "--bar");
      Workflow workflow = Workflow.create("id", TestData.WORKFLOW_URI, workflowConfiguration);
      setDockerImage(workflow.id(), workflow.configuration());
      trigger.event(workflow, NATURAL_TRIGGER, TIME);

      RunState runState =
          stateManager.get(WorkflowInstance.create(workflow.id(), scheduleCase.getValue()));

      assertThat(runState, is(notNullValue()));
      assertThat(runState.workflowInstance().parameter(), is(scheduleCase.getValue()));
    }
  }

  @Test
  public void testsShouldCoverAllScheduleCases() throws Exception {
    assertThat(SCHEDULE_ARG_EXPECTS.keySet(), is(Sets.newHashSet(Schedule.values())));
  }

  private WorkflowConfiguration schedule(Schedule schedule, String... args) {
    return WorkflowConfiguration.builder()
        .id("styx.TestEndpoint")
        .schedule(schedule)
        .dockerImage("busybox")
        .dockerArgs(ImmutableList.copyOf(args))
        .build();
  }

  // todo: do not use deprecated getDockerImage method
  private void setDockerImage(WorkflowId workflowId, WorkflowConfiguration workflowConfiguration) throws IOException {
    when(storage.getDockerImage(workflowId)).thenReturn(workflowConfiguration.dockerImage());
  }
}
