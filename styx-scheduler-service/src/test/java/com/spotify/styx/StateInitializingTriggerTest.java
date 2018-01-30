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

import static com.spotify.styx.model.Schedule.DAYS;
import static com.spotify.styx.model.Schedule.HOURS;
import static com.spotify.styx.model.Schedule.MONTHS;
import static com.spotify.styx.model.Schedule.WEEKS;
import static com.spotify.styx.util.ParameterUtil.toParameter;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfigurationBuilder;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.testdata.TestData;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
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

  @Mock StateManager stateManager;

  private TriggerListener trigger;

  @Before
  public void setUp() throws Exception {
    trigger = new StateInitializingTrigger(stateManager);
  }

  @Test
  public void shouldTriggerWorkflowInstance() throws Exception {
    WorkflowConfiguration workflowConfiguration = workflowConfiguration(HOURS);
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    trigger.event(workflow, NATURAL_TRIGGER, TIME);

    verify(stateManager).trigger(
        WorkflowInstance.create(workflow.id(), toParameter(workflowConfiguration.schedule(), TIME)),
        Trigger.natural());
  }

  @Test
  public void shouldInitializeWorkflowInstanceWithoutDockerArgs() throws Exception {
    WorkflowConfiguration workflowConfiguration =
        WorkflowConfigurationBuilder.from(workflowConfiguration(HOURS)).dockerArgs(Optional.empty()).build();
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    trigger.event(workflow, NATURAL_TRIGGER, TIME);

    WorkflowInstance expectedInstance = WorkflowInstance.create(workflow.id(),
        toParameter(workflowConfiguration.schedule(), TIME));

    verify(stateManager).trigger(expectedInstance, Trigger.natural());
  }

  @Test
  public void shouldInjectTriggerExecutionEventWithNaturalTrigger() throws Exception {
    WorkflowConfiguration workflowConfiguration = workflowConfiguration(HOURS);
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    trigger.event(workflow, NATURAL_TRIGGER, TIME);

    WorkflowInstance expectedInstance = WorkflowInstance.create(workflow.id(), "2016-01-18T09");

    verify(stateManager).trigger(expectedInstance, Trigger.natural());
  }

  @Test
  public void shouldInjectTriggerExecutionEventWithBackfillTrigger() throws Exception {
    WorkflowConfiguration workflowConfiguration = workflowConfiguration(HOURS);
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    trigger.event(workflow, BACKFILL_TRIGGER, TIME);

    WorkflowInstance expectedInstance = WorkflowInstance.create(workflow.id(), "2016-01-18T09");

    verify(stateManager).trigger(expectedInstance, BACKFILL_TRIGGER);
  }

  @Test
  public void shouldDoNothingIfDockerImageMissing() throws Exception {
    final WorkflowConfiguration configuration =
        WorkflowConfigurationBuilder.from(TestData.DAILY_WORKFLOW_CONFIGURATION)
            .dockerImage(Optional.empty())
            .build();
    Workflow workflow = Workflow.create("id", configuration);
    trigger.event(workflow, NATURAL_TRIGGER, TIME);

    verifyZeroInteractions(stateManager);
  }

  @Test
  @Parameters()
  public void shouldCreateWorkflowInstanceParameter() throws Exception {
    for (Map.Entry<Schedule, String> scheduleCase : SCHEDULE_ARG_EXPECTS.entrySet()) {
      reset(stateManager);
      WorkflowConfiguration workflowConfiguration = workflowConfiguration(
          scheduleCase.getKey(), "--date", "{}", "--bar");
      Workflow workflow = Workflow.create("id", workflowConfiguration);
      trigger.event(workflow, NATURAL_TRIGGER, TIME);

      WorkflowInstance expectedInstance = WorkflowInstance.create(workflow.id(), scheduleCase.getValue());

      verify(stateManager).trigger(expectedInstance, Trigger.natural());
    }
  }

  @Test
  public void testsShouldCoverAllScheduleCases() throws Exception {
    assertThat(SCHEDULE_ARG_EXPECTS.keySet(), is(Sets.newHashSet(Schedule.values())));
  }

  private WorkflowConfiguration workflowConfiguration(Schedule schedule, String... args) {
    return WorkflowConfiguration.builder()
        .id("styx.TestEndpoint")
        .schedule(schedule)
        .dockerImage("busybox")
        .dockerArgs(ImmutableList.copyOf(args))
        .build();
  }
}
