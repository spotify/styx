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

import static com.spotify.styx.model.Schedule.HOURS;
import static com.spotify.styx.util.ParameterUtil.toParameter;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfigurationBuilder;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.testdata.TestData;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class StateInitializingTriggerTest {

  private static final Instant TIME = Instant.parse("2016-01-18T09:11:22.333Z");
  private static final Trigger NATURAL_TRIGGER = Trigger.natural();
  private static final Trigger BACKFILL_TRIGGER = Trigger.backfill("trig");
  private static final TriggerParameters PARAMETERS = TriggerParameters.builder()
      .env("FOO", "foo", "BAR", "bar")
      .build();

  @Mock StateManager stateManager;

  private TriggerListener trigger;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    trigger = new StateInitializingTrigger(stateManager);
    when(stateManager.trigger(any(), any(), any())).then(a ->
        CompletableFuture.completedFuture(RunState.create(a.<WorkflowInstance>getArgument(0), RunState.State.QUEUED)));
  }

  @Test
  public void shouldTriggerWorkflowInstance() throws Exception {
    WorkflowConfiguration workflowConfiguration = workflowConfiguration(HOURS);
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    trigger.event(workflow, NATURAL_TRIGGER, TIME, PARAMETERS);

    verify(stateManager).trigger(
        WorkflowInstance.create(workflow.id(), toParameter(workflowConfiguration.schedule(), TIME)),
        Trigger.natural(), PARAMETERS);
  }

  @Test
  public void shouldInitializeWorkflowInstanceWithoutDockerArgs() throws Exception {
    WorkflowConfiguration workflowConfiguration =
        WorkflowConfigurationBuilder.from(workflowConfiguration(HOURS)).dockerArgs(Optional.empty()).build();
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    trigger.event(workflow, NATURAL_TRIGGER, TIME, PARAMETERS);

    WorkflowInstance expectedInstance = WorkflowInstance.create(workflow.id(),
        toParameter(workflowConfiguration.schedule(), TIME));

    verify(stateManager).trigger(expectedInstance, Trigger.natural(), PARAMETERS);
  }

  @Test
  public void shouldInjectTriggerExecutionEventWithNaturalTrigger() throws Exception {
    WorkflowConfiguration workflowConfiguration = workflowConfiguration(HOURS);
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    trigger.event(workflow, NATURAL_TRIGGER, TIME, PARAMETERS);

    WorkflowInstance expectedInstance = WorkflowInstance.create(workflow.id(), "2016-01-18T09");

    verify(stateManager).trigger(expectedInstance, Trigger.natural(), PARAMETERS);
  }

  @Test
  public void shouldInjectTriggerExecutionEventWithBackfillTrigger() throws Exception {
    WorkflowConfiguration workflowConfiguration = workflowConfiguration(HOURS);
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    trigger.event(workflow, BACKFILL_TRIGGER, TIME, PARAMETERS);

    WorkflowInstance expectedInstance = WorkflowInstance.create(workflow.id(), "2016-01-18T09");

    verify(stateManager).trigger(expectedInstance, BACKFILL_TRIGGER, PARAMETERS);
  }

  @Test
  public void shouldDoNothingIfDockerImageMissing() throws Exception {
    final WorkflowConfiguration configuration =
        WorkflowConfigurationBuilder.from(TestData.DAILY_WORKFLOW_CONFIGURATION)
            .dockerImage(Optional.empty())
            .build();
    Workflow workflow = Workflow.create("id", configuration);
    trigger.event(workflow, NATURAL_TRIGGER, TIME, PARAMETERS);

    verifyZeroInteractions(stateManager);
  }

  @Parameters({ "WEEKS, 2016-01-18",
                "DAYS, 2016-01-18",
                "HOURS, 2016-01-18T09",
                "MONTHS, 2016-01" })
  @Test
  public void shouldCreateWorkflowInstanceParameter(String schedule, String instance) throws Exception {
    var workflowConfiguration = workflowConfiguration(Schedule.parse(schedule), "--date", "{}", "--bar");
    var workflow = Workflow.create("id", workflowConfiguration);
    trigger.event(workflow, NATURAL_TRIGGER, TIME, PARAMETERS);
    var expectedInstance = WorkflowInstance.create(workflow.id(), instance);
    verify(stateManager).trigger(expectedInstance, Trigger.natural(), PARAMETERS);
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
