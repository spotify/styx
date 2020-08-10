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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

import com.google.common.collect.ImmutableList;
import com.spotify.styx.model.DockerExecConfBuilder;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfigurationBuilder;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.IsClosedException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
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

  private static final Map<Schedule, String> SCHEDULE_ARG_EXPECTS =
      Map.of(
          WEEKS, "2016-01-18",
          DAYS, "2016-01-18",
          HOURS, "2016-01-18T09",
          MONTHS, "2016-01"
      );

  @Mock StateManager stateManager;

  private TriggerListener trigger;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    trigger = new StateInitializingTrigger(stateManager);
  }

  @Test
  @Parameters(method = "validConfigurations")
  public void shouldTriggerWorkflowInstance(WorkflowConfiguration workflowConfiguration) throws Exception {
    Workflow workflow = Workflow.create("id", workflowConfiguration);
    trigger.event(workflow, NATURAL_TRIGGER, TIME, PARAMETERS);

    verify(stateManager).trigger(
        WorkflowInstance.create(workflow.id(), toParameter(workflowConfiguration.schedule(), TIME)),
        Trigger.natural(), PARAMETERS);
  }

  @SuppressWarnings("unused")
  private static WorkflowConfiguration[] validConfigurations() {
    return new WorkflowConfiguration[] {
        workflowConfiguration(HOURS),
        TestData.DOCKER_EXEC_WORKFLOW_CONFIGURATION
    };
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
  @Parameters(method = "noDockerImageConfigurations")
  public void shouldDoNothingIfDockerImageMissing(WorkflowConfiguration configuration) {
    var workflow = Workflow.create("id", configuration);
    trigger.event(workflow, NATURAL_TRIGGER, TIME, PARAMETERS);

    verifyNoInteractions(stateManager);
  }

  @SuppressWarnings("unused")
  private static WorkflowConfiguration[] noDockerImageConfigurations() {
    return new WorkflowConfiguration[] {
        WorkflowConfigurationBuilder.from(TestData.DAILY_WORKFLOW_CONFIGURATION)
            .dockerImage(Optional.empty())
            .build(),
        WorkflowConfigurationBuilder.from(TestData.DOCKER_EXEC_WORKFLOW_CONFIGURATION)
            .dockerExecConf(new DockerExecConfBuilder()
                .dockerImage(Optional.empty())
                .build())
            .build()
    };
  }

  @Test
  public void shouldDoNothingIfFlyteWorkflow() {
    var workflow = Workflow.create("id", TestData.FLYTE_WORKFLOW_CONFIGURATION);
    trigger.event(workflow, NATURAL_TRIGGER, TIME, PARAMETERS);

    verifyNoInteractions(stateManager);
  }

  @Test
  public void shouldCreateWorkflowInstanceParameter() throws Exception {
    for (Map.Entry<Schedule, String> scheduleCase : SCHEDULE_ARG_EXPECTS.entrySet()) {
      reset(stateManager);
      WorkflowConfiguration workflowConfiguration = workflowConfiguration(
          scheduleCase.getKey(), "--date", "{}", "--bar");
      Workflow workflow = Workflow.create("id", workflowConfiguration);
      trigger.event(workflow, NATURAL_TRIGGER, TIME, PARAMETERS);

      WorkflowInstance expectedInstance = WorkflowInstance.create(workflow.id(), scheduleCase.getValue());

      verify(stateManager).trigger(expectedInstance, Trigger.natural(), PARAMETERS);
    }
  }

  private static WorkflowConfiguration workflowConfiguration(Schedule schedule, String... args) {
    return WorkflowConfiguration.builder()
        .id("styx.TestEndpoint")
        .schedule(schedule)
        .dockerImage("busybox")
        .dockerArgs(ImmutableList.copyOf(args))
        .build();
  }

  @Test
  public void shouldWrapIsClosedExceptionIntoRuntime() throws Exception {
    var cause = new IsClosedException();
    doThrow(cause).when(stateManager).trigger(any(), any(), any());
    Workflow workflow = Workflow.create("id", workflowConfiguration(HOURS));

    var exception = assertThrows(
        RuntimeException.class,
        () -> trigger.event(workflow, NATURAL_TRIGGER, TIME, PARAMETERS)
    );

    assertThat(exception.getCause(), equalTo(cause));
  }
}
