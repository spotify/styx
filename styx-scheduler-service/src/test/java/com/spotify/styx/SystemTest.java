/*
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

import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.model.DataEndpoint;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.handlers.TerminationHandler;
import com.spotify.styx.testdata.TestData;

import org.junit.Test;

import java.time.Instant;

import static com.spotify.styx.model.WorkflowInstance.create;
import static java.util.Arrays.asList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SystemTest extends StyxSchedulerServiceFixture {

  private static final DataEndpoint DATA_ENDPOINT = DataEndpoint.create(
      "styx.TestEndpoint", Partitioning.HOURS, of("busybox"), of(asList("--hour", "{}")),
      empty());
  private final static String TEST_EXECUTION_ID_1 = "execution_1";
  private final static String TEST_DOCKER_IMAGE = "busybox:1.1";

  private static final Workflow HOURLY_WORKFLOW = Workflow.create(
      "styx",
      TestData.WORKFLOW_URI,
      DATA_ENDPOINT);

  @Test
  public void runsDockerImageWithArgsTemplate() throws Exception {
    givenTheTimeIs("2016-03-14T15:59:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);

    styxStarts();
    timePasses(1, MINUTES);

    // todo: add semantic wait utility
    Thread.sleep(1000);

    assertThat(dockerRuns, hasSize(1));
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

    styxStarts();
    timePasses(1, MINUTES);

    // todo: add semantic wait utility
    Thread.sleep(1000);

    assertThat(dockerRuns, hasSize(1));
    WorkflowInstance workflowInstance = dockerRuns.get(0)._1;
    RunSpec runSpec = dockerRuns.get(0)._2;
    assertThat(workflowInstance.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T15")));

    injectEvent(Event.started(workflowInstance));
    injectEvent(Event.terminate(workflowInstance, 20));

    // todo: add semantic wait utility
    Thread.sleep(1000);

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

    // todo: add semantic wait utility
    Thread.sleep(1000);

    assertThat(dockerRuns, hasSize(2));
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

    styxStarts();
    timePasses(1, MINUTES);

    // todo: add semantic wait utility
    Thread.sleep(1000);

    assertThat(dockerRuns, hasSize(1));
    WorkflowInstance workflowInstance = dockerRuns.get(0)._1;

    injectEvent(Event.started(workflowInstance));
    injectEvent(Event.terminate(workflowInstance, 20));

    // todo: add semantic wait utility
    Thread.sleep(1000);

    assertThat(dockerCleans, contains(TEST_EXECUTION_ID));
  }

  @Test
  public void cleansUpDockerRunsWhenFailing() throws Exception {
    givenTheTimeIs("2016-03-14T15:59:00Z");
    givenTheGlobalEnableFlagIs(true);
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);

    styxStarts();
    timePasses(1, MINUTES);

    // todo: add semantic wait utility
    Thread.sleep(1000);

    assertThat(dockerRuns, hasSize(1));
    WorkflowInstance workflowInstance = dockerRuns.get(0)._1;

    injectEvent(Event.runError(workflowInstance, "Something failed"));

    // todo: add semantic wait utility
    Thread.sleep(1000);

    assertThat(dockerCleans, contains(TEST_EXECUTION_ID));
  }

  @Test
  public void restoredStatesUseOriginalTimestamps() throws Exception {
    WorkflowInstance workflowInstance = create(HOURLY_WORKFLOW.id(), "2016-03-14T10");

    givenTheTimeIs("2016-03-14T15:17:45Z");
    givenWorkflow(HOURLY_WORKFLOW);
    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);

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

    // todo: add semantic wait utility
    Thread.sleep(1000);

    timePasses(1, MINUTES);

    // todo: add semantic wait utility
    Thread.sleep(1000);

    assertThat(dockerRuns, hasSize(1));
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

    // todo: add semantic wait utility
    Thread.sleep(1000);

    timePasses(1, MINUTES);

    // todo: add semantic wait utility
    Thread.sleep(1000);

    assertThat(dockerRuns, hasSize(1));
    WorkflowInstance workflowInstance2 = dockerRuns.get(0)._1;
    RunSpec runSpec = dockerRuns.get(0)._2;
    assertThat(workflowInstance2.workflowId(), is(HOURLY_WORKFLOW.id()));
    assertThat(runSpec, is(RunSpec.simple("busybox", "--hour", "2016-03-14T14")));
  }

//  @Test
//  public void publishesMessagesToPubSub() throws Exception {
//    givenTheTimeIs("2016-03-14T15:59:00Z");
//    givenTheGlobalEnableFlagIs(true);
//    givenWorkflow(HOURLY_WORKFLOW);
//    givenWorkflowEnabledStateIs(HOURLY_WORKFLOW, true);
//
//    styxStarts();
//    timePasses(1, MINUTES);
//
//    // todo: add semantic wait utility
//    Thread.sleep(1000);
//
//    assertThat(pubSubMessages, hasSize(1));
//    Message message = pubSubMessages.get(0);
//    assertThat(message.payloadAsString(), containsString(HOURLY_WORKFLOW.componentId()));
//    assertThat(message.payloadAsString(), containsString(HOURLY_WORKFLOW.endpointId()));
//    assertThat(message.payloadAsString(), containsString("ROLLING_OUT"));
//    assertThat(message.payloadAsString(), containsString("busybox"));
//
//    // fake that the docker image started
//    assertThat(dockerRuns, hasSize(1));
//    WorkflowInstance workflowInstance = dockerRuns.get(0)._1;
//    injectEvent(Event.started(workflowInstance));
//
//    // todo: add semantic wait utility
//    Thread.sleep(1000);
//
//    assertThat(pubSubMessages, hasSize(2));
//    Message message2 = pubSubMessages.get(1);
//    assertThat(message2.payloadAsString(), containsString(HOURLY_WORKFLOW.componentId()));
//    assertThat(message2.payloadAsString(), containsString(HOURLY_WORKFLOW.endpointId()));
//    assertThat(message2.payloadAsString(), containsString("DONE"));
//    assertThat(message2.payloadAsString(), containsString("busybox"));
//  }

  // todo: semantic waits
}
