/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2020 Spotify AB
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

package com.spotify.styx.test;

import static com.spotify.styx.model.Schedule.HOURS;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.styx.flyte.FlyteAdminClientRunner;
import com.spotify.styx.flyte.FlyteRunner;
import com.spotify.styx.flyte.client.FlyteAdminClient;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.model.FlyteIdentifier;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.EventRouter;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.core.Execution;
import flyteidl.core.IdentifierOuterClass;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.util.Optional;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class FlyteAdminClientRunnerTest {

  private static final FlyteExecConf FLYTE_EXEC_CONF = FlyteExecConf.builder()
      .referenceId(FlyteIdentifier.builder()
          .project("flyte-test")
          .domain("testing")
          .name("wf-name")
          .version("1234")
          .resourceType("lp")
          .build())
      .build();
  @Mock private FlyteAdminClient flyteAdminClient;
  @Mock private StateManager stateManager;
  @Mock private RunState runState;
  @Mock EventRouter eventRouter;

  private FlyteAdminClientRunner flyteRunner;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    flyteRunner = new FlyteAdminClientRunner(flyteAdminClient, stateManager);
  }

  @Test
  public void testIsEnabled() {
    assertTrue(flyteRunner.isEnabled());
  }

  @Test
  public void testCreateExecution() throws FlyteRunner.CreateExecutionException {
    when(flyteAdminClient.createExecution(any(), any(), any(), any(),any())).thenReturn(
        ExecutionOuterClass.ExecutionCreateResponse
            .newBuilder()
            .setId(IdentifierOuterClass.WorkflowExecutionIdentifier
                .newBuilder()
                .setProject("flyte-test")
                .setDomain("testing")
                .setName("test-create-execution")
                .build())
            .build());
    var flyteExecution = flyteRunner.createExecution("test-create-execution", FLYTE_EXEC_CONF);

    assertThat(flyteExecution.getProject(), is("flyte-test"));
    assertThat(flyteExecution.getDomain(), is("testing"));
    assertThat(flyteExecution.getName(), is("test-create-execution"));
    assertThat(flyteExecution.toUrn(), is("ex:flyte-test:testing:test-create-execution"));
  }

  @Test
  public void testThrowsFlyteLaunchPlanNotFound() {
    doThrow(new StatusRuntimeException(Status.NOT_FOUND))
        .when(flyteAdminClient).createExecution(any(), any(), any(), any(),any());

    assertThrows(
        FlyteRunner.LaunchPlanNotFound.class,
        () -> flyteRunner.createExecution("exec", FLYTE_EXEC_CONF));
  }

  @Test
  public void testThrowsCreateExecutionExceptionForOtherCode() {
    doThrow(new StatusRuntimeException(Status.INTERNAL))
        .when(flyteAdminClient).createExecution(any(), any(), any(), any(),any());

    assertThrows(
        FlyteRunner.CreateExecutionException.class,
        () -> flyteRunner.createExecution("exec", FLYTE_EXEC_CONF));
  }

  @Test
  public void testThrowsCreateExecutionExceptionForUnknownException() {
    doThrow(new IllegalStateException("test"))
        .when(flyteAdminClient).createExecution(any(), any(), any(), any(),any());

    assertThrows(
        FlyteRunner.CreateExecutionException.class,
        () -> flyteRunner.createExecution("exec", FLYTE_EXEC_CONF));
  }

  @Test
  public void testPollProjectCannotBeNull() {
    assertThrows(
        NullPointerException.class,
        () ->    flyteRunner.poll(null, "testing", "test-null-project", null)
    );
  }

  @Test
  public void testPollDomainCannotBeNull() {
    assertThrows(
        NullPointerException.class,
        () ->    flyteRunner.poll("flyte-test", null, "test-null-domain", null)
    );
  }

  @Test
  public void testPollNameCannotBeNull() {
    assertThrows(
        NullPointerException.class,
        () ->    flyteRunner.poll("flyte-test", "testing", null, null)
    );
  }

  @Test
  public void testTransititionRunningToTerminateSuccessfulRun() throws Exception {
    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");


    final ExecutionOuterClass.Execution mockExecution = Mockito.mock(ExecutionOuterClass.Execution.class);
    when(mockExecution.getClosure().getPhase()).thenReturn(Execution.WorkflowExecution.Phase.SUCCEEDED);
    when(runState.workflowInstance()).thenReturn(workflowInstance);

    flyteRunner.poll("flyte-test", "testing", "execution-name", runState);
    verify(eventRouter,  timeout(60_000)).receive(Event.terminate(workflowInstance, Optional.of(1)));
  }

  @Test
  @Parameters({
      "FAILED, USER:NotReady, 20",
      "ABORTED, USER:NotReady, 20",
      "TIMED_OUT, USER:NotReady, 20",
      "FAILED, USER:NotRetryble, 50",
      "ABORTED, USER:NotRetryble, 50",
      "TIMED_OUT, USER:NotRetryble, 50",
      "FAILED, USER:AnythingElse, 1",
      "ABORTED, USER:AnythingElse, 1",
      "TIMED_OUT, USER:AnythingElse, 1",
  })
  public void testTransititionRunningToTerminateExitCodes(Execution.WorkflowExecution.Phase phase, String flyteExitCode, int exitCode) throws Exception {
    Workflow workflow = Workflow.create("id", configuration());
    WorkflowInstance workflowInstance = WorkflowInstance.create(workflow.id(), "2016-03-14");

    final ExecutionOuterClass.Execution mockExecution = Mockito.mock(ExecutionOuterClass.Execution.class);
    when(mockExecution.getClosure().getPhase()).thenReturn(phase);
    when(mockExecution.getClosure().getError().getCode()).thenReturn(flyteExitCode);
    when(runState.workflowInstance()).thenReturn(workflowInstance);

    flyteRunner.poll("flyte-test", "testing", "execution-name", runState);
    verify(eventRouter,  timeout(60_000)).receive(Event.terminate(workflowInstance, Optional.of(exitCode)));
  }

  public WorkflowConfiguration configuration(String... args) {
    return WorkflowConfiguration.builder()
        .id("styx.TestEndpoint")
        .schedule(HOURS)
        .flyteExecConf(FlyteExecConf
            .builder()
            .referenceId(FlyteIdentifier
                .builder()
                .resourceType("lp")
                .project("flyte-test")
                .domain("testing")
                .name("test-lp")
                .version("1")
                .build())
            .build()
        )
        .build();
  }
}
