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

package com.spotify.styx.flyte;

import static com.spotify.styx.flyte.FlyteAdminClientRunner.STYX_EXECUTION_ID_ANNOTATION;
import static com.spotify.styx.flyte.FlyteAdminClientRunner.STYX_WORKFLOW_INSTANCE_ANNOTATION;
import static com.spotify.styx.flyte.FlyteAdminClientRunner.TERMINATE_CAUSE;
import static com.spotify.styx.flyte.FlyteAdminClientRunner.TERMINATION_GRACE_PERIOD;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.Timestamp;
import com.spotify.styx.QuietDeterministicScheduler;
import com.spotify.styx.flyte.client.FlyteAdminClient;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.util.Time;
import flyteidl.admin.Common;
import flyteidl.admin.ExecutionOuterClass;
import flyteidl.admin.ExecutionOuterClass.Execution;
import flyteidl.admin.ProjectOuterClass;
import flyteidl.core.Execution.WorkflowExecution.Phase;
import flyteidl.core.IdentifierOuterClass;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import junitparams.JUnitParamsRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.verification.VerificationMode;

@RunWith(JUnitParamsRunner.class)
@Ignore()
public class FlyteAdminClientRunnerTerminateDanglingTest {

  private static final Duration TERMINATE_DANGLING_INTERVAL = Duration.ofSeconds(60);
  private static final String PROJECT = "test-project";
  private static final String DOMAIN = "production";

  private static final String RUNNING_1 = "running-1";
  private static final String RUNNING_2 = "running-2";
  private static final String NON_STYX_1 = "non-styx-1";
  private static final String NON_STYX_2 = "non-styx-2";
  private static final String NA_DANGLING_1 = "na-dangling-1";
  private static final String NA_DANGLING_2 = "na-dangling-2";
  private static final String NA_DANGLING_YOUNG_1 = "na-dangling-young-1";
  private static final String NA_DANGLING_YOUNG_2 = "na-dangling-young-2";
  private static final String NIA_DANGLING_1 = "nia-dangling-1";
  private static final String NIA_DANGLING_2 = "nia-dangling-2";
  private static final String NIS_DANGLING_1 = "nis-dangling-1";
  private static final String NIS_DANGLING_2 = "nis-dangling-2";
  private static final String OA_DANGLING_1 = "oa-dangling-1";
  private static final String OA_DANGLING_2 = "oa-dangling-2";
  private static final Duration BACK_ENOUGH_TO_MAKE_EXECUTIONS_YOUNG = TERMINATION_GRACE_PERIOD.dividedBy(-2);
  private static final Duration BACK_ENOUGH_TO_MAKE_EXECUTIONS_OLD = TERMINATION_GRACE_PERIOD.multipliedBy(-2);

  @Mock private StateManager stateManager;
  @Mock private FlyteAdminClient adminClient;

  private final QuietDeterministicScheduler executor = spy(new QuietDeterministicScheduler());
  private final Set<WorkflowInstance> activeInstances = new LinkedHashSet<>();
  private final SettableTime time = new SettableTime();

  private FlyteAdminClientRunner runner;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    runner = new FlyteAdminClientRunner(
        "runnerId", adminClient, stateManager, TERMINATE_DANGLING_INTERVAL, executor, time);
    when(adminClient.listProjects())
        .thenReturn(ProjectOuterClass.Projects.newBuilder()
            .addProjects(ProjectOuterClass.Project.newBuilder()
                .setId(PROJECT)
                .addDomains(ProjectOuterClass.Domain.newBuilder()
                    .setId(DOMAIN)
                    .build())
                .build())
            .build());

    time.setOffset(BACK_ENOUGH_TO_MAKE_EXECUTIONS_OLD);
    var nonActiveDangling = executions(this::nonActiveDanglingExecution, NA_DANGLING_1, NA_DANGLING_2);
    var noIdInStateDangling = executions(this::noIdInStateDanglingExecution, NIS_DANGLING_1, NIS_DANGLING_2);
    var noIdInAnnotationDangling = executions(this::noIdInAnnotationDanglingExecution, NIA_DANGLING_1, NIA_DANGLING_2);
    var oldIdInAnnotationDangling = executions(this::oldIdInAnnotationDanglingExecution, OA_DANGLING_1, OA_DANGLING_2);
    var running = executions(this::runningExecution, RUNNING_1, RUNNING_2);
    var nonStyx = executions(this::runningNonStyxExecution, NON_STYX_1, NON_STYX_2);

    time.setOffset(BACK_ENOUGH_TO_MAKE_EXECUTIONS_YOUNG);
    var nonActiveYoungDangling = executions(this::nonActiveDanglingExecution, NA_DANGLING_YOUNG_1, NA_DANGLING_YOUNG_2);
    stubListExecutions(nonActiveYoungDangling, nonActiveDangling, noIdInStateDangling, noIdInAnnotationDangling,
        oldIdInAnnotationDangling, running, nonStyx);

    time.reset();
  }

  @After
  public void tearDown() throws Exception {
    runner.close();
  }

  @Test
  public void shouldCallListExecutionsWithFilter() {
    runner.terminateDanglingFlyteExecutions();

    ArgumentCaptor<String> filterCatcher = ArgumentCaptor.forClass(String.class);
    verify(adminClient).listExecutions(any(), any(), anyInt(), any(), filterCatcher.capture());
    var filters = filterCatcher.getValue().split("\\+");
    assertThat(filters, arrayContainingInAnyOrder(
        equalTo("value_in(phase,RUNNING)"),
        startsWith("gte(execution_created_at,"),
        startsWith("lte(execution_created_at,"))
    );
  }

  @Test
  public void shouldNotTerminateDanglingFlyteExecutionsWhenStateNotActiveAnymoreAndExecutionIsYoung() {
    runner.terminateDanglingFlyteExecutions();

    verifyTerminateExecution(never(), NA_DANGLING_YOUNG_1);
    verifyTerminateExecution(never(), NA_DANGLING_YOUNG_2);
  }

  @Test
  public void shouldTerminateDanglingFlyteExecutionsWhenStateNotActiveAnymore() {
    runner.terminateDanglingFlyteExecutions();

    verifyTerminateExecution(times(1), NA_DANGLING_1);
    verifyTerminateExecution(times(1), NA_DANGLING_2);
  }

  @Test
  public void shouldTerminateDanglingFlyteExecutionsWhenStateHasNoStyxRunId() {
    runner.terminateDanglingFlyteExecutions();

    verifyTerminateExecution(times(1), NIS_DANGLING_1);
    verifyTerminateExecution(times(1), NIS_DANGLING_2);
  }

  @Test
  public void shouldTerminateDanglingFlyteExecutionsWhenNoIdInAnnotation() {
    runner.terminateDanglingFlyteExecutions();

    verifyTerminateExecution(times(1), NIA_DANGLING_1);
    verifyTerminateExecution(times(1), NIA_DANGLING_2);
  }

  @Test
  public void shouldTerminateDanglingFlyteExecutionsWhenOldIdInAnnotation() {
    runner.terminateDanglingFlyteExecutions();

    verifyTerminateExecution(times(1), OA_DANGLING_1);
    verifyTerminateExecution(times(1), OA_DANGLING_2);
  }

  @Test
  public void shouldNotTerminateActiveStyxRunningFlyteExecutions() {
    runner.terminateDanglingFlyteExecutions();

    verifyTerminateExecution(never(), RUNNING_1);
    verifyTerminateExecution(never(), RUNNING_2);
  }

  @Test
  public void shouldNotTerminateNonStyxFlyteExecutions() {
    runner.terminateDanglingFlyteExecutions();

    verifyTerminateExecution(never(), NON_STYX_1);
    verifyTerminateExecution(never(), NON_STYX_2);
  }

  @Test
  public void shouldScheduleTerminationOnInit() {
    runner.init();
    executor.tick(TERMINATE_DANGLING_INTERVAL.getSeconds() * 2, TimeUnit.SECONDS);

    verify(adminClient, atLeastOnce()).listProjects();
  }

  @Test
  public void shouldCloseExecutorOnClose() throws IOException {
    runner.close();

    verify(executor).shutdown();
  }

  private void verifyTerminateExecution(VerificationMode mode, String name) {
    verify(adminClient, mode).terminateExecution(PROJECT, DOMAIN, name, TERMINATE_CAUSE);
  }

  public List<Execution> executions(Function<String, Execution> execFactory, String... execNames) {
    return Arrays.stream(execNames).map(execFactory).collect(Collectors.toList());
  }

  @SafeVarargs
  private void stubListExecutions(List<Execution>... executions) {
    when(adminClient.listExecutions(any(), any(), anyInt(), any(), any()))
        .thenReturn(ExecutionOuterClass.ExecutionList.newBuilder()
            .addAllExecutions(Arrays.stream(executions).flatMap(Collection::stream).collect(Collectors.toList()))
            .build());
    when(stateManager.listActiveInstances()).thenReturn(activeInstances);
  }

  private Execution noIdInStateDanglingExecution(String name) {
    final var workflowInstance = workflowInstance(name);
    addToActiveStates(workflowInstance, StateData.newBuilder().build());
    return execution(name, styxAnnotations(name, workflowInstance));
  }

  private Execution oldIdInAnnotationDanglingExecution(String name) {
    final var workflowInstance = workflowInstance(name);
    addToActiveStates(workflowInstance, StateData.newBuilder().executionId(name).build());
    return execution(name, oldRunIdStyxAnnotation(name, workflowInstance));
  }

  private Execution noIdInAnnotationDanglingExecution(String name) {
    final var workflowInstance = workflowInstance(name);
    addToActiveStates(workflowInstance, StateData.newBuilder().executionId(name).build());
    return execution(name, noRunIdStyxAnnotation(workflowInstance));
  }

  private Execution runningExecution(String name) {
    final var workflowInstance = workflowInstance(name);
    addToActiveStates(workflowInstance, StateData.newBuilder().executionId(name).build());
    return execution(name, styxAnnotations(name, workflowInstance));
  }

  private void addToActiveStates(WorkflowInstance workflowInstance, StateData state) {
    activeInstances.add(workflowInstance);
    when(stateManager.getActiveState(workflowInstance))
        .thenReturn(Optional.of(
            RunState.create(
                workflowInstance,
                RunState.State.RUNNING,
                state)));
  }

  private Execution nonActiveDanglingExecution(String name) {
    return execution(name, styxAnnotations(name, workflowInstance(name)));
  }

  private Execution runningNonStyxExecution(String name) {
    return execution(name, emptyAnnotations());
  }

  private Execution execution(String name, Common.Annotations annotations) {
    var timestamp = nowTimestamp();
    var identifier = IdentifierOuterClass.WorkflowExecutionIdentifier.newBuilder()
        .setProject(PROJECT)
        .setDomain(DOMAIN)
        .setName(name)
        .build();
    return Execution.newBuilder()
        .setId(identifier)
        .setSpec(ExecutionOuterClass.ExecutionSpec.newBuilder()
            .setAnnotations(annotations)
            .build())
        .setClosure(ExecutionOuterClass.ExecutionClosure.newBuilder()
            .setPhase(Phase.RUNNING)
            .setCreatedAt(timestamp)
            .setStartedAt(timestamp)
            .build())
        .build();
  }

  private Timestamp nowTimestamp() {
    var now = time.get();
    return Timestamp.newBuilder()
        .setSeconds(now.getEpochSecond())
        .setNanos(now.getNano())
        .build();
  }

  private Common.Annotations styxAnnotations(String name, WorkflowInstance workflowInstance) {
    return Common.Annotations.newBuilder()
        .putValues(STYX_WORKFLOW_INSTANCE_ANNOTATION, workflowInstance.toKey())
        .putValues(STYX_EXECUTION_ID_ANNOTATION, name)
        .build();
  }

  private Common.Annotations noRunIdStyxAnnotation(WorkflowInstance workflowInstance) {
    return Common.Annotations.newBuilder()
        .putValues(STYX_WORKFLOW_INSTANCE_ANNOTATION, workflowInstance.toKey())
        .build();
  }

  private Common.Annotations oldRunIdStyxAnnotation(String name, WorkflowInstance workflowInstance) {
    return Common.Annotations.newBuilder()
        .putValues(STYX_WORKFLOW_INSTANCE_ANNOTATION, workflowInstance.toKey())
        .putValues(STYX_EXECUTION_ID_ANNOTATION, "old" + name)
        .build();
  }

  private Common.Annotations emptyAnnotations() {
    return Common.Annotations.getDefaultInstance();
  }

  private WorkflowInstance workflowInstance(String name) {
    return WorkflowInstance.create(
        WorkflowId.create("component", name),
        "2020-10-24"
    );
  }

  private static class SettableTime implements Time {

    private final Instant base;

    private Duration offset = Duration.ZERO;

    private SettableTime() {
      this.base = Instant.now();
    }

    @Override
    public Instant get() {
      return base.plus(offset);
    }

    private void setOffset(Duration offset) {
      this.offset = offset;
    }

    private void reset() {
      this.offset = Duration.ZERO;
    }
  }
}
