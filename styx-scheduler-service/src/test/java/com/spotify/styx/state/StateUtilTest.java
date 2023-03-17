/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

package com.spotify.styx.state;

import static com.spotify.styx.state.StateUtil.getActiveInstanceStates;
import static com.spotify.styx.state.StateUtil.getTimedOutInstances;
import static com.spotify.styx.storage.Storage.GLOBAL_RESOURCE_ID;
import static com.spotify.styx.testdata.TestData.RESOURCE_IDS;
import static com.spotify.styx.testdata.TestData.WORKFLOW_ID;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static com.spotify.styx.testdata.TestData.WORKFLOW_WITH_RESOURCES;
import static com.spotify.styx.testdata.TestData.getWorkflow;
import static com.spotify.styx.testdata.TestData.getWorkflowInstance;
import static com.spotify.styx.testdata.TestData.getWorkflowNoRunningTimeout;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import com.spotify.styx.WorkflowResourceDecorator;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class StateUtilTest {

  @Mock private Storage storage;
  @Mock private TimeoutConfig timeoutConfig;
  @Mock private Supplier<Map<WorkflowId, Workflow>> workflowCache;

  private final WorkflowResourceDecorator resourceDecorator = WorkflowResourceDecorator.NOOP;
  final private StyxConfig config = StyxConfig.newBuilder()
      .globalConcurrency(Optional.of(100L))
      .globalDockerRunnerId("")
      .globalFlyteRunnerId("")
      .build();

  @Before
  public void setUp() throws IOException {
    final RunState runState =
        RunState.create(WORKFLOW_INSTANCE, RunState.State.RUNNING, Instant.ofEpochMilli(10L));
    when(storage.readActiveStates()).thenReturn(Map.of(WORKFLOW_INSTANCE, runState));
    when(storage.config()).thenReturn(config);
    when(workflowCache.get()).thenReturn(Map.of(WORKFLOW_ID, WORKFLOW_WITH_RESOURCES));
  }

  @Test
  public void shouldGetUsedResources() throws IOException {
    final Map<String, Long> resourcesUsageMap = getResourcesUsageMap();
    final Map<String, Long> expectedResourcesUsageMap =
        RESOURCE_IDS.stream().collect(Collectors.toMap(Function.identity(), item -> 1L));
    expectedResourcesUsageMap.put(GLOBAL_RESOURCE_ID, 1L);

    assertThat(resourcesUsageMap, is(expectedResourcesUsageMap));
  }

  @Test
  public void shouldGetUsedResourcesWithNoRunning() throws IOException {
    final RunState runState =
        RunState.create(WORKFLOW_INSTANCE, RunState.State.QUEUED, Instant.ofEpochMilli(10L));

    when(storage.readActiveStates()).thenReturn(Map.of(WORKFLOW_INSTANCE, runState));
    assertThat(getResourcesUsageMap(), is(Map.of()));
  }

  @Test
  public void shouldGetUsedResourcesNoStates() throws IOException {
    when(storage.readActiveStates()).thenReturn(emptyMap());
    assertThat(getResourcesUsageMap(), is(Map.of()));
  }

  @Test(expected = IOException.class)
  public void shouldGetUsedResourcesStorageException() throws IOException {
    when(storage.readActiveStates()).thenThrow(new IOException());
    getResourcesUsageMap();
  }

  @Test
  public void shouldConsumeResource() {
    assertTrue(StateUtil.isConsumingResources(RunState.State.PREPARE));
    assertTrue(StateUtil.isConsumingResources(RunState.State.SUBMITTING));
    assertTrue(StateUtil.isConsumingResources(RunState.State.SUBMITTED));
    assertTrue(StateUtil.isConsumingResources(RunState.State.RUNNING));
  }

  @Test
  public void shouldNotConsumeResource() {
    assertFalse(StateUtil.isConsumingResources(RunState.State.NEW));
    assertFalse(StateUtil.isConsumingResources(RunState.State.QUEUED));
    assertFalse(StateUtil.isConsumingResources(RunState.State.TERMINATED));
    assertFalse(StateUtil.isConsumingResources(RunState.State.FAILED));
    assertFalse(StateUtil.isConsumingResources(RunState.State.ERROR));
    assertFalse(StateUtil.isConsumingResources(RunState.State.DONE));
  }

  @Test
  public void shouldGetTimedOutRunningInstances() throws IOException {
    final RunState runState =
        RunState.create(WORKFLOW_INSTANCE, RunState.State.RUNNING, Instant.ofEpochMilli(10L));
    final Duration timeout = Duration.ofMillis(3L);
    when(timeoutConfig.ttlOf(runState.state())).thenReturn(timeout);
    when(timeoutConfig.getMaxRunningTimeout()).thenReturn(timeout);
    when(storage.readActiveStates()).thenReturn(Map.of(WORKFLOW_INSTANCE, runState));
    when(workflowCache.get()).thenReturn(Map.of(WORKFLOW_ID, getWorkflow(WORKFLOW_ID, Duration.ofMillis(2L))));

    final Map<WorkflowInstance, RunState> activeStates = storage.readActiveStates();
    final List<InstanceState> activeInstanceStates = getActiveInstanceStates(activeStates);
    final Set<WorkflowInstance> timedOutInstances =
        getTimedOutInstances(workflowCache.get(), activeInstanceStates, Instant.ofEpochMilli(12L), timeoutConfig);
    assertThat(timedOutInstances, contains(WORKFLOW_INSTANCE));
  }

  @Test
  public void hasNotTimedOut() {
    Instant timeEnteredRunningPhase = Instant.ofEpochMilli(10L);
    Duration workflowRunningTimeout = Duration.ofMillis(7L);
    Instant currentTime = Instant.ofEpochMilli(16L);
    Duration defaultRunStateTimeout = Duration.ofMillis(20L);
    Duration maxRunningTimeout = Duration.ofMillis(23L);
    Workflow workflow = getWorkflow(WORKFLOW_ID, workflowRunningTimeout);

    final RunState runState =
            RunState.create(getWorkflowInstance(WORKFLOW_ID), RunState.State.RUNNING, timeEnteredRunningPhase);

    boolean hasTimedOut = StateUtil.hasTimedOut(Optional.of(workflow), runState,
            currentTime,
            defaultRunStateTimeout,
            maxRunningTimeout);

    assertFalse(hasTimedOut);
  }

  @Test
  public void hasTimedOutDueToDefaultRunningTimeout() {
    Instant timeEnteredRunningPhase = Instant.ofEpochMilli(10L);
    Instant currentTime = Instant.ofEpochMilli(18L);

    Duration defaultRunningTimeout = Duration.ofMillis(5L);
    Duration anythingLargerThanWorkflowRunningTimeout = Duration.ofMillis(100000L);

    Workflow workflow = getWorkflowNoRunningTimeout(WORKFLOW_ID);

    final RunState runState =
            RunState.create(getWorkflowInstance(WORKFLOW_ID), RunState.State.RUNNING, timeEnteredRunningPhase);

    boolean hasTimedOut = StateUtil.hasTimedOut(Optional.of(workflow), runState,
            currentTime,
            defaultRunningTimeout,
            anythingLargerThanWorkflowRunningTimeout);

    assertTrue(hasTimedOut);
  }

  @Test
  public void hasTimedOutMaxRunningTimeoutIsSmallerThanWorkflowTimeout() {
    Instant timeEnteredRunningPhase = Instant.ofEpochMilli(10L);
    Duration workflowRunningTimeout = Duration.ofMillis(7L);
    Instant currentTime = Instant.ofEpochMilli(18L);

    Duration anything = Duration.ofMillis(10000L);
    Duration maxRunningTimeout = Duration.ofMillis(5L);

    Workflow workflow = getWorkflow(WORKFLOW_ID, workflowRunningTimeout);

    final RunState runState =
            RunState.create(getWorkflowInstance(WORKFLOW_ID), RunState.State.RUNNING, timeEnteredRunningPhase);

    boolean hasTimedOut = StateUtil.hasTimedOut(Optional.of(workflow), runState,
            currentTime,
            anything,
            maxRunningTimeout);

    assertTrue(hasTimedOut);
  }

  @Test
  public void hasTimedOut() {
    Instant timeEnteredRunningPhase = Instant.ofEpochMilli(10L);
    Duration workflowRunningTimeout = Duration.ofMillis(7L);
    Instant currentTime = Instant.ofEpochMilli(17L);

    Duration anythingLargerThanWorkflowRunningTimeout = Duration.ofMillis(10000L);
    Duration anything = anythingLargerThanWorkflowRunningTimeout;
    Workflow workflow = getWorkflow(WORKFLOW_ID, workflowRunningTimeout);

    final RunState runState =
            RunState.create(getWorkflowInstance(WORKFLOW_ID), RunState.State.RUNNING, timeEnteredRunningPhase);

    boolean hasTimedOut = StateUtil.hasTimedOut(Optional.of(workflow), runState,
            currentTime,
            anything,
            anythingLargerThanWorkflowRunningTimeout);

    assertTrue(hasTimedOut);
  }

  @Test
  public void shouldGetTimedOutRunningInstancesBaseOnGlobalTimeoutIfWorkflowNotFound() throws IOException {
    final RunState runState =
        RunState.create(WORKFLOW_INSTANCE, RunState.State.RUNNING, Instant.ofEpochMilli(10L));
    final Duration timeout = Duration.ofMillis(3L);
    when(timeoutConfig.ttlOf(runState.state())).thenReturn(timeout);
    when(timeoutConfig.getMaxRunningTimeout()).thenReturn(timeout);
    when(storage.readActiveStates()).thenReturn(Map.of(WORKFLOW_INSTANCE, runState));

    final Map<WorkflowInstance, RunState> activeStates = storage.readActiveStates();
    final List<InstanceState> activeInstanceStates = getActiveInstanceStates(activeStates);
    final Set<WorkflowInstance> timedOutInstances =
        getTimedOutInstances(Map.of(), activeInstanceStates, Instant.ofEpochMilli(13L), timeoutConfig);
    assertThat(timedOutInstances, contains(WORKFLOW_INSTANCE));
  }

  @Test
  public void shouldGetTimedOutRunningInstancesForInvalidCustomTimeout() throws IOException {
    final RunState runState =
            RunState.create(WORKFLOW_INSTANCE, RunState.State.RUNNING, Instant.ofEpochMilli(10L));
    final Duration timeout = Duration.ofMillis(1L);
    when(timeoutConfig.ttlOf(runState.state())).thenReturn(timeout);
    when(timeoutConfig.getMaxRunningTimeout()).thenReturn(timeout);
    when(storage.readActiveStates()).thenReturn(Map.of(WORKFLOW_INSTANCE, runState));
    when(workflowCache.get()).thenReturn(Map.of(WORKFLOW_ID, getWorkflow(WORKFLOW_ID, Duration.ofMillis(2L))));

    final Map<WorkflowInstance, RunState> activeStates = storage.readActiveStates();
    final List<InstanceState> activeInstanceStates = getActiveInstanceStates(activeStates);
    final Set<WorkflowInstance> timedOutInstances =
            getTimedOutInstances(workflowCache.get(), activeInstanceStates, Instant.ofEpochMilli(11L), timeoutConfig);
    assertThat(timedOutInstances, contains(WORKFLOW_INSTANCE));
  }

  @Test
  public void shouldGetTimedOutQueuingInstances() throws IOException {
    final RunState runState =
        RunState.create(WORKFLOW_INSTANCE, RunState.State.QUEUED, Instant.ofEpochMilli(10L));
    final Duration timeout = Duration.ofMillis(1L);
    when(timeoutConfig.ttlOf(runState.state())).thenReturn(timeout);
    when(timeoutConfig.getMaxRunningTimeout()).thenReturn(timeout);
    when(storage.readActiveStates()).thenReturn(Map.of(WORKFLOW_INSTANCE, runState));
    when(workflowCache.get()).thenReturn(Map.of(WORKFLOW_ID, getWorkflow(WORKFLOW_ID, Duration.ofMillis(2L))));

    final Map<WorkflowInstance, RunState> activeStates = storage.readActiveStates();
    final List<InstanceState> activeInstanceStates = getActiveInstanceStates(activeStates);
    final Set<WorkflowInstance> timedOutInstances =
        getTimedOutInstances(workflowCache.get(), activeInstanceStates, Instant.ofEpochMilli(11L), timeoutConfig);
    assertThat(timedOutInstances, contains(WORKFLOW_INSTANCE));
  }

  @Test
  public void shouldNotGetTimedOutRunningInstances() throws IOException {
    final RunState runState =
        RunState.create(WORKFLOW_INSTANCE, RunState.State.RUNNING, Instant.ofEpochMilli(10L));
    final Duration timeout = Duration.ofMillis(2L);
    when(timeoutConfig.ttlOf(runState.state())).thenReturn(timeout);
    when(timeoutConfig.getMaxRunningTimeout()).thenReturn(timeout);
    when(storage.readActiveStates()).thenReturn(Map.of(WORKFLOW_INSTANCE, runState));
    when(workflowCache.get()).thenReturn(Map.of(WORKFLOW_ID, WORKFLOW_WITH_RESOURCES));

    final Map<WorkflowInstance, RunState> activeStates = storage.readActiveStates();
    final List<InstanceState> activeInstanceStates = getActiveInstanceStates(activeStates);
    final Set<WorkflowInstance> timedOutInstances =
        getTimedOutInstances(workflowCache.get(), activeInstanceStates, Instant.ofEpochMilli(11L), timeoutConfig);
    assertThat(timedOutInstances.isEmpty(), is(true));
  }

  private Map<String, Long> getResourcesUsageMap() throws IOException {
    final Map<WorkflowInstance, RunState> activeStates = storage.readActiveStates();
    final List<InstanceState> activeInstanceStates = getActiveInstanceStates(activeStates);
    boolean globalConcurrencyEnabled = storage.config().globalConcurrency().isPresent();

    return StateUtil.getResourceUsage(globalConcurrencyEnabled,
        activeInstanceStates, ImmutableSet.of(), resourceDecorator, workflowCache.get());
  }
}
