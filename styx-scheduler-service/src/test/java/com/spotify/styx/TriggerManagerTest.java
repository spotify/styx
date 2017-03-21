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

import static com.spotify.styx.testdata.TestData.FULL_DATA_SCHEDULE;
import static java.time.temporal.ChronoUnit.DAYS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.AlreadyInitializedException;
import com.spotify.styx.util.FutureUtil;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class TriggerManagerTest {

  private static final Trigger NATURAL_TRIGGER = Trigger.natural();

  private static final Instant NEXT_EXECUTION = Instant.parse("2016-10-01T00:00:00Z");
  private static final Instant NEXT_EXECUTION_PLUS_DAY = Instant.parse("2016-10-02T00:00:00Z");
  private static final Instant NEXT_EXECUTION_MINUS_DAY = Instant.parse("2016-09-30T00:00:00Z");

  private static final Instant MANAGER_TIME_TRUNCATED = Instant.parse("2016-10-10T00:00:00Z");
  private static final Instant MANAGER_TIME_MINUS_DAY_TRUNCATED = Instant.parse("2016-10-09T00:00:00Z");
  private static final Instant MANAGER_TIME_PLUS_DAY_TRUNCATED = Instant.parse("2016-10-11T00:00:00Z");

  private static Workflow WORKFLOW_DAILY =
      Workflow.create("comp", URI.create("http:/foo"), FULL_DATA_SCHEDULE);

  @Mock
  Storage storage;
  @Mock
  TriggerListener triggerListener;

  private TriggerManager triggerManager;
  private final Time MANAGER_TIME = () -> Instant.parse("2016-10-10T13:11:11Z");

  private final ExecutorService executor = Executors.newCachedThreadPool();

  @After
  public void tearDown() throws Exception {
    executor.shutdownNow();
  }

  @Before
  public void setUp() throws IOException {
    triggerManager = new TriggerManager(triggerListener, MANAGER_TIME, storage, Stats.NOOP);
    when(triggerListener.event(any(Workflow.class), any(Trigger.class), any(Instant.class)))
        .thenReturn(CompletableFuture.completedFuture(null));
  }

  @Test
  public void shouldNotUpdateNextNaturalTriggerUntilTriggerExecutionIsComplete() throws Exception {
    setupWithNextNaturalTrigger(true, NEXT_EXECUTION);
    final CompletableFuture<Void> triggerExecutionFuture = new CompletableFuture<>();
    when(triggerListener.event(any(Workflow.class), any(Trigger.class), any(Instant.class)))
        .thenReturn(triggerExecutionFuture);
    executor.execute(triggerManager::tick);
    verify(triggerListener, timeout(60_000)).event(
        WORKFLOW_DAILY, NATURAL_TRIGGER, NEXT_EXECUTION_MINUS_DAY);
    // HACK: Sleep to avoid racily missing an undesired invocation of updateNextNaturalTrigger
    Thread.sleep(5000);
    verify(storage, never()).updateNextNaturalTrigger(any(WorkflowId.class), any(Instant.class));
    triggerExecutionFuture.complete(null);
    verify(storage, timeout(60_000)).updateNextNaturalTrigger(
        WORKFLOW_DAILY.id(), NEXT_EXECUTION_PLUS_DAY);
  }

  @Test
  public void shouldNotUpdateNextNaturalTriggerIfTriggerExecutionFails() throws Exception {
    setupWithNextNaturalTrigger(true, NEXT_EXECUTION);
    when(triggerListener.event(any(Workflow.class), any(Trigger.class), any(Instant.class)))
        .thenReturn(FutureUtil.exceptionallyCompletedFuture(
            new RuntimeException("trigger execution failure!")));
    triggerManager.tick();
    verify(triggerListener).event(WORKFLOW_DAILY, NATURAL_TRIGGER, NEXT_EXECUTION_MINUS_DAY);
    verify(storage, never()).updateNextNaturalTrigger(any(WorkflowId.class), any(Instant.class));
  }

  @Test
  public void shouldTriggerExecutionOnEnabledWithNextNaturalTrigger() throws IOException {
    setupWithNextNaturalTrigger(true, NEXT_EXECUTION);
    triggerManager.tick();
    verify(triggerListener).event(WORKFLOW_DAILY, NATURAL_TRIGGER, NEXT_EXECUTION_MINUS_DAY);
    verify(storage).updateNextNaturalTrigger(WORKFLOW_DAILY.id(), NEXT_EXECUTION_PLUS_DAY);
  }

  @Test
  public void shouldTriggerExecutionOnEnabledWithoutNextNaturalTrigger() throws IOException {
    setupWithoutNextNaturalTrigger(true);
    triggerManager.tick();
    verify(triggerListener).event(WORKFLOW_DAILY, NATURAL_TRIGGER, MANAGER_TIME_MINUS_DAY_TRUNCATED);
    verify(storage).updateNextNaturalTrigger(WORKFLOW_DAILY.id(), MANAGER_TIME_PLUS_DAY_TRUNCATED);
  }

  @Test
  public void shouldNotTriggerExecutionOnDisabledWorkflowWithNextNaturalTrigger() throws IOException {
    setupWithNextNaturalTrigger(false, MANAGER_TIME_TRUNCATED);
    triggerManager.tick();
    verify(triggerListener, never()).event(any(), any(), any());
    verify(storage).updateNextNaturalTrigger(WORKFLOW_DAILY.id(), MANAGER_TIME_PLUS_DAY_TRUNCATED);
  }

  @Test
  public void shouldNotTriggerExecutionOnDisabledWorkflowWithoutNextNaturalTrigger() throws IOException {
    setupWithoutNextNaturalTrigger(false);
    triggerManager.tick();
    verify(triggerListener, never()).event(any(), any(), any());
    verify(storage).updateNextNaturalTrigger(WORKFLOW_DAILY.id(), MANAGER_TIME_PLUS_DAY_TRUNCATED);
  }

  @Test
  public void shouldNotTriggerExecutionIfNextNaturalTriggerAfterManagerTime() throws IOException {
    setupWithNextNaturalTrigger(true, MANAGER_TIME_TRUNCATED.plus(1, DAYS));
    triggerManager.tick();
    verify(triggerListener, never()).event(any(), any(), any());
    verify(storage, never()).updateNextNaturalTrigger(any(), any());
  }

  @Test
  public void shouldNotTriggerExecutionOnDisabledGlobally() throws IOException {
    when(storage.globalEnabled()).thenReturn(false);
    triggerManager.tick();
    verify(triggerListener, never()).event(any(), any(), any());
    verify(storage, never()).updateNextNaturalTrigger(any(), any());
  }

  @Test
  public void shouldNotUpdateNextNaturalTriggerIfTriggerListenerThrows() throws Exception {
    setupWithNextNaturalTrigger(true, NEXT_EXECUTION);
    doThrow(new RuntimeException()).when(triggerListener).event(any(), any(), any());
    triggerManager.tick();
    verify(storage, never()).updateNextNaturalTrigger(any(), any());
  }

  @Test
  public void shouldUpdateNextNaturalTriggerIfAlreadyInitialized() throws Exception {
    setupWithNextNaturalTrigger(true, NEXT_EXECUTION);
    doThrow(new AlreadyInitializedException("")).when(triggerListener).event(any(), any(), any());
    triggerManager.tick();
    verify(storage).updateNextNaturalTrigger(WORKFLOW_DAILY.id(), NEXT_EXECUTION_PLUS_DAY);
  }

  private void setupWithNextNaturalTrigger(boolean enabled, Instant nextNaturalTrigger) throws IOException {
    when(storage.globalEnabled()).thenReturn(true);
    if (enabled) {
      when(storage.enabled()).thenReturn(ImmutableSet.of(WORKFLOW_DAILY.id()));
    } else {
      when(storage.enabled()).thenReturn(ImmutableSet.of());
    }
    when(storage.workflowsWithNextNaturalTrigger()).thenReturn(
        ImmutableMap.of(WORKFLOW_DAILY,
                        Optional.of(nextNaturalTrigger)));
  }

  private void setupWithoutNextNaturalTrigger(boolean enabled) throws IOException {
    when(storage.globalEnabled()).thenReturn(true);
    if (enabled) {
      when(storage.enabled()).thenReturn(ImmutableSet.of(WORKFLOW_DAILY.id()));
    } else {
      when(storage.enabled()).thenReturn(ImmutableSet.of());
    }
    when(storage.workflowsWithNextNaturalTrigger()).thenReturn(
        ImmutableMap.of(WORKFLOW_DAILY,
                        Optional.empty()));
  }
}
