/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

package com.spotify.styx.util;

import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresent;
import static com.google.common.collect.Sets.newTreeSet;
import static com.spotify.styx.testdata.TestData.EXECUTION_DESCRIPTION;
import static com.spotify.styx.testdata.TestData.RESOURCE_IDS;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.spotify.styx.api.RunStateDataPayload.RunStateData;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.util.Optional;
import java.util.SortedSet;
import junitparams.JUnitParamsRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class ReplayEventsTest {

  private static final TriggerParameters TRIGGER_PARAMETERS = TriggerParameters.builder()
      .env("FOO", "foo",
          "BAR", "bar")
      .build();

  private Storage storage;

  @Before
  public void setUp() {
    storage = mock(Storage.class);
  }

  @Test
  public void shouldNotBeConstructable() throws ReflectiveOperationException {
    assertThat(ClassEnforcer.assertNotInstantiable(ReplayEvents.class), is(true));
  }

  @Test
  public void restoreRunStateForInactiveBackfill() throws Exception {
    SortedSet<SequenceEvent> events = newTreeSet(SequenceEvent.COUNTER_COMPARATOR);
    events.add(SequenceEvent.create(
        Event.triggerExecution(WORKFLOW_INSTANCE, Trigger.backfill("bf-1"), TRIGGER_PARAMETERS),      1L, 1L));
    events.add(SequenceEvent.create(Event.dequeue(WORKFLOW_INSTANCE, RESOURCE_IDS),                   2L, 2L));
    events.add(SequenceEvent.create(Event.submit(WORKFLOW_INSTANCE, EXECUTION_DESCRIPTION, "exec-1"), 3L, 3L));
    events.add(SequenceEvent.create(Event.submitted(WORKFLOW_INSTANCE, "exec-1", "test"),             4L, 4L));
    events.add(SequenceEvent.create(Event.started(WORKFLOW_INSTANCE),                                 5L, 5L));
    events.add(SequenceEvent.create(Event.terminate(WORKFLOW_INSTANCE, Optional.of(0)),               6L, 6L));
    events.add(SequenceEvent.create(Event.success(WORKFLOW_INSTANCE),                                 7L, 7L));
    events.add(SequenceEvent.create(
        Event.triggerExecution(WORKFLOW_INSTANCE, Trigger.adhoc("ad-hoc"), TRIGGER_PARAMETERS),       8L, 8L));
    events.add(SequenceEvent.create(Event.dequeue(WORKFLOW_INSTANCE, RESOURCE_IDS),                   9L, 9L));
    events.add(SequenceEvent.create(Event.halt(WORKFLOW_INSTANCE),                                    10L, 10L));

    when(storage.readEvents(WORKFLOW_INSTANCE)).thenReturn(events);

    RunStateData restoredRunStateData =
        ReplayEvents.getBackfillRunStateData(WORKFLOW_INSTANCE, storage, "bf-1").orElseThrow();

    assertThat(restoredRunStateData.state(), is("DONE"));
    assertThat(restoredRunStateData.stateData().lastExit(), isPresent());
    assertThat(restoredRunStateData.stateData().lastExit().orElseThrow(), is(0));

    assertThat(restoredRunStateData.initialTimestamp().orElseThrow(), is(1L));
    assertThat(restoredRunStateData.latestTimestamp().orElseThrow(), is(7L));
  }

  @Test
  public void restoreRunStateWhenMissingEvent() throws Exception {
    SortedSet<SequenceEvent> events = newTreeSet(SequenceEvent.COUNTER_COMPARATOR);
    events.add(SequenceEvent.create(
        Event.triggerExecution(WORKFLOW_INSTANCE, Trigger.backfill("bf-1"), TRIGGER_PARAMETERS),      1L, 1L));
    events.add(SequenceEvent.create(Event.dequeue(WORKFLOW_INSTANCE, RESOURCE_IDS),                   2L, 2L));
    // missing Event.submit(WORKFLOW_INSTANCE, EXECUTION_DESCRIPTION, "exec-1")
    events.add(SequenceEvent.create(Event.submitted(WORKFLOW_INSTANCE, "exec-1", "test"),             4L, 4L));
    events.add(SequenceEvent.create(Event.started(WORKFLOW_INSTANCE),                                 5L, 5L));
    events.add(SequenceEvent.create(Event.terminate(WORKFLOW_INSTANCE, Optional.of(0)),               6L, 6L));
    events.add(SequenceEvent.create(Event.success(WORKFLOW_INSTANCE),                                 7L, 7L));
    events.add(SequenceEvent.create(
        Event.triggerExecution(WORKFLOW_INSTANCE, Trigger.adhoc("ad-hoc"), TRIGGER_PARAMETERS),       8L, 8L));
    events.add(SequenceEvent.create(Event.dequeue(WORKFLOW_INSTANCE, RESOURCE_IDS),                   9L, 9L));
    events.add(SequenceEvent.create(Event.halt(WORKFLOW_INSTANCE),                                    10L, 10L));

    when(storage.readEvents(WORKFLOW_INSTANCE)).thenReturn(events);

    RunStateData restoredRunStateData =
        ReplayEvents.getBackfillRunStateData(WORKFLOW_INSTANCE, storage, "bf-1").orElseThrow();

    assertThat(restoredRunStateData.state(), is("DONE"));
    assertThat(restoredRunStateData.stateData().lastExit(), isPresent());
    assertThat(restoredRunStateData.stateData().lastExit().orElseThrow(), is(0));

    assertThat(restoredRunStateData.initialTimestamp().orElseThrow(), is(1L));
    assertThat(restoredRunStateData.latestTimestamp().orElseThrow(), is(7L));
  }

  @Test
  public void returnsEmptyWithMissingBackfill() throws Exception {
    SortedSet<SequenceEvent> events = newTreeSet(SequenceEvent.COUNTER_COMPARATOR);
    events.add(SequenceEvent.create(
        Event.triggerExecution(WORKFLOW_INSTANCE, Trigger.backfill("bf-1"), TRIGGER_PARAMETERS), 1L, 1L));
    events.add(SequenceEvent.create(Event.dequeue(WORKFLOW_INSTANCE, RESOURCE_IDS),              2L, 2L));

    when(storage.readEvents(WORKFLOW_INSTANCE)).thenReturn(events);

    Optional<RunStateData> restoredRunStateData = ReplayEvents.getBackfillRunStateData(
        WORKFLOW_INSTANCE,
        storage, "erroneous-id");

    assertThat(restoredRunStateData, is(Optional.empty()));
  }

  @Test
  public void returnsEmptyIfNoEvent() throws Exception {
    SortedSet<SequenceEvent> events = newTreeSet(SequenceEvent.COUNTER_COMPARATOR);
    when(storage.readEvents(WORKFLOW_INSTANCE)).thenReturn(events);

    Optional<RunStateData> restoredRunStateData = ReplayEvents.getBackfillRunStateData(
        WORKFLOW_INSTANCE,
        storage, "bf-1");

    assertThat(restoredRunStateData, is(Optional.empty()));
  }

  @Test
  public void throwExceptionWhenFailedToReadEvents() throws Exception {
    IOException exception = new IOException();
    when(storage.readEvents(WORKFLOW_INSTANCE)).thenThrow(exception);

    try {
      ReplayEvents.getBackfillRunStateData(WORKFLOW_INSTANCE, storage, "bf-1");
      fail();
    } catch (RuntimeException e) {
      assertThat(e.getCause(), is(exception));
    }
  }
}
