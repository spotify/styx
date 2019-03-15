/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.util;

import com.spotify.styx.api.RunStateDataPayload.RunStateData;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.SortedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReplayEvents {

  private static final Logger LOG = LoggerFactory.getLogger(ReplayEvents.class);

  private ReplayEvents() {
    throw new UnsupportedOperationException();
  }

  public static Optional<RunStateData> getBackfillRunStateData(
      WorkflowInstance workflowInstance,
      Storage storage,
      String backfillId) {

    final SortedSet<SequenceEvent> sequenceEvents = getSequenceEvents(workflowInstance, storage);
    if (sequenceEvents.isEmpty()) {
      return Optional.empty();
    }

    final SettableTime time = new SettableTime();
    RunState restoredState = RunState.fresh(workflowInstance, time);

    boolean backfillFound = false;

    final SettableTime initialTime = new SettableTime();
    final SettableTime latestTime = new SettableTime();

    // events are written after the datastore transition transaction is successfully
    // committed, so we can trust the sequence of events faithfully reflect the state
    // transition if they have all been successfully written to bigtable
    for (SequenceEvent sequenceEvent : sequenceEvents) {
      time.set(Instant.ofEpochMilli(sequenceEvent.timestamp()));

      final boolean triggerExecutionEventMet =
          "triggerExecution".equals(EventUtil.name(sequenceEvent.event()));

      if (triggerExecutionEventMet) {
        if (backfillFound) {
          break;
        }

        restoredState = RunState.fresh(workflowInstance, time);
        initialTime.set(time.get());
      }

      try {
        restoredState = restoredState.transition(sequenceEvent.event(), time);
      } catch (IllegalStateException e) {
        LOG.warn("failed to transition state, move on to next event", e);
      }

      latestTime.set(time.get());

      if (backfillFound(triggerExecutionEventMet, backfillId, restoredState)) {
        backfillFound = true;
      }
    }

    if (backfillFound) {
      final RunStateData runStateData = RunStateData.newBuilder()
          .workflowInstance(restoredState.workflowInstance())
          .state(restoredState.state().name())
          .stateData(restoredState.data())
          .initialTimestamp(initialTime.get().toEpochMilli())
          .latestTimestamp(latestTime.get().toEpochMilli())
          .build();
      return Optional.of(runStateData);
    } else {
      return Optional.empty();
    }
  }

  private static SortedSet<SequenceEvent> getSequenceEvents(
      final WorkflowInstance workflowInstance, final Storage storage) {
    final SortedSet<SequenceEvent> sequenceEvents;
    try {
      sequenceEvents = storage.readEvents(workflowInstance);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return sequenceEvents;
  }

  private static boolean backfillFound(boolean triggerExecutionEventMet,
                                       String backfillId, RunState restoredState) {
    return triggerExecutionEventMet && restoredState.data().trigger()
        .map(trigger -> backfillId.equals(TriggerUtil.triggerId(trigger)))
        .orElse(false);
  }

  private static final class SettableTime implements Time {

    private Instant now = Instant.now();

    @Override
    public Instant get() {
      return now;
    }

    void set(Instant time) {
      now = time;
    }
  }
}
