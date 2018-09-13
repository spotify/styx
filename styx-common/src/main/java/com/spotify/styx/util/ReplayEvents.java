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

import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.SortedSet;

public final class ReplayEvents {

  private ReplayEvents() {
    throw new UnsupportedOperationException();
  }

  public static Optional<RunState> getBackfillRunState(
      WorkflowInstance workflowInstance,
      Storage storage,
      String backfillId) {
    final SettableTime time = new SettableTime();
    boolean backfillFound = false;

    final SortedSet<SequenceEvent> sequenceEvents;
    try {
      sequenceEvents = storage.readEvents(workflowInstance);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (sequenceEvents.isEmpty()) {
      return Optional.empty();
    }

    RunState restoredState = RunState.fresh(workflowInstance, time);

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
      }

      restoredState = restoredState.transition(sequenceEvent.event(), time);

      if (backfillFound(triggerExecutionEventMet, backfillId, restoredState)) {
        backfillFound = true;
      }
    }

    return backfillFound ? Optional.of(restoredState) : Optional.empty();
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
