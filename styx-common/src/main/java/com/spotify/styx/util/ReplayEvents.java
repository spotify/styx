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

import com.google.common.base.Throwables;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.RunState;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;

public final class ReplayEvents {

  private ReplayEvents() {
    throw new UnsupportedOperationException();
  }

  // TODO: fix NPath complexity
  public static Optional<RunState> getBackfillRunState(
      WorkflowInstance workflowInstance,
      Map<WorkflowInstance, RunState> activeWorkflowInstances,
      Storage storage,
      String backfillId) {
    final SettableTime time = new SettableTime();
    boolean backfillFound = false;

    final SortedSet<SequenceEvent> sequenceEvents;
    try {
      sequenceEvents = storage.readEvents(workflowInstance);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    if (sequenceEvents.isEmpty()) {
      return Optional.empty();
    }

    RunState restoredState = RunState.fresh(workflowInstance, time);

    final long lastConsumedEvent =
        Optional.ofNullable(activeWorkflowInstances.get(workflowInstance))
            .map(RunState::counter)
            .orElse(sequenceEvents.last().counter());

    for (SequenceEvent sequenceEvent : sequenceEvents) {
      // The active state event counters are read before the events themselves and styx is 
      // concurrently storing events, thus we might encounter an event with a counter value that is
      // later than the earlier read active state event counter. Events _after_ the active state
      // event counter might be dropped in some circumstances, hence we stop processing here to
      // avoid returning phantom data.
      if (sequenceEvent.counter() > lastConsumedEvent) {
        break;
      }

      time.set(Instant.ofEpochMilli(sequenceEvent.timestamp()));
      if ("triggerExecution".equals(EventUtil.name(sequenceEvent.event()))) {
        if (backfillFound) {
          return Optional.of(restoredState);
        }
        restoredState = RunState.fresh(workflowInstance, time);
      }

      restoredState = restoredState.transition(sequenceEvent.event());
      if ("triggerExecution".equals(EventUtil.name(sequenceEvent.event()))
          && restoredState.data().trigger().isPresent()
          && backfillId.equals(TriggerUtil.triggerId(restoredState.data().trigger().get()))) {
        backfillFound = true;
      }
    }
    return backfillFound ? Optional.of(restoredState) : Optional.empty();
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
