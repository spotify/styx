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

import static java.lang.String.format;

import com.google.common.base.Throwables;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.stream.Collectors;
import javaslang.Tuple;
import javaslang.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ReplayEvents {

  private static final Logger LOG = LoggerFactory.getLogger(ReplayEvents.class);

  private ReplayEvents() {
  }

  public static Map<RunState, Long> replayActiveStates(
      Map<WorkflowInstance, com.google.cloud.Tuple<Long, RunState>> instances,
      Storage storage,
      boolean printLogs) throws IOException {
    LOG.info("Replaying active states");

    final OutputHandler replayLogger = printLogs ? transitionLogger("  ") : OutputHandler.NOOP;

    return instances.entrySet().parallelStream().map(entry -> {
      final WorkflowInstance workflowInstance = entry.getKey();
      final long lastConsumedEvent = entry.getValue().x();
      final RunState runState = entry.getValue().y();
      final SettableTime time = new SettableTime();
      if (printLogs) {
        LOG.debug("Replaying {} up to #{}", workflowInstance.toKey(), lastConsumedEvent);
      }

      final SortedSet<SequenceEvent> sequenceEvents;
      try {
        sequenceEvents = storage.readEvents(workflowInstance);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
      RunState restoredState = RunState.fresh(workflowInstance, time);

      for (SequenceEvent sequenceEvent : sequenceEvents) {
        // At the time of writing, we don't expect to get events while Styx is not running.
        // That is because the only event producers are going to be in the same process.
        // Thus, we don't expect any event in the sequence to be later than the last consumed
        // event. We will treat this as an error for now and skip the rest of the events.
        if (sequenceEvent.counter() > lastConsumedEvent) {
          if (printLogs) {
            LOG.error("Got unexpected newer event than the last consumed event {} > {} for {}",
                sequenceEvent.counter(), lastConsumedEvent, workflowInstance.toKey());
          }
          break;
        }

        time.set(Instant.ofEpochMilli(sequenceEvent.timestamp()));

        if ("triggerExecution".equals(EventUtil.name(sequenceEvent.event()))) {
          restoredState = RunState.fresh(workflowInstance, time);
        }

        if (printLogs) {
          LOG.debug("  replaying #{} {}", sequenceEvent.counter(), sequenceEvent.event());
        }
        restoredState = restoredState.transition(sequenceEvent.event());
        replayLogger.transitionInto(restoredState);
      }

      return Tuple.of(restoredState, lastConsumedEvent);
    })
    .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
  }

  public static Optional<RunState> getBackfillRunState(
      WorkflowInstance workflowInstance,
      Map<WorkflowInstance, com.google.cloud.Tuple<Long, RunState>> activeWorkflowInstances,
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
        activeWorkflowInstances.getOrDefault(workflowInstance,
          com.google.cloud.Tuple.of(sequenceEvents.last().counter(), restoredState)).x();

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

  public static OutputHandler transitionLogger(String prefix) {
    return (state) -> {
      final String instanceKey = state.workflowInstance().toKey();
      LOG.info(
          "{}{} transition -> {} {}",
          prefix, instanceKey, state.state().name().toLowerCase(), stateInfo(state));
    };
  }

  private static String stateInfo(RunState state) {
    switch (state.state()) {
      case NEW:
      case PREPARE:
      case ERROR:
      case DONE:
        return format("tries:%d", state.data().tries());

      case SUBMITTED:
      case RUNNING:
      case FAILED:
        return format("tries:%d execId:%s",
            state.data().tries(), state.data().executionId());

      case TERMINATED:
        return format("tries:%d execId:%s exitCode:%s",
            state.data().tries(), state.data().executionId(), state.data().lastExit().map(
                String::valueOf).orElse("-"));

      case QUEUED:
        return format("tries:%d delayMs:%s",
            state.data().tries(), state.data().retryDelayMillis());

      default:
        return "";
    }
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
