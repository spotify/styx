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
import com.spotify.styx.storage.EventStorage;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;
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
      Map<WorkflowInstance, Long> instances,
      EventStorage eventStorage,
      boolean printLogs) throws IOException {
    LOG.info("Replaying active states");

    final OutputHandler replayLogger = printLogs ? transitionLogger("  ") : OutputHandler.NOOP;

    return instances.entrySet().parallelStream().map(entry -> {
      final WorkflowInstance workflowInstance = entry.getKey();
      final long lastConsumedEvent = entry.getValue();
      final SettableTime time = new SettableTime();
      if (printLogs) {
        LOG.info("Replaying {} up to #{}", workflowInstance.toKey(), lastConsumedEvent);
      }

      final SortedSet<SequenceEvent> sequenceEvents;
      try {
        sequenceEvents = eventStorage.readEvents(workflowInstance);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
      RunState restoreState = RunState.fresh(workflowInstance, time);

      for (SequenceEvent sequenceEvent : sequenceEvents) {
        // At the time of writing, we don't expect to get events while Styx is not running.
        // That is because the only event producers are going to be in the same process.
        // Thus, we don't expect any event in the sequence to be later than the last consumed
        // event. We will treat this as an error for now and skip the rest of the events.
        if (sequenceEvent.counter() > lastConsumedEvent) {
          LOG.error("Got unexpected newer event than the last consumed event {} > {} for {}",
                    sequenceEvent.counter(), lastConsumedEvent, workflowInstance.toKey());
          break;
        }

        time.set(Instant.ofEpochMilli(sequenceEvent.timestamp()));

        if ("triggerExecution".equals(EventUtil.name(sequenceEvent.event()))) {
          restoreState = RunState.fresh(workflowInstance, time);
        }

        if (printLogs) {
          LOG.info("  replaying #{} {}", sequenceEvent.counter(), sequenceEvent.event());
        }
        restoreState = restoreState.transition(sequenceEvent.event());
        replayLogger.transitionInto(restoreState);
      }

      return Tuple.of(restoreState, lastConsumedEvent);
    })
    .collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));
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
        return format("tries:%d execId:%s exitCode:%d",
            state.data().tries(), state.data().executionId(), state.data().lastExit());

      case QUEUED:
        return format("tries:%d delayMs:%d",
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
