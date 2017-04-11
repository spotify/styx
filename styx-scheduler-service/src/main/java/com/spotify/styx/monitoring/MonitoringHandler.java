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

package com.spotify.styx.monitoring;

import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.Time;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An {@link OutputHandler} handler that reports execution elapsed time between submitted and
 * running {@link RunState} to FastForward.
 */
public class MonitoringHandler implements OutputHandler {

  private final Time time;
  private final Stats stats;

  private final Map<String, Instant> map = new HashMap<>();

  public MonitoringHandler(Time time, Stats stats) {
    this.time = Objects.requireNonNull(time);
    this.stats = Objects.requireNonNull(stats);
  }

  @Override
  public void transitionInto(RunState state) {
    switch (state.state()) {
      case SUBMITTED:
        final Instant submittedTime = time.get();
        map.put(state.workflowInstance().toKey(), submittedTime);
        break;

      case TERMINATED:
        if (state.data().lastExit().isPresent()) {
          stats.exitCode(state.workflowInstance().workflowId(), state.data().lastExit().get());
        }
        break;

      case RUNNING:
        final Instant a = time.get();
        final Instant b = map.remove(state.workflowInstance().toKey());
        final long durationSeconds = b.until(a, ChronoUnit.SECONDS);
        stats.submitToRunningTime(durationSeconds);

      default:
        // do nothing
    }
  }
}
