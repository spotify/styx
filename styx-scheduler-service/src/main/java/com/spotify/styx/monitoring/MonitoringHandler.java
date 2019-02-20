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

import com.spotify.styx.model.Event;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import java.util.Objects;
import java.util.Optional;

/**
 * An {@link OutputHandler} handler that reports workflow exit codes.
 */
public class MonitoringHandler implements OutputHandler {

  private final Stats stats;

  public MonitoringHandler(Stats stats) {
    this.stats = Objects.requireNonNull(stats);
  }

  @Override
  public Optional<Event> transitionInto(RunState state) {
    switch (state.state()) {
      case TERMINATED:
        if (state.data().lastExit().isPresent()) {
          stats.recordExitCode(state.data().lastExit().get());
        }
        break;

      default:
        // do nothing
    }
    return Optional.empty();
  }
}
