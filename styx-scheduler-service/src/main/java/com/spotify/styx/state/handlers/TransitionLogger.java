/*-
 * -\-\-
 * Spotify Styx Scheduler
 * --
 * Copyright (C) 2018 Spotify AB
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

package com.spotify.styx.state.handlers;

import static java.lang.String.format;

import com.google.common.annotations.VisibleForTesting;
import com.spotify.styx.model.Event;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.RunState;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransitionLogger implements OutputHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TransitionLogger.class);

  private final String prefix;

  // TODO: this probably wants to be an event listener instead?

  public TransitionLogger(String prefix) {
    this.prefix = Objects.requireNonNull(prefix);
  }

  @Override
  public Optional<Event> transitionInto(RunState state) {
    final String name = state.state().name().toLowerCase();
    LOG.info("{}{} transition -> {} {}", prefix, state.workflowInstance(), name, stateInfo(state));
    return Optional.empty();
  }

  @VisibleForTesting
  static String stateInfo(RunState state) {
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
}
