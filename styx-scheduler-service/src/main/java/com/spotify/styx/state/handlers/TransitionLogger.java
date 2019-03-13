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
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.state.RunState;
import com.spotify.styx.util.EventUtil;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransitionLogger implements BiConsumer<SequenceEvent, RunState> {

  private final Logger log;

  public TransitionLogger() {
    this(LoggerFactory.getLogger(TransitionLogger.class));
  }

  TransitionLogger(Logger log) {
    this.log = Objects.requireNonNull(log, "log");
  }

  @Override
  public void accept(SequenceEvent sequenceEvent, RunState state) {
    var stateName = state.state().name().toLowerCase(Locale.ROOT);
    log.info("{} transition #{} {}({}) -> {} {}",
        state.workflowInstance(),
        sequenceEvent.counter(),
        EventUtil.name(sequenceEvent.event()),
        EventUtil.info(sequenceEvent.event()),
        stateName,
        stateInfo(state));
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
