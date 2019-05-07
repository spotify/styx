/*-
 * -\-\-
 * Spotify Styx Service Common
 * --
 * Copyright (C) 2019 Spotify AB
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

package com.spotify.styx.state;

import static com.spotify.styx.util.GuardedRunnable.runGuarded;

import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.monitoring.TracingProxy;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

@FunctionalInterface
public interface EventConsumer extends BiConsumer<SequenceEvent, RunState> {

  static EventConsumer fanEvent(Collection<EventConsumer> eventConsumers) {
    return (sequenceEvent, runState) -> eventConsumers
        .forEach(eventConsumer -> runGuarded(() -> eventConsumer.accept(sequenceEvent, runState)));
  }

  static EventConsumer tracing(EventConsumer eventConsumer) {
    return TracingProxy.instrument(EventConsumer.class, eventConsumer);
  }

  static List<EventConsumer> tracing(Collection<EventConsumer> eventConsumers) {
    return eventConsumers.stream().map(EventConsumer::tracing).collect(Collectors.toList());
  }
}
