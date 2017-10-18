/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

import com.spotify.styx.model.SequenceEvent;
import java.io.Closeable;

/**
 * Basic interface to consume {@link SequenceEvent}
 */
public interface EventConsumer extends Closeable {

  /**
   * This is meant to be called when an internal Styx event has been processed by the
   * {@link StateManager}.
   *
   * @param sequenceEvent The {@link SequenceEvent} that caused an internal state machine transition
   *                      via the {@link StateManager}
   * @throws IsClosed if the event consumer is closed and can not handle events
   */
  void processedEvent(SequenceEvent sequenceEvent) throws IsClosed;

  /**
   * Exception that signals that the {@link EventConsumer} is in a closed state.
   */
  class IsClosed extends Exception {
  }

  EventConsumer NOOP = new NoopEventConsumer();
}
