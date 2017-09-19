/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.publisher;

import com.spotify.styx.model.SequenceEvent;

/**
 * Interface for acting on internal Styx events asynchronously, to be used without any interaction
 * with the internal state machines and/or the storage layer in use by the service.
 */
public interface EventInterceptor {

  /**
   * Called when a state machine transition happens. The implementation of this will be called in
   * a separate thread asynchronously. To be used for external applications, like emitting event
   * information to external services.
   *
   * @param sequenceEvent The event being processed by the internal state machine
   */
  void eventTransitioned(SequenceEvent sequenceEvent);

  EventInterceptor NOOP = new NoopEventInterceptor();
}
