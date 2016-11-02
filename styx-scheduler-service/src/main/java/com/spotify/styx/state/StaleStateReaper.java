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

package com.spotify.styx.state;

/**
 * An interface for triggering timeouts on stale {@link RunState}s.
 *
 * <p>Whether a state is stale or not is determined by a set of TTL values corresponding to each
 * state.
 */
public interface StaleStateReaper {

  /**
   * Checks all active states and triggers a
   * {@link com.spotify.styx.model.Event#timeout(com.spotify.styx.model.WorkflowInstance)} event
   * on states that are stale.
   *
   * <p>If the underlying state store is closed, this method will have no effect.
   */
  void triggerTimeouts();
}
