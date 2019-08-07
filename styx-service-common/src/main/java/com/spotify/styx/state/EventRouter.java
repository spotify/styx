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

import com.spotify.styx.model.Event;
import com.spotify.styx.util.IsClosedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface EventRouter {

  Logger LOG = LoggerFactory.getLogger(EventRouter.class);

  /**
   * Receive an {@link Event} and route it to the corresponding active {@link RunState} based on
   * the {@link Event#workflowInstance()} key of the event.
   *
   * @param event The event to receive
   * @throws IsClosedException if the state receiver is closed and can not handle events
   */
  void receive(Event event) throws IsClosedException;

  /**
   * Receive an {@link Event} and route it to the corresponding active {@link RunState} based on
   * the {@link Event#workflowInstance()} key of the event.
   *
   * @param event   The event to receive
   * @param counter The state counter upon which the event must act upon
   * @throws IsClosedException if the state receiver is closed and can not handle events
   */
  void receive(Event event, long counter) throws IsClosedException;

  /**
   * Like {@link #receive(Event)} but ignoring the {@link IsClosedException} exception.
   *
   * @param event The event to receive
   */
  default void receiveIgnoreClosed(Event event) {
    try {
      receive(event);
    } catch (IsClosedException isClosedException) {
      LOG.info("Ignored event, state receiver closed", isClosedException);
    }
  }

  /**
   * Like {@link #receive(Event)} but ignoring the {@link IsClosedException} exception.
   *
   * @param event The event to receive
   * @param counter The state counter upon which the event must act upon
   */
  default void receiveIgnoreClosed(Event event, long counter) {
    try {
      receive(event, counter);
    } catch (IsClosedException isClosedException) {
      LOG.info("Ignored event, state receiver closed", isClosedException);
    }
  }
}
