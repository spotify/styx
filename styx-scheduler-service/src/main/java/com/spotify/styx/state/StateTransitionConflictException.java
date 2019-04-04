/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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
 * An exception signifying a {@link RunState} transition failed due to conflict with another concurrent transition.
 * It is generally safe to swallow this exception in situations where it is known that the operation will be retried.
 */
public class StateTransitionConflictException extends RuntimeException {

  public StateTransitionConflictException(Throwable cause) {
    super(cause);
  }

  public StateTransitionConflictException(String message) {
    super(message);
  }
}
