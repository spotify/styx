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

package com.spotify.styx;

import com.spotify.styx.model.Workflow;
import java.time.Instant;

/**
 * Interface to manage triggering of executions.
 */
public interface TriggerListener {

  /**
   * Handles a triggering event for some {@link Workflow} at some {@link Instant} in time.
   *
   * @param workflow   The workflow that generated the event
   * @param triggerId  The identifier for this trigger
   * @param instant    The instant at which the event is supposed to happen
   */
  void event(Workflow workflow, String triggerId, Instant instant);
}
