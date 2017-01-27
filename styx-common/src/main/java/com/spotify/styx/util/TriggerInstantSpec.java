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

package com.spotify.styx.util;

import com.google.auto.value.AutoValue;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.WorkflowConfiguration;
import java.time.Instant;

/**
 * A value type containing the scheduled and offset time of a trigger. {@link #offsetInstant()} is
 * when the trigger actually should happen, while {@link #instant()} is the specified time
 * according to the {@link WorkflowConfiguration#schedule()}.
 */
@AutoValue
public abstract class TriggerInstantSpec {

  /**
   * The scheduled instant based on the Workflow {@link Schedule}.
   */
  public abstract Instant instant();

  /**
   * The actual instant at which the Workflow will be instantiated, with respect to the
   * {@link WorkflowConfiguration#schedule()}.
   */
  public abstract Instant offsetInstant();

  public static TriggerInstantSpec create(Instant instant, Instant triggerInstant) {
    return new AutoValue_TriggerInstantSpec(instant, triggerInstant);
  }
}
