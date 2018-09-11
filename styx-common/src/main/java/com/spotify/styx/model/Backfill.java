/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2017 Spotify AB
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

package com.spotify.styx.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.norberg.automatter.AutoMatter;
import java.time.Instant;
import java.util.Optional;

@AutoMatter
@JsonIgnoreProperties(ignoreUnknown = true)
public interface Backfill {

  String id();

  Instant start();

  Instant end();

  WorkflowId workflowId();

  int concurrency();

  Optional<String> description();

  Instant nextTrigger();

  Schedule schedule();

  boolean allTriggered();

  boolean halted();

  boolean reverse();

  Optional<TriggerParameters> triggerParameters();

  BackfillBuilder builder();

  static BackfillBuilder newBuilder() {
    return new BackfillBuilder();
  }
}
