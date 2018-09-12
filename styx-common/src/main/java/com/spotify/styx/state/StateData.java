/*-
 * -\-\-
 * Spotify Styx Common
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.google.common.collect.Iterables;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.TriggerParameters;
import io.norberg.automatter.AutoMatter;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A value type for holding data related to the various states of the {@link RunState}.
 *
 * <p>Uses auto-matter over auto-value because we get a builder by default, which is also better
 * suited for copy-modifying values. Another plus is the boilerplate-free Jackson integration.
 */
@AutoMatter
@JsonIgnoreProperties(ignoreUnknown = true)
public interface StateData {

  int tries();
  int consecutiveFailures();
  double retryCost();
  Optional<Long> retryDelayMillis();
  Optional<Integer> lastExit();
  Optional<Trigger> trigger();
  Optional<String> triggerId(); //for backwards compatibility
  Optional<String> executionId();
  Optional<ExecutionDescription> executionDescription();
  Optional<Set<String>> resourceIds(); // resources referenced in the workflow configuration at the time of dequeue
  Optional<TriggerParameters> triggerParameters();

  /**
   * This field is deprecated and kept only for backwards compatibility.
   *
   * @deprecated Use {@link #message()} instead.
   */
  @Deprecated List<Message> messages();

  default Optional<Message> message() {
    return messages().isEmpty() ? Optional.empty() : Optional.of(Iterables.getLast(messages()));
  }

  StateDataBuilder builder();

  static StateDataBuilder newBuilder() {
    return new StateDataBuilder();
  }

  static StateData zero() {
    return newBuilder().build();
  }
}
