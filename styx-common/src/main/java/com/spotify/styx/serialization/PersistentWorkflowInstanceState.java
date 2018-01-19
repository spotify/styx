/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
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

package com.spotify.styx.serialization;

import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import io.norberg.automatter.AutoMatter;
import java.time.Instant;
import javax.annotation.Nullable;

@AutoMatter
public interface PersistentWorkflowInstanceState {

  long counter();

  // TODO: remove @Nullable when all persisted active states in datastore contains the runstate

  @Nullable Instant timestamp();

  @Nullable State state();

  @Nullable StateData data();

  static PersistentWorkflowInstanceStateBuilder builder() {
    return new PersistentWorkflowInstanceStateBuilder();
  }

  static PersistentWorkflowInstanceStateBuilder builder(RunState runState) {
    return builder()
        .timestamp(Instant.ofEpochMilli(runState.timestamp()))
        .state(runState.state())
        .data(runState.data());
  }

  static PersistentWorkflowInstanceState of(RunState runState, long counter) {
    return builder(runState).counter(counter).build();
  }

  static PersistentWorkflowInstanceState of(long counter) {
    return builder().counter(counter).build();
  }
}
