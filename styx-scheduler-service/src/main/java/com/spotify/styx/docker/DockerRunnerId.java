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

package com.spotify.styx.docker;

import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.state.RunState;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public class DockerRunnerId implements Function<RunState, String> {

  private Supplier<StyxConfig> styxConfig;

  public DockerRunnerId(Supplier<StyxConfig> styxConfig) {
    this.styxConfig = Objects.requireNonNull(styxConfig, "styxConfig");
  }

  @Override
  public String apply(RunState runState) {
    if (runState.state() == RunState.State.SUBMITTING) {
      return styxConfig.get().globalDockerRunnerId();
    }
    return runState.data().runnerId().orElseGet(() -> styxConfig.get().globalDockerRunnerId());
  }
}
