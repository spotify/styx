/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx.docker;

import static java.util.Objects.requireNonNull;

import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.state.RunState;
import java.util.function.Function;
import java.util.function.Supplier;

public class RunnerId implements Function<RunState, String> {

  private final Supplier<StyxConfig> styxConfig;
  private final Function<StyxConfig, String> extractorFunction;

  private RunnerId(Supplier<StyxConfig> styxConfig,
                  Function<StyxConfig, String> extractorFunction) {
    this.styxConfig = requireNonNull(styxConfig, "styxConfig");
    this.extractorFunction = requireNonNull(extractorFunction, "extractorFunction");
  }

  @Override
  public String apply(RunState runState) {
    if (runState.state() == RunState.State.SUBMITTING) {
      return extractorFunction.apply(styxConfig.get());
    }
    return runState.data().runnerId().orElseGet(() -> extractorFunction.apply(styxConfig.get()));
  }

  public static RunnerId dockerRunnerId(Supplier<StyxConfig> styxConfig) {
    return new RunnerId(styxConfig, StyxConfig::globalDockerRunnerId);
  }

  public static RunnerId flyteRunnerId(Supplier<StyxConfig> styxConfig) {
    return new RunnerId(styxConfig, StyxConfig::globalFlyteRunnerId);
  }
}
