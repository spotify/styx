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

import com.spotify.styx.state.RunState;
import java.io.IOException;
import java.util.function.Function;

/**
 * A {@link DockerRunner} that routes to several underlying instances that are created using the
 * injected {@link com.spotify.styx.docker.DockerRunner.DockerRunnerFactory}.
 *
 * <p>Current implementation only creates one runner with the default id.
 */
class RoutingDockerRunner extends AbstractRoutingRunner<DockerRunner>
    implements DockerRunner {

  RoutingDockerRunner(DockerRunnerFactory dockerRunnerFactory, Function<RunState, String> runnerId) {
    super(dockerRunnerFactory, runnerId);
  }

  @Override
  public String start(RunState runState, RunSpec runSpec) throws IOException {
    return runner(runState).start(runState, runSpec);
  }

  @Override
  public void poll(RunState runState) {
    runner(runState).poll(runState);
  }

  @Override
  public void cleanup() throws IOException {
    for (var v: runners.values()) {
      v.cleanup();
    }
  }
}
