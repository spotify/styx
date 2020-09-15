/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2020 Spotify AB
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

package com.spotify.styx.flyte;

import com.spotify.styx.docker.AbstractRoutingRunner;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.state.RunState;
import java.util.function.Function;

public class RoutingFlyteRunner extends AbstractRoutingRunner<FlyteRunner>
    implements FlyteRunner {

  public RoutingFlyteRunner(
      FlyteRunnerFactory runnerFactory,
      Function<RunState, String> runnerId) {
    super(runnerFactory, runnerId);
  }


  @Override
  public String createExecution(RunState runState, String name, FlyteExecConf flyteExecConf)
      throws CreateExecutionException {
    return runner(runState).createExecution(runState, name, flyteExecConf);
  }

  @Override
  public void terminateExecution(RunState runState, FlyteExecutionId flyteExecutionId) {
    runner(runState).terminateExecution(runState, flyteExecutionId);
  }

  @Override
  public void poll(FlyteExecutionId flyteExecutionId, RunState runState) throws PollingException {
    runner(runState).poll(flyteExecutionId, runState);
  }
}
