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

import com.spotify.styx.flyte.client.FlyteAdminClient;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import java.io.Closeable;
import java.util.function.Function;

public interface FlyteRunner extends Closeable {
  default boolean isEnabled() {
    return true;
  }

  String createExecution(RunState runState, String name, FlyteExecConf flyteExecConf)
      throws CreateExecutionException;

  void terminateExecution(RunState runState, FlyteExecutionId flyteExecutionId);

  void poll(FlyteExecutionId flyteExecutionId, RunState runState);

  static FlyteRunner noop() {
    return new NoopFlyteRunner();
  }

  static FlyteRunner flyteAdmin(final String runnerId,
                                final FlyteAdminClient flyteAdminClient,
                                final StateManager stateManager) {
    final var runner = new FlyteAdminClientRunner(runnerId, flyteAdminClient, stateManager);
    runner.init();
    return runner;
  }

  /**
   * Creates a {@link FlyteRunner} that will dynamically create and route to other Flyte runner
   * instances using the given factory.
   *
   * <p>The active Flyte runner id will be read from runnerId supplier on each routing decision.
   */
  static FlyteRunner routing(FlyteRunner.FlyteRunnerFactory flyteRunnerFactory, Function<RunState, String> runnerId) {
    return new RoutingFlyteRunner(flyteRunnerFactory, runnerId);
  }

  /**
   * Factory for {@link FlyteRunner} instances identified by a string identifier
   */
  interface FlyteRunnerFactory extends Function<String, FlyteRunner> { }

  class CreateExecutionException extends Exception {
    CreateExecutionException(FlyteExecConf conf, Throwable cause) {
      super("Couldn't create execution for:" + conf + " cause: " + cause.getMessage(), cause);
    }

    public CreateExecutionException(String message) {
      super(message);
    }
    public CreateExecutionException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  class LaunchPlanNotFound extends CreateExecutionException {
    public LaunchPlanNotFound(FlyteExecConf conf, Throwable cause) {
      super("Launch plan not found: " + conf.referenceId(), cause);
    }
  }
}
