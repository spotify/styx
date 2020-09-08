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

import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.state.RunState;

public interface FlyteRunner {
  default boolean isEnabled() {
    return true;
  }

  FlyteExecutionId createExecution(final String name, final FlyteExecConf flyteExecConf) throws CreateExecutionException;

  static FlyteRunner noop() {
    return new NoopFlyteRunner();
  }

  class CreateExecutionException extends Exception {
    CreateExecutionException(FlyteExecConf conf, Throwable cause) {
      super("Couldn't create execution for:" + conf, cause);
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

  void poll(FlyteExecutionId flyteExecutionId, RunState runState)
      throws PollingException;

  class PollingException extends Exception {
    public PollingException(FlyteExecutionId id, Throwable cause) {
      super("Could not poll for execution: " + id.toUrn(), cause);
    }

    public PollingException(String message) {
      super(message);
    }

    public PollingException(String message, Throwable cause) {
      super(message, cause);
    }
  }

  class ExecutionNotFoundException extends PollingException {
    public ExecutionNotFoundException(FlyteExecutionId id, Throwable cause) {
      super("Could not poll for execution: " + id.toUrn(), cause);
    }
  }
}
