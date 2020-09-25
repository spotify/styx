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
import java.util.Map;

/**
 * No-op {@code FlyteRunner} meant to be used when Flyte is disabled or not available.
 */
class NoopFlyteRunner implements FlyteRunner {

  @Override
  public boolean isEnabled() {
    return false;
  }

  @Override
  public String createExecution(RunState runState, String name, FlyteExecConf flyteExecConf,
                                Map<String, String> annotations)
      throws CreateExecutionException {
    throw new CreateExecutionException("Cannot create execution for: " + flyteExecConf);
  }

  @Override
  public void terminateExecution(RunState runState, FlyteExecutionId flyteExecutionId) {
    throw new IllegalStateException("Cannot halt execution : " + flyteExecutionId);
  }

  @Override
  public void poll(final FlyteExecutionId flyteExecutionId, final RunState runState)
      throws PollingException {
    throw new PollingException("Cannot poll for execution: " + flyteExecutionId.toUrn());
  }

  @Override
  public void close() {
    // nothing to close
  }
}
