/*-
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

package com.spotify.styx.monitoring;

import static com.spotify.styx.util.ExceptionUtil.findCause;

import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.docker.InvalidExecutionException;
import com.spotify.styx.state.StateTransitionConflictException;
import com.spotify.styx.util.Time;
import java.lang.reflect.Proxy;
import javaslang.control.Try;

/**
 * A proxy for instrumenting an instance of {@link DockerRunner} using {@link Proxy#newProxyInstance}.
 */
public class MeteredDockerRunnerProxy extends MeteredProxy<DockerRunner> {

  private MeteredDockerRunnerProxy(DockerRunner delegate, Stats stats, Time time) {
    super(delegate, stats, time);
  }

  public static DockerRunner instrument(DockerRunner dockerRunner, Stats stats, Time time) {
    return MeteredProxy.instrument(DockerRunner.class, new MeteredDockerRunnerProxy(dockerRunner, stats, time));
  }

  @Override
  protected void checkResult(String operation, long durationMillis, Try<?> result, Stats stats) {
    result.onFailure(error -> reportDockerOperationError(operation, error, stats));
    final String status = (result.isSuccess()) ? "success" : "failure";
    stats.recordDockerOperation(operation, durationMillis, status);
  }

  private void reportDockerOperationError(String operation, Throwable e, Stats stats) {
    if (findCause(e, StateTransitionConflictException.class) != null) {
      stats.recordDockerOperationError(operation, "state-transition-conflict");
    } else if (findCause(e, InvalidExecutionException.class) != null) {
      stats.recordDockerOperationError(operation, "invalid-execution");
    } else {
      stats.recordDockerOperationError(operation, "unknown");
    }
  }
}
