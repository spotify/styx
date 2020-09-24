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

package com.spotify.styx.docker;

import com.google.common.io.Closer;
import com.spotify.styx.state.RunState;
import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

public abstract class AbstractRoutingRunner<T extends Closeable> implements Closeable {

  protected final Function<String, T> runnerFactory;
  protected final Function<RunState, String> runnerId;
  protected final ConcurrentMap<String, T> runners = new ConcurrentHashMap<>();

  public AbstractRoutingRunner(Function<String, T> runnerFactory, Function<RunState, String> runnerId) {
    this.runnerFactory = Objects.requireNonNull(runnerFactory);
    this.runnerId = Objects.requireNonNull(runnerId);
  }

  protected T runner(RunState runState) {
    var id = runnerId.apply(runState);
    return runners.computeIfAbsent(id, runnerFactory);
  }


  @Override
  public final void close() throws IOException {
    final var closer = Closer.create();
    runners.values().forEach(closer::register);
    closer.close();
  }
}
