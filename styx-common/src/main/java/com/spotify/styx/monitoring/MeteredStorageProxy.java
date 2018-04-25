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

import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.Time;
import java.lang.reflect.Proxy;
import javaslang.control.Try;

/**
 * A proxy for instrumenting an instance of {@link Storage} {@link Proxy#newProxyInstance}.
 */
public class MeteredStorageProxy extends MeteredProxy<Storage> {

  MeteredStorageProxy(Storage delegate, Stats stats, Time time) {
    super(delegate, stats, time);
  }

  public static Storage instrument(Storage storage, Stats stats, Time time) {
    return MeteredProxy.instrument(Storage.class, new MeteredStorageProxy(storage, stats, time));
  }

  @Override
  protected void checkResult(final String operation, final long durationMillis,
                               final Try<?> result,
                               final Stats stats) {
    final String status = (result.isSuccess()) ? "success" : "failure";
    stats.recordStorageOperation(operation, durationMillis, status);
  }
}
