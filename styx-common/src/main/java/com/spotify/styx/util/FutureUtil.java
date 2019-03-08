/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2017 Spotify AB
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

package com.spotify.styx.util;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javaslang.control.Try;

public class FutureUtil {

  private FutureUtil() {
    throw new UnsupportedOperationException();
  }

  public static <T> CompletableFuture<T> exceptionallyCompletedFuture(final Throwable t) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(t);
    return future;
  }

  public static <T> CompletableFuture<T> exceptionallyCompletedFuture(final Throwable t, final Class<T> cls) {
    return exceptionallyCompletedFuture(t);
  }

  /**
   * Gathers results from futures that may fail.
   * @return The values or failures of all futures.
   */
  public static <K, T> Map<K, Try<T>> gatherIO(
      final Map<K, ? extends Future<T>> futures, long timeout, TimeUnit timeUnit) {
    final ImmutableMap.Builder<K, Try<T>> results = ImmutableMap.builder();
    for (Map.Entry<K, ? extends Future<T>> entry : futures.entrySet()) {
      results.put(entry.getKey(), Try.of(() -> entry.getValue().get(timeout, timeUnit)));
    }
    return results.build();
  }
}
