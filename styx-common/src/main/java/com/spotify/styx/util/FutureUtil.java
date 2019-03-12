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

import static java.util.stream.Collectors.toMap;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeoutException;
import javaslang.control.Try;

public class FutureUtil {

  private FutureUtil() {
    throw new UnsupportedOperationException();
  }

  /**
   * Gathers results from futures that may fail or time out.
   * @param timeout A global timeout. All futures must have completed before this future completes. Any future
   *                not yet completed will be cancelled.
   * @return The values or failures of all futures.
   */
  public static <K, T> Map<K, Try<T>> gatherIO(
      final Map<K, ? extends CompletableFuture<T>> futures, CompletionStage<Void> timeout) {
    return futures.entrySet().stream()
        // Apply timeout
        .peek(e -> timeout.thenRun(() -> e.getValue().completeExceptionally(new TimeoutException())))
        // Collect results
        .collect(toMap(Map.Entry::getKey, e -> Try.of(() -> e.getValue().get())));
  }
}
