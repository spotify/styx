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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

public class FutureUtil {

  private FutureUtil() {
    throw new UnsupportedOperationException();
  }

  /**
   * Gathers results from futures that may fail with IOExceptions.
   * @param timeout A global timeout. All futures must have completed before this future completes. Any future
   *                not yet completed will be cancelled.
   * @return The values of all futures, in the same order.
   * @throws IOException if interrupted or any of the futures timed out or failed with an {@link IOException}.
   */
  public static <T> List<T> gatherIO(List<? extends CompletableFuture<? extends T>> futures,
                                     CompletionStage<Void> timeout)
      throws IOException {
    // Apply timeout
    timeout.thenRun(() -> futures.forEach(f -> f.completeExceptionally(new TimeoutException())));
    // Collect results
    var values = ImmutableList.<T>builder();
    for (var future : futures) {
      try {
        values.add(future.get());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      } catch (ExecutionException e) {
        final Throwable cause = e.getCause();
        Throwables.propagateIfPossible(cause, IOException.class);
        if (cause instanceof TimeoutException) {
          throw new IOException(cause);
        } else {
          throw new RuntimeException(cause);
        }
      }
    }
    return values.build();
  }
}
