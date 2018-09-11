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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureUtil {

  private FutureUtil() {
    throw new UnsupportedOperationException();
  }

  public static <T> CompletableFuture<T> exceptionallyCompletedFuture(final Throwable t) {
    CompletableFuture<T> future = new CompletableFuture<>();
    future.completeExceptionally(t);
    return future;
  }

  /**
   * Gathers results from futures that may fail with IOExceptions.
   * @return The values of all futures, in the same order.
   * @throws IOException if interrupted or any of the futures timed out or failed with an {@link IOException}.
   */
  public static <T> List<T> gatherIO(final List<Future<T>> futures, long timeout, TimeUnit timeUnit)
      throws IOException {
    final ImmutableList.Builder<T> values = ImmutableList.builder();
    for (Future<T> future : futures) {
      try {
        values.add(future.get(timeout, timeUnit));
      } catch (TimeoutException e) {
        throw new IOException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException(e);
      } catch (ExecutionException e) {
        final Throwable cause = e.getCause();
        Throwables.propagateIfPossible(cause, IOException.class);
        throw new RuntimeException(cause);
      }
    }
    return values.build();
  }
}
