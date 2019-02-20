/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
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

package com.spotify.styx.state;

import com.spotify.styx.util.MDCUtil;
import eu.javaspecialists.tjsn.concurrency.stripedexecutor.StripedExecutorService;
import eu.javaspecialists.tjsn.concurrency.stripedexecutor.StripedRunnable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Utilities for working with {@link StripedExecutorService}.
 */
class Striping {

  private Striping() {
    throw new UnsupportedOperationException();
  }

  static <T> CompletableFuture<T> supplyAsyncStriped(Supplier<T> supplier, Object stripe,
      StripedExecutorService executor) {

    final CompletableFuture<T> future = new CompletableFuture<>();

    // This runnable must be created here on the calling thread
    final Runnable runnable = MDCUtil.withMDC(() -> {
      try {
        future.complete(supplier.get());
      } catch (Throwable e) {
        future.completeExceptionally(e);
      }
    });

    // The StripedRunnable cannot be wrapped using withMDC as that would hide the stripe
    executor.execute(new StripedRunnable() {
      @Override
      public Object getStripe() {
        return stripe;
      }

      @Override
      public void run() {
        runnable.run();
      }
    });

    return future;
  }
}
