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

package com.spotify.styx.util;

import io.grpc.Context;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

public class GrpcContextUtil {

  private GrpcContextUtil() {
    throw new UnsupportedOperationException();
  }

  /**
   * A variant of {@link Context#currentContextExecutor(Executor)} for {@link ExecutorService}s.
   */
  public static ExecutorService currentContextExecutorService(ExecutorService executorService) {
    return new CurrentContextExecutorService(executorService);
  }

  private static class CurrentContextExecutorService extends AbstractExecutorService {

    private final ExecutorService executorService;

    CurrentContextExecutorService(ExecutorService executorService) {
      this.executorService = Objects.requireNonNull(executorService, "executorService");
    }

    @Override
    public void shutdown() {
      executorService.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
      return executorService.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
      return executorService.isShutdown();
    }

    @Override
    public boolean isTerminated() {
      return executorService.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      return executorService.awaitTermination(timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
      executorService.execute(Context.current().wrap(command));
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
      return super.newTaskFor(Context.current().wrap(runnable), value);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
      return super.newTaskFor(Context.current().wrap(callable));
    }
  }
}
