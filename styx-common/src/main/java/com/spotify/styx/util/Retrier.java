/*
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 Spotify AB
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
import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple utility for running a function with retries.
 */
public class Retrier {

  private static final Logger LOG = LoggerFactory.getLogger(Retrier.class);

  private final int maxRetries;
  private final Duration retryDelay;
  private final String errorMessage;

  private Retrier(Builder builder) {
    this.maxRetries = builder.maxRetries;
    this.retryDelay = builder.retryDelay;
    this.errorMessage = builder.errorMessage;
  }

  /**
   * Run an operation and retry on exceptions.
   *
   * @param operation  The operation function to run
   * @param <T>        The return type of the operation function
   * @return the value returned from the operation
   * @throws Exception if the retry limit is reached. The thrown exception will be the last
   * exception thrown by the operation function.
   */
  public <T> T runWithRetries(FnWithException<T, ? extends Exception> operation) throws Exception {
    int retries = 0;

    while (retries < maxRetries) {
      try {
        return operation.apply();
      } catch (Exception e) {
        if (++retries == maxRetries) {
          throw e;
        }

        LOG.warn(String.format("Failed to %s (attempt #%d)", errorMessage, retries), e);
        try {
          Thread.sleep(retryDelay.toMillis());
        } catch (InterruptedException e1) {
          throw Throwables.propagate(e1);
        }
      }
    }

    throw new Exception("This should never happen");
  }

  /**
   * Run an operation and retry on exceptions.
   *
   * @param operation  The operation function to run
   * @throws Exception if the retry limit is reached. The thrown exception will be the last
   * exception thrown by the operation function.
   */
  public void runWithRetries(RunnableWithException<? extends Exception> operation) throws Exception {
    runWithRetries((FnWithException<Void, Exception>) () -> {
      operation.run();
      return null;
    });
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private static final int DEFAULT_MAX_RETRIES = 10;
    private static final int DEFAULT_RETRY_DELAY_SECONDS = 30;

    private int maxRetries = DEFAULT_MAX_RETRIES;
    private Duration retryDelay = Duration.ofSeconds(DEFAULT_RETRY_DELAY_SECONDS);
    private String errorMessage = "run operation";

    public Builder maxRetries(int maxRetries) {
      this.maxRetries = maxRetries;
      return this;
    }

    public Builder retryDelay(Duration retryDelay) {
      this.retryDelay = retryDelay;
      return this;
    }

    public Builder errorMessage(String errorMessage) {
      this.errorMessage = errorMessage;
      return this;
    }

    public Retrier build() {
      return new Retrier(this);
    }
  }
}
