/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

import static com.github.rholder.retry.StopStrategies.stopAfterDelay;
import static com.github.rholder.retry.WaitStrategies.exponentialWait;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategy;
import com.github.rholder.retry.WaitStrategy;
import com.google.api.client.googleapis.services.AbstractGoogleClientRequest;
import com.google.api.client.http.HttpResponseException;
import com.google.common.base.Throwables;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class GoogleApiClientUtil {

  private GoogleApiClientUtil() {
    throw new UnsupportedOperationException();
  }

  private static final WaitStrategy DEFAULT_RETRY_WAIT_STRATEGY = exponentialWait();
  private static final StopStrategy DEFAULT_RETRY_STOP_STRATEGY = stopAfterDelay(30, SECONDS);

  public static <T> T executeWithRetries(AbstractGoogleClientRequest<T> request) throws IOException {
    return executeWithRetries(request, DEFAULT_RETRY_WAIT_STRATEGY, DEFAULT_RETRY_STOP_STRATEGY);
  }

  public static <T> T executeWithRetries(AbstractGoogleClientRequest<T> request,
                                         WaitStrategy waitStrategy,
                                         StopStrategy stopStrategy) throws IOException {
    var retryer = RetryerBuilder.<T>newBuilder()
        .retryIfException(GoogleApiClientUtil::isRetryableRequestFailure)
        .withWaitStrategy(waitStrategy)
        .withStopStrategy(stopStrategy)
        .build();
    try {
      return retryer.call(request::execute);
    } catch (RetryException e) {
      throw new IOException(e);
    } catch (ExecutionException e) {
      Throwables.throwIfInstanceOf(e.getCause(), IOException.class);
      throw new IOException(e);
    }
  }

  private static boolean isRetryableRequestFailure(Throwable t) {
    if (t instanceof HttpResponseException) {
      var statusCode = ((HttpResponseException) t).getStatusCode();
      if (statusCode == 429) {
        return true;
      }
      return statusCode / 100 == 5;
    }
    return t instanceof IOException;
  }
}
