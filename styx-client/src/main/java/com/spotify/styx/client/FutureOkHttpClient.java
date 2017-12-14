/*-
 * -\-\-
 * styx-client
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

package com.spotify.styx.client;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;

/**
 * Wrap OkHttpClient and return a CompletionStage instead of having to pass callbacks.
 */
class FutureOkHttpClient {

  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(90);
  private static final Duration DEFAULT_WRITE_TIMEOUT = Duration.ofSeconds(90);

  static final MediaType APPLICATION_JSON = MediaType.parse("application/json");

  private final OkHttpClient client;

  static FutureOkHttpClient createDefault() {
    return new FutureOkHttpClient(
        new OkHttpClient.Builder()
            .connectTimeout(DEFAULT_CONNECT_TIMEOUT.getSeconds(), TimeUnit.SECONDS)
            .readTimeout(DEFAULT_READ_TIMEOUT.getSeconds(), TimeUnit.SECONDS)
            .writeTimeout(DEFAULT_WRITE_TIMEOUT.getSeconds(), TimeUnit.SECONDS)
            .build()
    );
  }

  FutureOkHttpClient(OkHttpClient client) {
    this.client = client;
  }

  CompletionStage<Response> send(Request request) {
    final CompletableFuture<Response> future = new CompletableFuture<>();

    client.newCall(request).enqueue(new Callback() {
      @Override
      public void onFailure(Call call, IOException e) {
        future.completeExceptionally(e);
      }

      @Override
      public void onResponse(Call call, Response response) throws IOException {
        future.complete(response);
      }
    });

    return future;
  }
}
