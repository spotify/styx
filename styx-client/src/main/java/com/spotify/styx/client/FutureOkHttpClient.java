/*-
 * -\-\-
 * Spotify Styx API Client
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.spotify.styx.serialization.Json;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.ByteString;

/**
 * Wrap OkHttpClient and return a CompletionStage instead of having to pass callbacks.
 */
class FutureOkHttpClient implements Closeable {

  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
  private static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(90);
  private static final Duration DEFAULT_WRITE_TIMEOUT = Duration.ofSeconds(90);

  static final MediaType APPLICATION_JSON =
      Objects.requireNonNull(MediaType.parse("application/json"));

  private final OkHttpClient client;

  public static FutureOkHttpClient create(OkHttpClient client) {
    return new FutureOkHttpClient(client);
  }

  public static FutureOkHttpClient createDefault() {
    return FutureOkHttpClient.create(
        new OkHttpClient.Builder()
            .connectTimeout(DEFAULT_CONNECT_TIMEOUT.getSeconds(), TimeUnit.SECONDS)
            .readTimeout(DEFAULT_READ_TIMEOUT.getSeconds(), TimeUnit.SECONDS)
            .writeTimeout(DEFAULT_WRITE_TIMEOUT.getSeconds(), TimeUnit.SECONDS)
            .build()
    );
  }

  private FutureOkHttpClient(OkHttpClient client) {
    this.client = client;
  }

  public CompletionStage<Response> send(Request request) {
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

  private static Request internalForUri(HttpUrl uri, String method, ByteString payload) {
    return new Request.Builder().url(uri.uri().toString())
        .method(method, RequestBody.create(APPLICATION_JSON, payload))
        .build();
  }

  public static Request forUri(HttpUrl uri, String method, Object payload) {
    try {
      return internalForUri(uri, method, Json.serialize(payload));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static Request forUri(HttpUrl.Builder uriBuilder, String method, Object payload) {
    return forUri(uriBuilder.build(), method, payload);
  }

  public static Request forUri(HttpUrl uri, String method) {
    return new Request.Builder().url(uri.uri().toString()).method(method, null).build();
  }

  public static Request forUri(HttpUrl.Builder uriBuilder, String method) {
    return forUri(uriBuilder.build(), method);
  }

  public static Request forUri(HttpUrl.Builder uriBuilder) {
    return forUri(uriBuilder.build());
  }

  public static Request forUri(HttpUrl uri) {
    return new Request.Builder().url(uri.uri().toString()).build();
  }

  public void close() {
    client.connectionPool().evictAll();
    client.dispatcher().executorService().shutdown();
  }
}
