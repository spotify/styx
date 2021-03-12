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
import java.io.IOException;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wrap OkHttpClient and return a CompletionStage instead of having to pass callbacks.
 */
class FutureOkHttpClient implements AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(FutureOkHttpClient.class);

  private static final MediaType APPLICATION_JSON =
      Objects.requireNonNull(MediaType.parse("application/json"));

  private final OkHttpClient client;

  static FutureOkHttpClient create(OkHttpClient client) {
    return new FutureOkHttpClient(client);
  }

  private FutureOkHttpClient(OkHttpClient client) {
    this.client = client;
  }

  CompletionStage<Response> send(Request request) {
    log.debug("{} {}", request.method(), request.url());
    final long start = System.nanoTime();

    final CompletableFuture<Response> future = new CompletableFuture<>();

    client.newCall(request).enqueue(new Callback() {
      @Override
      public void onFailure(Call call, IOException e) {
        log.debug("{} {}: failed (latency: {}s)", request.method(), request.url(), latency(start), e);
        future.completeExceptionally(e);
      }

      @Override
      public void onResponse(Call call, Response response) throws IOException {
        log.debug("{} {}: {} {} (latency: {}s)", request.method(), request.url(), response.code(), response.message(),
            latency(start));
        future.complete(response);
      }
    });

    return future;
  }

  private static String latency(long start) {
    final long end = System.nanoTime();
    return Long.toString(TimeUnit.NANOSECONDS.toSeconds(end - start));
  }

  private static Request internalForUri(HttpUrl uri, String method, ByteString payload) {
    return new Request.Builder().url(uri.uri().toString())
        .method(method, RequestBody.create(APPLICATION_JSON, payload))
        .build();
  }

  static Request forUri(HttpUrl uri, String method, Object payload) {
    try {
      return internalForUri(uri, method, Json.serialize(payload));
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  static Request forUri(HttpUrl.Builder uriBuilder, String method, Object payload) {
    return forUri(uriBuilder.build(), method, payload);
  }

  static Request forUri(HttpUrl uri, String method) {
    return new Request.Builder().url(uri.uri().toString()).method(method, null).build();
  }

  static Request forUri(HttpUrl.Builder uriBuilder, String method) {
    return forUri(uriBuilder.build(), method);
  }

  static Request forUri(HttpUrl.Builder uriBuilder) {
    return forUri(uriBuilder.build());
  }

  static Request forUri(HttpUrl uri) {
    return new Request.Builder().url(uri.uri().toString()).build();
  }

  @Override
  public void close() {
    client.connectionPool().evictAll();
    client.dispatcher().executorService().shutdown();
  }
}
