/*-
 * -\-\-
 * Spotify Styx API Service
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

package com.spotify.styx.api;

import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.spotify.apollo.Request;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.SyncHandler;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import okio.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A collection of static methods implementing the apollo Middleware interface, useful for
 * transforming Response objects holding value objects into Response object holding byte
 * strings.
 */
public final class Middlewares {

  private static final Logger LOG = LoggerFactory.getLogger(Middlewares.class);

  private Middlewares() {
  }

  public static Middleware<SyncHandler<? extends Response<?>>, AsyncHandler<Response<ByteString>>>
      json() {
    return innerHandler -> jsonAsync().apply(Middleware.syncToAsync(innerHandler));
  }

  public static Middleware<AsyncHandler<? extends Response<?>>, AsyncHandler<Response<ByteString>>>
      jsonAsync() {
    return innerHandler -> innerHandler.map(response -> {
      if (!response.payload().isPresent()) {
        // noinspection unchecked
        return (Response<ByteString>) response;
      }

      final Object tPayload = response.payload().get();
      try {
        final byte[] bytes = OBJECT_MAPPER.writeValueAsBytes(tPayload);
        final ByteString payload = ByteString.of(bytes);

        return response.withPayload(payload)
            .withHeader("Content-Type", "application/json");
      } catch (JsonProcessingException e) {
        return Response.forStatus(
            Status.INTERNAL_SERVER_ERROR.withReasonPhrase(
                "Failed to serialize response " + e.getMessage()));
      }
    });
  }

  public static Middleware<AsyncHandler<? extends Response<?>>,
      AsyncHandler<? extends Response<ByteString>>> clientValidator(
      Supplier<Optional<List<String>>> supplier) {
    return innerHandler -> requestContext -> {
      if (requestContext.request().header("User-Agent")
          .map(header -> supplier.get().orElse(ImmutableList.of()).contains(header))
          .orElse(false)) {
        // TODO: fire some stats
        return CompletableFuture
            .completedFuture(Response.forStatus(Status.NOT_ACCEPTABLE.withReasonPhrase(
                "blacklisted client version, please upgrade")));
      } else {
        // noinspection unchecked
        return (CompletionStage<Response<ByteString>>) innerHandler.invoke(requestContext);
      }
    };
  }

  public static Middleware<AsyncHandler<? extends Response<?>>,
      AsyncHandler<? extends Response<ByteString>>> auditLogging() {
    return innerHandler -> requestContext -> {
      final Request request = requestContext.request();
      if (!"GET".equals(request.method())) {
        LOG.info("[AUDIT] {} {} with headers {} parameters {} and payload {}",
                 request.method(),
                 request.uri(),
                 request.headers(),
                 request.parameters(),
                 request.payload().map(ByteString::utf8).orElse("")
                     .replaceAll("\n", " "));
      }
      // noinspection unchecked
      return (CompletionStage<Response<ByteString>>) innerHandler.invoke(requestContext);
    };
  }
}
