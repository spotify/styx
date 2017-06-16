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
import static java.util.concurrent.CompletableFuture.completedFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.net.HttpHeaders;
import com.spotify.apollo.Request;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.SyncHandler;
import io.norberg.automatter.AutoMatter;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
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

  public static final String BEARER_PREFIX = "Bearer ";
  private static final Set<String> BLACKLISTED_HEADERS = ImmutableSet.of(HttpHeaders.AUTHORIZATION);
  private static final GoogleIdTokenVerifier GOOGLE_ID_TOKEN_VERIFIER;

  static {
    final NetHttpTransport transport;
    try {
      transport = GoogleNetHttpTransport.newTrustedTransport();
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException(e);
    }
    GOOGLE_ID_TOKEN_VERIFIER = new GoogleIdTokenVerifier
        .Builder(transport, Utils.getDefaultJsonFactory())
        .build();
  }

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

  public static <T> Middleware<AsyncHandler<Response<T>>, AsyncHandler<Response<T>>> clientValidator(
      Supplier<Optional<List<String>>> supplier) {
    return innerHandler -> requestContext -> {
      if (requestContext.request().header("User-Agent")
          .map(header -> supplier.get().orElse(ImmutableList.of()).contains(header))
          .orElse(false)) {
        // TODO: fire some stats
        return
            completedFuture(Response.forStatus(Status.NOT_ACCEPTABLE.withReasonPhrase(
                "blacklisted client version, please upgrade")));
      } else {
        return innerHandler.invoke(requestContext);
      }
    };
  }

  public static <T> Middleware<AsyncHandler<Response<T>>, AsyncHandler<Response<T>>> exceptionHandler() {
    return innerHandler -> requestContext -> {
      try {
        return innerHandler.invoke(requestContext).handle((r, t) -> {
          if (t != null) {
            if (t instanceof ResponseException) {
              return ((ResponseException) t).getResponse();
            } else {
              throw new CompletionException(t);
            }
          } else {
            return r;
          }
        });
      } catch (ResponseException e) {
        return completedFuture(e.getResponse());
      }
    };
  }

  private static GoogleIdToken verifyIdToken(String s) {
    try {
      return GOOGLE_ID_TOKEN_VERIFIER.verify(s);
    } catch (GeneralSecurityException e) {
      return null; // will be treated as an invalid token
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> Middleware<AsyncHandler<Response<T>>, AsyncHandler<Response<T>>> auditLogger() {
    return innerHandler -> requestContext -> {
      final Request request = requestContext.request();
      final AuthContext authContext = auth(requestContext);

      if (!"GET".equals(request.method())) {
        LOG.info("[AUDIT] {} {} by {} with headers {} parameters {} and payload {}",
            request.method(),
            request.uri(),
            authContext.user().map(idToken -> idToken.getPayload().getEmail()).orElse("anonymous"),
            filterHeaders(request.headers()),
            request.parameters(),
            request.payload().map(ByteString::utf8).orElse("")
                .replaceAll("\n", " "));
      }
      return innerHandler.invoke(requestContext);
    };
  }

  @AutoMatter
  public interface AuthContext {
    Optional<GoogleIdToken> user();
  }

  private static AuthContext auth(RequestContext requestContext) {
    final Request request = requestContext.request();
    final boolean hasAuthHeader = request.header(HttpHeaders.AUTHORIZATION).isPresent();

    if (!hasAuthHeader) {
      return Optional::empty;
    }

    final String authHeader = request.header(HttpHeaders.AUTHORIZATION).get();
    if (!authHeader.startsWith(BEARER_PREFIX)) {
      throw new ResponseException(Response.forStatus(Status.BAD_REQUEST
          .withReasonPhrase("Authorization token must be of type Bearer")));
    }

    final GoogleIdToken googleIdToken;
    try {
      final String token = authHeader.substring(BEARER_PREFIX.length());
      googleIdToken = verifyIdToken(token);
    } catch (IllegalArgumentException e) {
      throw new ResponseException(Response.forStatus(Status.BAD_REQUEST
          .withReasonPhrase("Failed to parse Authorization token")), e);
    }

    if (googleIdToken == null) {
      throw new ResponseException(Response.forStatus(Status.UNAUTHORIZED
          .withReasonPhrase("Authorization token is invalid")));
    }

    return () -> Optional.of(googleIdToken);
  }

  private static Map<String, String> filterHeaders(Map<String, String> headers) {
    return headers.entrySet().stream()
        .filter(entry -> !BLACKLISTED_HEADERS.contains(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  // todo: make use of following middleware where we need to use the auth context in route handlers

  interface Authenticated<T> extends Function<AuthContext, T> {}
  interface Requested<T> extends Function<RequestContext, T> {}

  private static <T> Middleware<Requested<Authenticated<T>>, AsyncHandler<Response<ByteString>>> authed() {
    return ar -> jsonAsync().apply(requestContext -> {
      final T payload = ar
          .apply(requestContext)
          .apply(auth(requestContext));
      return completedFuture(Response.forPayload(payload));
    });
  }

  private static <T> Middleware<AsyncHandler<Response<T>>, AsyncHandler<Response<T>>> authValidator() {
    return h -> rc -> {
      // ensure auth context is valid (throws otherwise)
      auth(rc);

      return h.invoke(rc);
    };
  }
}
