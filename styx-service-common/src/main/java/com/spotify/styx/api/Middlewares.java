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

import static com.spotify.apollo.Status.INTERNAL_SERVER_ERROR;
import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static io.opencensus.trace.AttributeValue.stringAttributeValue;
import static io.opencensus.trace.Status.UNKNOWN;
import static java.util.concurrent.CompletableFuture.completedFuture;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;
import com.spotify.apollo.Request;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.entity.EntityMiddleware.EntityResponseHandler;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.SyncHandler;
import com.spotify.styx.util.MDCUtil;
import io.norberg.automatter.AutoMatter;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import okio.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * A collection of static methods implementing the apollo Middleware interface, useful for
 * transforming Response objects holding value objects into Response object holding byte
 * strings.
 */
public final class Middlewares {

  private static final Logger LOG = LoggerFactory.getLogger(Middlewares.class);

  private static final List<String> BLACKLISTED_HEADERS =
      ImmutableList.of(HttpHeaders.AUTHORIZATION.toLowerCase(Locale.ROOT), "service-identity");

  private static final String REQUEST_ID = "request-id";
  private static final String X_STYX_REQUEST_ID = "X-Styx-Request-Id";

  private Middlewares() {
    throw new UnsupportedOperationException();
  }

  public static Middleware<SyncHandler<? extends Response<?>>, AsyncHandler<Response<ByteString>>>
      json() {
    return innerHandler -> jsonAsync().apply(Middleware.syncToAsync(innerHandler));
  }

  static Middleware<AsyncHandler<? extends Response<?>>, AsyncHandler<Response<ByteString>>>
      jsonAsync() {
    return innerHandler -> innerHandler.map(response -> {
      if (response.payload().isEmpty()) {
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
            INTERNAL_SERVER_ERROR.withReasonPhrase(
                "Failed to serialize response " + e.getMessage()));
      }
    });
  }

  public static <T> Middleware<AsyncHandler<Response<T>>, AsyncHandler<Response<T>>> clientValidator(
      Supplier<Set<String>> supplier) {
    return innerHandler -> requestContext -> {
      if (requestContext.request().header("User-Agent")
          .map(header -> supplier.get().contains(header))
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

  public static <T> Middleware<AsyncHandler<Response<T>>, AsyncHandler<Response<T>>> exceptionAndRequestIdHandler() {
    return innerHandler -> requestContext -> {

      // Accept the request id from the incoming request if present. Otherwise generate one.
      final String requestIdHeader = requestContext.request().headers().get(X_STYX_REQUEST_ID);
      final String requestId = (requestIdHeader != null)
          ? requestIdHeader
          : UUID.randomUUID().toString().replace("-", ""); // UUID with no dashes, easier to deal with

      try (MDC.MDCCloseable ignored = MDCUtil.safePutCloseable(REQUEST_ID, requestId)) {
        return innerHandler.invoke(requestContext).handle((r, t) -> {
          final Response<T> response;
          if (t != null) {
            if (t instanceof ResponseException) {
              response = ((ResponseException) t).getResponse();
            } else {
              var internalServerErrorReason = internalServerErrorReason(requestId, t);
              LOG.warn(internalServerErrorReason, t);
              response = Response.forStatus(INTERNAL_SERVER_ERROR.withReasonPhrase(internalServerErrorReason));
            }
          } else {
            response = r;
          }
          return response.withHeader(X_STYX_REQUEST_ID, requestId);
        });
      } catch (ResponseException e) {
        return completedFuture(e.<T>getResponse()
            .withHeader(X_STYX_REQUEST_ID, requestId));
      } catch (Throwable t) {
        var internalServerErrorReason = internalServerErrorReason(requestId, t);
        LOG.warn(internalServerErrorReason, t);
        return completedFuture(Response.<T>forStatus(INTERNAL_SERVER_ERROR
            .withReasonPhrase(internalServerErrorReason))
            .withHeader(X_STYX_REQUEST_ID, requestId));
      }
    };
  }

  private static String internalServerErrorReason(String requestId, Throwable t) {
    // TODO: returning internal error messages in reason phrase might be a security issue. Make configurable?
    final StringBuilder reason = new StringBuilder(INTERNAL_SERVER_ERROR.reasonPhrase())
        .append(" (").append("Request ID: ").append(requestId).append(")")
        .append(": ").append(t.getClass().getSimpleName())
        .append(": ").append(t.getMessage());
    final Throwable rootCause = Throwables.getRootCause(t);
    if (!t.equals(rootCause)) {
      reason.append(": ").append(rootCause.getClass().getSimpleName())
            .append(": ").append(rootCause.getMessage());
    }
    // Remove any line breaks
    return CharMatcher.anyOf("\n\r").replaceFrom(reason.toString(), ' ');
  }

  static <T> Middleware<AsyncHandler<Response<T>>, AsyncHandler<Response<T>>> httpLogger(
      RequestAuthenticator authenticator) {
    return httpLogger(LOG, authenticator);
  }

  static <T> Middleware<AsyncHandler<Response<T>>, AsyncHandler<Response<T>>> httpLogger(
      Logger log,
      RequestAuthenticator authenticator) {
    return innerHandler -> requestContext -> {
      final Request request = requestContext.request();

      log.info("{}{} {} by {} with headers {} parameters {} and payload {}",
               "GET".equals(request.method()) ? "" : "[AUDIT] ",
               request.method(),
               request.uri(),
          // TODO: pass in auth context instead of authenticating twice
          auth(requestContext, authenticator).user().map(idToken -> idToken.getPayload()
                   .getEmail())
                   .orElse("anonymous"),
               hideSensitiveHeaders(request.headers()),
               request.parameters(),
               request.payload().map(ByteString::utf8).orElse("")
                   .replaceAll("\n", " "));

      return innerHandler.invoke(requestContext);
    };
  }

  public static <T> Middleware<AsyncHandler<Response<T>>, AsyncHandler<Response<T>>> tracer(
      Tracer tracer, String service) {
    return innerHandler -> requestContext -> {
      final Request request = requestContext.request();
      final URI uri = URI.create(request.uri());
      final Span span = tracer.spanBuilder(service + '/' + uri.getPath()).startSpan();
      span.putAttribute("method", stringAttributeValue(request.method()));
      span.putAttribute("uri", stringAttributeValue(request.uri()));
      final CompletionStage<Response<T>> response;
      try  {
        response = innerHandler.invoke(requestContext);
      } catch (Exception e) {
        span.setStatus(UNKNOWN);
        span.end();
        Throwables.throwIfUnchecked(e);
        throw new RuntimeException(e);
      }
      response.whenComplete((r, ex) -> {
        if (ex != null) {
          span.setStatus(UNKNOWN);
        }
        span.end();
      });
      return response;
    };
  }

  @AutoMatter
  public interface AuthContext {

    Optional<GoogleIdToken> user();
  }

  interface Authenticated<T> extends Function<AuthContext, T> {

  }

  interface Requested<T> extends Function<RequestContext, T> {

  }

  public static <T> Middleware<Requested<Authenticated<Response<T>>>, SyncHandler<Response<T>>> authed(
      RequestAuthenticator authenticator) {
    return ar -> rc -> ar
        .apply(rc)
        .apply(auth(rc, authenticator));
  }

  public static <E, R> Middleware<Authenticated<EntityResponseHandler<E, R>>, SyncHandler<Response<ByteString>>> authedEntity(
      Middleware<EntityResponseHandler<E, R>, SyncHandler<Response<ByteString>>> entityMiddleware,
      Middleware<Requested<Authenticated<Response<ByteString>>>, SyncHandler<Response<ByteString>>> authed) {
    return ar -> rc -> authed.apply(r -> ac -> entityMiddleware.apply(ar.apply(ac)).invoke(rc)).invoke(rc);
  }

  public static <E, R> Middleware<Authenticated<EntityResponseHandler<E, R>>, SyncHandler<Response<ByteString>>> authedEntity(
      RequestAuthenticator authenticator,
      Middleware<EntityResponseHandler<E, R>, SyncHandler<Response<ByteString>>> entityMiddleware) {
    return authedEntity(entityMiddleware, authed(authenticator));
  }

  private static AuthContext auth(RequestContext requestContext,
      RequestAuthenticator authenticator) {
    return authenticator.authenticate(requestContext.request());
  }

  private static Map<String, String> hideSensitiveHeaders(Map<String, String> headers) {
    return headers.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey,
            entry -> BLACKLISTED_HEADERS.stream()
                         .anyMatch(header -> entry.getKey().toLowerCase(Locale.ROOT).contains(header))
                     ? "<hidden>"
                     : entry.getValue()));
  }

  public static <T> Middleware<AsyncHandler<Response<T>>, AsyncHandler<Response<T>>> authenticator(
      RequestAuthenticator authenticator) {
    return h -> rc -> {
      final Optional<GoogleIdToken> idToken = auth(rc, authenticator).user();
      if (!"GET".equals(rc.request().method()) && idToken.isEmpty()) {
        return completedFuture(
            Response.forStatus(Status.UNAUTHORIZED.withReasonPhrase("Unauthorized access")));
      }

      return h.invoke(rc);
    };
  }
}
