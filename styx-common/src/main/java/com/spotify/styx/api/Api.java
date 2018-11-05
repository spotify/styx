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

import static com.spotify.styx.api.Middlewares.authenticator;
import static com.spotify.styx.api.Middlewares.clientValidator;
import static com.spotify.styx.api.Middlewares.exceptionAndRequestIdHandler;
import static com.spotify.styx.api.Middlewares.httpLogger;
import static com.spotify.styx.api.Middlewares.tracer;

import com.spotify.apollo.Response;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Route;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import okio.ByteString;

public final class Api {

  private static final Tracer tracer = Tracing.getTracer();

  public enum Version {
    V3;

    public String prefix() {
      return "/api/" + name().toLowerCase();
    }

  }

  private Api() {
    throw new UnsupportedOperationException();
  }

  public static Stream<Route<AsyncHandler<Response<ByteString>>>> prefixRoutes(
      Collection<Route<AsyncHandler<Response<ByteString>>>> routes,
      Version... versions) {
    return Stream.of(versions)
        .flatMap(v -> routes.stream().map(route -> route.withPrefix(v.prefix())));
  }

  public static Stream<Route<AsyncHandler<Response<ByteString>>>> withCommonMiddleware(
      Stream<Route<AsyncHandler<Response<ByteString>>>> routes,
      Authenticator authenticator,
      String service) {
    return withCommonMiddleware(routes, Collections::emptyList, authenticator, service);
  }

  public static Stream<Route<AsyncHandler<Response<ByteString>>>> withCommonMiddleware(
      Stream<Route<AsyncHandler<Response<ByteString>>>> routes,
      Supplier<List<String>> clientBlacklistSupplier,
      Authenticator authenticator,
      String service) {
    return routes.map(r -> r
        .withMiddleware(httpLogger(authenticator))
        .withMiddleware(authenticator(authenticator))
        .withMiddleware(clientValidator(clientBlacklistSupplier))
        .withMiddleware(exceptionAndRequestIdHandler())
        .withMiddleware(tracer(tracer, service)));
  }
}
