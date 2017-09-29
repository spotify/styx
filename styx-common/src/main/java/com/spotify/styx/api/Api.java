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

import static com.spotify.styx.api.Middlewares.auditLogger;
import static com.spotify.styx.api.Middlewares.clientValidator;
import static com.spotify.styx.api.Middlewares.exceptionHandler;

import com.spotify.apollo.Response;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Route;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;
import okio.ByteString;

public final class Api {

  public enum Version {
    V3;

    public String prefix() {
      return "/api/" + name().toLowerCase();
    }
  }

  public static Stream<Route<AsyncHandler<Response<ByteString>>>> prefixRoutes(
      Collection<Route<AsyncHandler<Response<ByteString>>>> routes,
      Version... versions) {
    return Stream.of(versions)
        .flatMap(v -> routes.stream().map(route -> route.withPrefix(v.prefix())));
  }

  public static Stream<Route<AsyncHandler<Response<ByteString>>>> withCommonMiddleware(
      Stream<Route<AsyncHandler<Response<ByteString>>>> routes) {
    return withCommonMiddleware(routes, Optional::empty);
  }

  public static Stream<Route<AsyncHandler<Response<ByteString>>>> withCommonMiddleware(
      Stream<Route<AsyncHandler<Response<ByteString>>>> routes,
      Supplier<Optional<List<String>>> clientBlacklistSupplier) {
    return routes.map(r -> r
        .withMiddleware(auditLogger())
        .withMiddleware(clientValidator(clientBlacklistSupplier))
        .withMiddleware(exceptionHandler()));
  }
  private Api() {
  }
}
