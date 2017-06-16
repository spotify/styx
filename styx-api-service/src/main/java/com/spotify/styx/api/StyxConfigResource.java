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

import static com.spotify.styx.api.Api.Version.V0;
import static com.spotify.styx.api.Api.Version.V1;
import static com.spotify.styx.api.Api.Version.V2;
import static com.spotify.styx.api.Middlewares.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import com.google.common.base.Throwables;
import com.spotify.apollo.Request;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Route;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import okio.ByteString;

public class StyxConfigResource {

  static final String BASE = "/config";

  private final Storage storage;

  public StyxConfigResource(Storage storage) {
    this.storage = Objects.requireNonNull(storage);
  }

  public Stream<Route<AsyncHandler<Response<ByteString>>>> routes() {
    final List<Route<AsyncHandler<Response<ByteString>>>> routes = Arrays.asList(
        Route.with(
            json(), "GET", BASE,
            rc -> styxConfig()),
        Route.with(
            json(), "PATCH", BASE,
            rc -> patchStyxConfig(rc.request()))
    );

    return Api.prefixRoutes(routes, V0, V1, V2);
  }

  private Response<StyxConfig> styxConfig() {
    final boolean enabled;
    try {
      enabled = storage.globalEnabled();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return Response.forPayload(StyxConfig.create(enabled));
  }

  private Response<StyxConfig> patchStyxConfig(Request request) {
    final Optional<String> enabledParameter = request.parameter("enabled");
    if (!enabledParameter.isPresent()) {
      return Response.forStatus(
          Status.BAD_REQUEST.withReasonPhrase("Missing 'enabled' query parameter"));
    }

    final boolean enabled = Boolean.parseBoolean(enabledParameter.get());

    try {
      storage.setGlobalEnabled(enabled);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }

    return Response.forPayload(StyxConfig.create(enabled));
  }

  @AutoValue
  abstract static class StyxConfig {

    @JsonProperty
    public abstract boolean enabled();

    static StyxConfig create(boolean enabled) {
      return new AutoValue_StyxConfigResource_StyxConfig(enabled);
    }
  }
}
