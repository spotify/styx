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

import static com.spotify.styx.api.Api.Version.V1;
import static com.spotify.styx.api.Api.Version.V2;
import static java.util.stream.Collectors.toList;

import com.google.common.base.Throwables;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.entity.EntityMiddleware;
import com.spotify.apollo.entity.JacksonEntityCodec;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.Route;
import com.spotify.styx.model.Resource;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import okio.ByteString;

public final class ResourceResource {

  static final String BASE = "/resources";

  private final Storage storage;

  public ResourceResource(Storage storage) {
    this.storage = Objects.requireNonNull(storage);
  }

  public Stream<Route<AsyncHandler<Response<ByteString>>>> routes() {
    final EntityMiddleware em =
        EntityMiddleware.forCodec(JacksonEntityCodec.forMapper(Json.OBJECT_MAPPER));

    final List<Route<AsyncHandler<Response<ByteString>>>> routes = Stream.of(
        Route.with(
            em.serializerDirect(ResourcesPayload.class),
            "GET", BASE,
            rc -> getResources()),
        Route.with(
            em.direct(Resource.class),
            "POST", BASE,
            rc -> this::postResource),
        Route.with(
            em.serializerResponse(Resource.class),
            "GET", BASE + "/<rid>",
            rc -> getResource(rc.pathArgs().get("rid"))),
        Route.with(
            em.serializerResponse(Void.class),
            "DELETE", BASE + "/<rid>",
            rc -> deleteResource(rc.pathArgs().get("rid"))),
        Route.with(
            em.response(Resource.class),
            "PUT", BASE + "/<rid>",
            rc -> payload -> updateResource(rc.pathArgs().get("rid"), payload))
    )
        .map(r -> r.withMiddleware(Middleware::syncToAsync))
        .collect(toList());

    return Api.prefixRoutes(routes, V1, V2);
  }

  private ResourcesPayload getResources() {
    try {
      return ResourcesPayload.create(storage.resources());
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private Response<Resource> getResource(String id) {
    try {
      return storage.resource(id).map(Response::forPayload)
          .orElse(Response.forStatus(Status.NOT_FOUND));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private Response<Void> deleteResource(String id) {
    try {
      storage.deleteResource(id);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return Response.forStatus(Status.NO_CONTENT);
  }

  private Resource postResource(Resource resource) {
    try {
      storage.storeResource(resource);
      return resource;
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private Response<Resource> updateResource(String id, Resource resource) {
    if (!resource.id().equals(id)) {
      return Response.forStatus(
          Status.BAD_REQUEST.withReasonPhrase("ID of payload does not match ID in uri."));
    }

    try {
      storage.storeResource(resource);
    } catch (IOException e) {
      return Response
          .forStatus(
              Status.INTERNAL_SERVER_ERROR.withReasonPhrase("Failed to store resource."));
    }

    return Response.forStatus(Status.OK).withPayload(resource);
  }
}
