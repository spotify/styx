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

package com.spotify.styx.api.deprecated;

import static com.spotify.styx.api.Api.Version.V1;
import static com.spotify.styx.util.StreamUtil.cat;
import static java.util.stream.Collectors.toList;

import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.entity.EntityMiddleware;
import com.spotify.apollo.entity.JacksonEntityCodec;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Middleware;
import com.spotify.apollo.route.Route;
import com.spotify.styx.api.Api;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.serialization.Json;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;
import okio.ByteString;

@Deprecated
public final class BackfillResource {

  static final String BASE = "/backfills";

  private final com.spotify.styx.api.BackfillResource backfillResource;

  public BackfillResource(com.spotify.styx.api.BackfillResource backfillResource) {
    this.backfillResource = Objects.requireNonNull(backfillResource);
  }

  public Stream<? extends Route<? extends AsyncHandler<? extends Response<ByteString>>>> routes() {
    final EntityMiddleware em =
        EntityMiddleware.forCodec(JacksonEntityCodec.forMapper(Json.OBJECT_MAPPER));

    final List<Route<AsyncHandler<Response<ByteString>>>> entityRoutes = Stream.of(
        Route.with(
            em.serializerDirect(BackfillsPayload.class),
            "GET", BASE,
            this::getBackfills),
        Route.with(
            em.response(BackfillInput.class, Backfill.class),
            "POST", BASE,
            rc -> this::postBackfill),
        Route.with(
            em.serializerResponse(BackfillPayload.class),
            "GET", BASE + "/<bid>",
            rc -> getBackfill(arg("bid", rc))),
        Route.with(
            em.response(Backfill.class),
            "PUT", BASE + "/<bid>",
            rc -> payload -> updateBackfill(arg("bid", rc), payload))
    )
        .map(r -> r.withMiddleware(Middleware::syncToAsync))
        .collect(toList());

    final List<Route<AsyncHandler<Response<ByteString>>>> routes = Collections.singletonList(
        Route.async(
            "DELETE", BASE + "/<bid>",
            rc -> haltBackfill(arg("bid", rc), rc))
    );

    return cat(
        Api.prefixRoutes(entityRoutes, V1),
        Api.prefixRoutes(routes, V1)
    );
  }

  private BackfillsPayload getBackfills(RequestContext requestContext) {
    // FIXME: adaptor
    return backfillResource.getBackfills(requestContext);
  }

  private Response<BackfillPayload> getBackfill(String id) {
    // FIXME: adaptor
    return backfillResource.getBackfill(id);
  }


  private CompletionStage<Response<ByteString>> haltBackfill(String id, RequestContext rc) {
    // FIXME: adaptor
    return backfillResource.haltBackfill(id, rc);
  }

  private Response<Backfill> postBackfill(BackfillInput input) {
    // FIXME: adaptor
    return backfillResource.postBackfill(input);
  }

  private Response<Backfill> updateBackfill(String id, Backfill backfill) {
    // FIXME: adaptor
    return backfillResource.updateBackfill(id, backfill);
  }

  private static String arg(String name, RequestContext rc) {
    return rc.pathArgs().get(name);
  }
}
