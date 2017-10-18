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

package com.spotify.styx;

import static com.spotify.styx.api.Middlewares.authValidator;
import static com.spotify.styx.util.Connections.createBigTableConnection;
import static com.spotify.styx.util.Connections.createDatastore;
import static java.util.Objects.requireNonNull;

import com.google.cloud.datastore.Datastore;
import com.google.common.io.Closer;
import com.spotify.apollo.AppInit;
import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Route;
import com.spotify.styx.api.Api;
import com.spotify.styx.api.BackfillResource;
import com.spotify.styx.api.ResourceResource;
import com.spotify.styx.api.SchedulerProxyResource;
import com.spotify.styx.api.StatusResource;
import com.spotify.styx.api.StyxConfigResource;
import com.spotify.styx.api.WorkflowResource;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.CachedSupplier;
import com.spotify.styx.util.DockerImageValidator;
import com.spotify.styx.util.StorageFactory;
import com.spotify.styx.util.StreamUtil;
import com.typesafe.config.Config;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;
import okio.ByteString;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entrypoint for Styx API Service
 */
public class StyxApi implements AppInit {

  public static final String SERVICE_NAME = "styx-api";

  public static final String SCHEDULER_SERVICE_BASE_URL = "styx.scheduler.base-url";
  public static final String DEFAULT_SCHEDULER_SERVICE_BASE_URL = "http://localhost:8080";

  public static final Duration DEFAULT_RETRY_BASE_DELAY_BT = Duration.ofSeconds(1);

  private static final Logger LOG = LoggerFactory.getLogger(StyxApi.class);

  public static class Builder {

    private StorageFactory storageFactory = StyxApi::storage;

    public Builder setStorageFactory(StorageFactory storageFactory) {
      this.storageFactory = storageFactory;
      return this;
    }

    public StyxApi build() {
      return new StyxApi(storageFactory);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static StyxApi createDefault() {
    return newBuilder().build();
  }

  private final StorageFactory storageFactory;

  private StyxApi(StorageFactory storageFactory) {
    this.storageFactory = requireNonNull(storageFactory);
  }

  @Override
  public void create(Environment environment) {
    final Config config = environment.config();
    final String schedulerServiceBaseUrl = config.hasPath(SCHEDULER_SERVICE_BASE_URL)
                                           ? config.getString(SCHEDULER_SERVICE_BASE_URL)
                                           : DEFAULT_SCHEDULER_SERVICE_BASE_URL;

    final Storage storage = storageFactory.apply(environment);
    
    // N.B. if we need to forward a request to scheduler that behind an nginx, we CAN NOT
    // use rc.requestScopedClient() and at the same time inherit all headers from original
    // request, because request scoped client would add Authorization header again which
    // results duplicated headers, and that would make nginx unhappy.

    final WorkflowResource workflowResource = new WorkflowResource(storage,
                                                                   schedulerServiceBaseUrl,
                                                                   new DockerImageValidator(),
                                                                   environment.client());
    final BackfillResource backfillResource = new BackfillResource(schedulerServiceBaseUrl,
                                                                   storage);
    final ResourceResource resourceResource = new ResourceResource(storage);
    final StyxConfigResource styxConfigResource = new StyxConfigResource(storage);
    final StatusResource statusResource = new StatusResource(storage);
    final SchedulerProxyResource schedulerProxyResource = new SchedulerProxyResource(
        schedulerServiceBaseUrl, environment.client());

    final Supplier<Optional<List<String>>> clientBlacklistSupplier =
        new CachedSupplier<>(storage::clientBlacklist, Instant::now);

    final Stream<Route<AsyncHandler<Response<ByteString>>>> routes = StreamUtil.cat(
        workflowResource.routes(),
        backfillResource.routes(),
        resourceResource.routes(),
        styxConfigResource.routes(),
        statusResource.routes(),
        schedulerProxyResource.routes()
    ).map(r -> r.withMiddleware(authValidator()));

    environment.routingEngine()
        .registerAutoRoute(Route.sync("GET", "/ping", rc -> "pong"))
        .registerRoutes(Api.withCommonMiddleware(routes, clientBlacklistSupplier));
  }

  private static AggregateStorage storage(Environment environment) {
    final Config config = environment.config();
    final Closer closer = environment.closer();

    final Connection bigTable = closer.register(createBigTableConnection(config));
    final Datastore datastore = createDatastore(config);
    return new AggregateStorage(bigTable, datastore, DEFAULT_RETRY_BASE_DELAY_BT);
  }
}
