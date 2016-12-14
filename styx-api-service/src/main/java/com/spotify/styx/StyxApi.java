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

import static com.spotify.styx.util.Connections.createBigTableConnection;
import static com.spotify.styx.util.Connections.createDatastore;
import static java.util.Objects.requireNonNull;

import com.google.cloud.datastore.Datastore;
import com.google.common.io.Closer;
import com.spotify.apollo.AppInit;
import com.spotify.apollo.Environment;
import com.spotify.apollo.route.Route;
import com.spotify.styx.api.CliResource;
import com.spotify.styx.api.ResourceResource;
import com.spotify.styx.api.StyxConfigResource;
import com.spotify.styx.api.WorkflowResource;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.EventStorage;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.EventStorageFactory;
import com.spotify.styx.util.Singleton;
import com.spotify.styx.util.StorageFactory;
import com.typesafe.config.Config;
import java.time.Duration;
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

    private final Singleton<AggregateStorage> storage = Singleton.create(StyxApi::storage);

    private StorageFactory storageFactory = storage::apply;
    private EventStorageFactory eventStorageFactory = storage::apply;

    public Builder setStorageFactory(StorageFactory storageFactory) {
      this.storageFactory = storageFactory;
      return this;
    }

    public Builder setEventStorageFactory(EventStorageFactory eventStorageFactory) {
      this.eventStorageFactory = eventStorageFactory;
      return this;
    }

    public StyxApi build() {
      return new StyxApi(storageFactory, eventStorageFactory);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static StyxApi createDefault() {
    return newBuilder().build();
  }

  private final StorageFactory storageFactory;
  private final EventStorageFactory eventStorageFactory;

  private StyxApi(StorageFactory storageFactory, EventStorageFactory eventStorageFactory) {
    this.storageFactory = requireNonNull(storageFactory);
    this.eventStorageFactory = requireNonNull(eventStorageFactory);
  }

  @Override
  public void create(Environment environment) {
    final Config config = environment.config();
    final String schedulerServiceBaseUrl = config.hasPath(SCHEDULER_SERVICE_BASE_URL)
        ? config.getString(SCHEDULER_SERVICE_BASE_URL)
        : DEFAULT_SCHEDULER_SERVICE_BASE_URL;

    final Storage storage = storageFactory.apply(environment);
    final EventStorage eventStorage = eventStorageFactory.apply(environment);

    final WorkflowResource workflowResource = new WorkflowResource(storage);
    final ResourceResource resourceResource = new ResourceResource(storage);
    final StyxConfigResource styxConfigResource = new StyxConfigResource(storage);
    final CliResource cliResource = new CliResource(schedulerServiceBaseUrl, eventStorage);

    environment.routingEngine()
        .registerAutoRoute(Route.sync("GET", "/ping", rc -> "pong"))
        .registerRoutes(workflowResource.routes())
        .registerRoutes(resourceResource.routes())
        .registerRoutes(styxConfigResource.routes())
        .registerRoutes(cliResource.routes());
  }

  private static AggregateStorage storage(Environment environment) {
    final Config config = environment.config();
    final Closer closer = environment.closer();

    final Connection bigTable = closer.register(createBigTableConnection(config));
    final Datastore datastore = createDatastore(config);
    return new AggregateStorage(bigTable, datastore, DEFAULT_RETRY_BASE_DELAY_BT);
  }
}
