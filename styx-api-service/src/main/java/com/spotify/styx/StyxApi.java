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
import com.spotify.apollo.Response;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.apollo.route.Route;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.styx.api.Api;
import com.spotify.styx.api.BackfillResource;
import com.spotify.styx.api.ResourceResource;
import com.spotify.styx.api.SchedulerProxyResource;
import com.spotify.styx.api.StatusResource;
import com.spotify.styx.api.WorkflowResource;
import com.spotify.styx.api.workflow.WorkflowInitializer;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.monitoring.MeteredStorageProxy;
import com.spotify.styx.monitoring.MetricsStats;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.monitoring.StatsFactory;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.CachedSupplier;
import com.spotify.styx.util.DockerImageValidator;
import com.spotify.styx.util.ShardedCounter;
import com.spotify.styx.util.ShardedCounterSnapshotFactory;
import com.spotify.styx.util.StorageFactory;
import com.spotify.styx.util.StreamUtil;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.WorkflowValidator;
import com.typesafe.config.Config;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Stream;
import okio.ByteString;
import org.apache.hadoop.hbase.client.Connection;

/**
 * Main entrypoint for Styx API Service
 */
public class StyxApi implements AppInit {

  public static final String SERVICE_NAME = "styx-api";

  public static final String SCHEDULER_SERVICE_BASE_URL = "styx.scheduler.base-url";
  public static final String DEFAULT_SCHEDULER_SERVICE_BASE_URL = "http://localhost:8080";

  public static final Duration DEFAULT_RETRY_BASE_DELAY_BT = Duration.ofSeconds(1);

  private final String serviceName;
  private final StorageFactory storageFactory;
  private final WorkflowConsumerFactory workflowConsumerFactory;
  private final StatsFactory statsFactory;
  private final Time time;

  public interface WorkflowConsumerFactory
      extends BiFunction<Environment, Stats, BiConsumer<Optional<Workflow>, Optional<Workflow>>> { }

  public static class Builder {

    private String serviceName = "styx-api";
    private StorageFactory storageFactory = StyxApi::storage;
    private WorkflowConsumerFactory workflowConsumerFactory = (env, stats) -> (oldWorkflow, newWorkflow) -> { };
    private StatsFactory statsFactory = StyxApi::stats;
    private Time time = Instant::now;

    public Builder setServiceName(String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    public Builder setStorageFactory(StorageFactory storageFactory) {
      this.storageFactory = storageFactory;
      return this;
    }

    public Builder setWorkflowConsumerFactory(WorkflowConsumerFactory workflowConsumerFactory) {
      this.workflowConsumerFactory = workflowConsumerFactory;
      return this;
    }

    public Builder setStatsFactory(StatsFactory statsFactory) {
      this.statsFactory = statsFactory;
      return this;
    }

    public Builder setTime(Time time) {
      this.time = time;
      return this;
    }

    public StyxApi build() {
      return new StyxApi(this);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static StyxApi createDefault() {
    return newBuilder().build();
  }

  private StyxApi(Builder builder) {
    this.serviceName = requireNonNull(builder.serviceName);
    this.storageFactory = requireNonNull(builder.storageFactory);
    this.workflowConsumerFactory = requireNonNull(builder.workflowConsumerFactory);
    this.statsFactory = requireNonNull(builder.statsFactory);
    this.time = requireNonNull(builder.time);
  }

  @Override
  public void create(Environment environment) {
    final Config config = environment.config();
    final String schedulerServiceBaseUrl = config.hasPath(SCHEDULER_SERVICE_BASE_URL)
                                           ? config.getString(SCHEDULER_SERVICE_BASE_URL)
                                           : DEFAULT_SCHEDULER_SERVICE_BASE_URL;

    final Stats stats = statsFactory.apply(environment);
    final Storage storage = MeteredStorageProxy.instrument(storageFactory.apply(environment, stats), stats, time);
    final BiConsumer<Optional<Workflow>, Optional<Workflow>> workflowConsumer =
        workflowConsumerFactory.apply(environment, stats);
    
    // N.B. if we need to forward a request to scheduler that behind an nginx, we CAN NOT
    // use rc.requestScopedClient() and at the same time inherit all headers from original
    // request, because request scoped client would add Authorization header again which
    // results duplicated headers, and that would make nginx unhappy. This has been fixed
    // in later Apollo version.

    final WorkflowResource workflowResource = new WorkflowResource(storage,
        new WorkflowValidator(new DockerImageValidator()),
        new WorkflowInitializer(storage, time),
        workflowConsumer);

    final BackfillResource backfillResource = new BackfillResource(schedulerServiceBaseUrl,
        storage,
        new WorkflowValidator(new DockerImageValidator()));
    environment.closer().register(backfillResource);

    final ShardedCounter shardedCounter = new ShardedCounter(storage, stats, new ShardedCounterSnapshotFactory(storage));
    final ResourceResource resourceResource = new ResourceResource(storage, shardedCounter);
    final StatusResource statusResource = new StatusResource(storage);
    final SchedulerProxyResource schedulerProxyResource = new SchedulerProxyResource(
        schedulerServiceBaseUrl, environment.client());

    final Supplier<StyxConfig> configSupplier =
        new CachedSupplier<>(storage::config, Instant::now);
    final Supplier<List<String>> clientBlacklistSupplier =
        () -> configSupplier.get().clientBlacklist();

    final Stream<Route<AsyncHandler<Response<ByteString>>>> routes = StreamUtil.cat(
        workflowResource.routes(),
        backfillResource.routes(),
        resourceResource.routes(),
        statusResource.routes(),
        schedulerProxyResource.routes()
    );

    environment.routingEngine()
        .registerAutoRoute(Route.sync("GET", "/ping", rc -> "pong"))
        .registerRoutes(Api.withCommonMiddleware(routes, clientBlacklistSupplier, serviceName));
  }

  private static AggregateStorage storage(Environment environment, Stats stats) {
    final Config config = environment.config();
    final Closer closer = environment.closer();

    final Connection bigTable = closer.register(createBigTableConnection(config));
    final Datastore datastore = createDatastore(config, stats);
    return new AggregateStorage(bigTable, datastore, DEFAULT_RETRY_BASE_DELAY_BT);
  }

  private static Stats stats(Environment environment) {
    return new MetricsStats(environment.resolve(SemanticMetricRegistry.class), Instant::now);
  }
}
