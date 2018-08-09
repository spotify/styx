/*-
 * -\-\-
 * Spotify Styx Scheduler Service
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

import static com.spotify.apollo.environment.ConfigUtil.optionalInt;
import static com.spotify.styx.state.OutputHandler.fanOutput;
import static com.spotify.styx.state.StateUtil.getResourcesUsageMap;
import static com.spotify.styx.util.Connections.createBigTableConnection;
import static com.spotify.styx.util.Connections.createDatastore;
import static com.spotify.styx.util.GuardedRunnable.runGuarded;
import static com.spotify.styx.util.ShardedCounter.NUM_SHARDS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Gauge;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.container.v1beta1.Container;
import com.google.api.services.container.v1beta1.ContainerScopes;
import com.google.api.services.container.v1beta1.model.Cluster;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.IamScopes;
import com.google.cloud.datastore.Datastore;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.apollo.AppInit;
import com.spotify.apollo.Environment;
import com.spotify.apollo.route.Route;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.styx.api.Api;
import com.spotify.styx.api.SchedulerResource;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.MeteredDockerRunnerProxy;
import com.spotify.styx.monitoring.MeteredStorageProxy;
import com.spotify.styx.monitoring.MetricsStats;
import com.spotify.styx.monitoring.MonitoringHandler;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.monitoring.StatsFactory;
import com.spotify.styx.publisher.Publisher;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.QueuedStateManager;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.TimeoutConfig;
import com.spotify.styx.state.handlers.DockerRunnerHandler;
import com.spotify.styx.state.handlers.ExecutionDescriptionHandler;
import com.spotify.styx.state.handlers.PublisherHandler;
import com.spotify.styx.state.handlers.TerminationHandler;
import com.spotify.styx.state.handlers.TransitionLogger;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.InMemStorage;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.CachedSupplier;
import com.spotify.styx.util.CounterSnapshotFactory;
import com.spotify.styx.util.Debug;
import com.spotify.styx.util.DockerImageValidator;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.RetryUtil;
import com.spotify.styx.util.Shard;
import com.spotify.styx.util.ShardedCounter;
import com.spotify.styx.util.ShardedCounterSnapshotFactory;
import com.spotify.styx.util.StorageFactory;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerUtil;
import com.spotify.styx.util.WorkflowValidator;
import com.typesafe.config.Config;
import eu.javaspecialists.tjsn.concurrency.stripedexecutor.StripedExecutorService;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.opencensus.common.Scope;
import io.opencensus.trace.Tracer;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.samplers.Samplers;
import java.io.Closeable;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StyxScheduler implements AppInit {

  public static final String SERVICE_NAME = "styx-scheduler";

  public static final String GKE_CLUSTER_PATH = "styx.gke";
  public static final String GKE_CLUSTER_PROJECT_ID = "project-id";
  public static final String GKE_CLUSTER_ZONE = "cluster-zone";
  public static final String GKE_CLUSTER_ID = "cluster-id";
  public static final String GKE_CLUSTER_NAMESPACE = "namespace";

  public static final String STYX_STALE_STATE_TTL_CONFIG = "styx.stale-state-ttls";
  public static final String STYX_MODE = "styx.mode";
  public static final String STYX_MODE_DEVELOPMENT = "development";
  public static final String STYX_EVENT_PROCESSING_THREADS = "styx.event-processing-threads";
  public static final String STYX_SCHEDULER_TICK_INTERVAL = "styx.scheduler.tick-interval";
  public static final String STYX_TRIGGER_TICK_INTERVAL = "styx.trigger.tick-interval";
  public static final String STYX_SCHEDULER_THREADS = "styx.scheduler-threads";
  private static final String STYX_ENVIRONMENT = "styx.environment";
  private static final String STYX_ENVIRONMENT_PRODUCTION = "production";

  public static final int DEFAULT_STYX_EVENT_PROCESSING_THREADS = 32;
  public static final int DEFAULT_STYX_SCHEDULER_THREADS = 32;
  public static final Duration DEFAULT_SCHEDULER_TICK_INTERVAL = Duration.ofSeconds(2);
  public static final Duration DEFAULT_TRIGGER_TICK_INTERVAL = Duration.ofSeconds(1);
  public static final Duration CLEANER_TICK_INTERVAL = Duration.ofMinutes(30);
  public static final Duration RUNTIME_CONFIG_UPDATE_INTERVAL = Duration.ofSeconds(5);
  public static final Duration DEFAULT_RETRY_BASE_DELAY = Duration.ofMinutes(3);
  public static final int DEFAULT_RETRY_MAX_EXPONENT = 4;
  public static final Duration DEFAULT_RETRY_BASE_DELAY_BT = Duration.ofSeconds(1);
  public static final RetryUtil DEFAULT_RETRY_UTIL =
      new RetryUtil(DEFAULT_RETRY_BASE_DELAY, DEFAULT_RETRY_MAX_EXPONENT);
  public static final double DEFAULT_SUBMISSION_RATE_PER_SEC = 1000D;

  private static final Logger LOG = LoggerFactory.getLogger(StyxScheduler.class);

  private static final Tracer tracer = Tracing.getTracer();

  private final String serviceName;
  private final Time time;
  private final StorageFactory storageFactory;
  private final DockerRunnerFactory dockerRunnerFactory;
  private final StatsFactory statsFactory;
  private final ExecutorFactory executorFactory;
  private final PublisherFactory publisherFactory;
  private final RetryUtil retryUtil;
  private final WorkflowResourceDecorator resourceDecorator;
  private final EventConsumerFactory eventConsumerFactory;
  private final WorkflowExecutionGateFactory executionGateFactory;

  private StateManager stateManager;
  private Scheduler scheduler;
  private TriggerManager triggerManager;
  private BackfillTriggerManager backfillTriggerManager;

  // === Type aliases for dependency injectors ====================================================
  public interface PublisherFactory extends Function<Environment, Publisher> { }
  public interface EventConsumerFactory extends BiFunction<Environment, Stats, BiConsumer<SequenceEvent, RunState>> { }
  public interface WorkflowExecutionGateFactory extends BiFunction<Environment, Storage, WorkflowExecutionGate> { }

  @FunctionalInterface
  interface DockerRunnerFactory {
    DockerRunner create(
        String id,
        Environment environment,
        StateManager stateManager,
        ScheduledExecutorService scheduler,
        Stats stats,
        Debug debug);
  }

  @FunctionalInterface
  interface ExecutorFactory {
    ScheduledExecutorService create(
        int threads,
        ThreadFactory threadFactory);
  }

  public static class Builder {

    private String serviceName = "styx-scheduler";
    private Time time = Instant::now;
    private StorageFactory storageFactory = storage(StyxScheduler::storage);
    private DockerRunnerFactory dockerRunnerFactory = StyxScheduler::createDockerRunner;
    private StatsFactory statsFactory = StyxScheduler::stats;
    private ExecutorFactory executorFactory = Executors::newScheduledThreadPool;
    private PublisherFactory publisherFactory = (env) -> Publisher.NOOP;
    private RetryUtil retryUtil = DEFAULT_RETRY_UTIL;
    private WorkflowResourceDecorator resourceDecorator = WorkflowResourceDecorator.NOOP;
    private EventConsumerFactory eventConsumerFactory = (env, stats) -> (event, state) -> { };
    private WorkflowExecutionGateFactory executionGateFactory = (env, storage) -> WorkflowExecutionGate.NOOP;

    public Builder setServiceName(String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    public Builder setTime(Time time) {
      this.time = time;
      return this;
    }

    public Builder setStorageFactory(StorageFactory storageFactory) {
      this.storageFactory = storageFactory;
      return this;
    }

    public Builder setDockerRunnerFactory(DockerRunnerFactory dockerRunnerFactory) {
      this.dockerRunnerFactory = dockerRunnerFactory;
      return this;
    }

    public Builder setStatsFactory(StatsFactory statsFactory) {
      this.statsFactory = statsFactory;
      return this;
    }

    public Builder setExecutorFactory(ExecutorFactory executorFactory) {
      this.executorFactory = executorFactory;
      return this;
    }

    public Builder setPublisherFactory(PublisherFactory publisherFactory) {
      this.publisherFactory = publisherFactory;
      return this;
    }

    public Builder setRetryUtil(RetryUtil retryUtil) {
      this.retryUtil = retryUtil;
      return this;
    }

    public Builder setResourceDecorator(WorkflowResourceDecorator resourceDecorator) {
      this.resourceDecorator = resourceDecorator;
      return this;
    }

    public Builder setEventConsumerFactory(EventConsumerFactory eventConsumerFactory) {
      this.eventConsumerFactory = eventConsumerFactory;
      return this;
    }

    public Builder setExecutionGateFactory(WorkflowExecutionGateFactory executionGateFactory) {
      this.executionGateFactory = executionGateFactory;
      return this;
    }

    public StyxScheduler build() {
      return new StyxScheduler(this);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static StyxScheduler createDefault() {
    return newBuilder().build();
  }

  // ==============================================================================================

  private StyxScheduler(Builder builder) {
    this.serviceName = requireNonNull(builder.serviceName);
    this.time = requireNonNull(builder.time);
    this.storageFactory = requireNonNull(builder.storageFactory);
    this.dockerRunnerFactory = requireNonNull(builder.dockerRunnerFactory);
    this.statsFactory = requireNonNull(builder.statsFactory);
    this.executorFactory = requireNonNull(builder.executorFactory);
    this.publisherFactory = requireNonNull(builder.publisherFactory);
    this.retryUtil = requireNonNull(builder.retryUtil);
    this.resourceDecorator = requireNonNull(builder.resourceDecorator);
    this.eventConsumerFactory = requireNonNull(builder.eventConsumerFactory);
    this.executionGateFactory = requireNonNull(builder.executionGateFactory);
  }

  @Override
  public void create(Environment environment) {
    final Config config = environment.config();
    final Closer closer = environment.closer();

    final Thread.UncaughtExceptionHandler uncaughtExceptionHandler =
        (thread, throwable) -> LOG.error("Thread {} threw {}", thread, throwable);

    final ThreadFactory tickTf = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("styx-tick-%d")
        .setUncaughtExceptionHandler(uncaughtExceptionHandler)
        .build();

    final Publisher publisher = publisherFactory.apply(environment);
    closer.register(publisher);

    final ScheduledExecutorService tickExecutor = executorFactory.create(3, tickTf);
    closer.register(executorCloser("tick-executor", tickExecutor));
    final StripedExecutorService eventProcessingExecutor = new StripedExecutorService(
        optionalInt(config, STYX_EVENT_PROCESSING_THREADS).orElse(DEFAULT_STYX_EVENT_PROCESSING_THREADS));
    closer.register(executorCloser("event-processing", eventProcessingExecutor));
    final ExecutorService eventConsumerExecutor = Executors.newSingleThreadExecutor();
    closer.register(executorCloser("event-consumer", eventConsumerExecutor));
    final ExecutorService schedulerExecutor = Executors.newWorkStealingPool(
        optionalInt(config, STYX_SCHEDULER_THREADS).orElse(DEFAULT_STYX_SCHEDULER_THREADS));
    closer.register(executorCloser("scheduler", schedulerExecutor));

    final Stats stats = statsFactory.apply(environment);
    final Storage storage = MeteredStorageProxy.instrument(storageFactory.apply(environment, stats), stats, time);
    closer.register(storage);

    final CounterSnapshotFactory counterSnapshotFactory = new ShardedCounterSnapshotFactory(storage);
    final ShardedCounter shardedCounter = new ShardedCounter(storage, counterSnapshotFactory);

    final Config staleStateTtlConfig = config.getConfig(STYX_STALE_STATE_TTL_CONFIG);
    final TimeoutConfig timeoutConfig = TimeoutConfig.createFromConfig(staleStateTtlConfig);

    final Supplier<Map<WorkflowId, Workflow>> workflowCache = new CachedSupplier<>(storage::workflows, time);

    initializeResources(storage, shardedCounter, counterSnapshotFactory, timeoutConfig, workflowCache);

    // TODO: hack to get around circular reference. Change OutputHandler.transitionInto() to
    //       take StateManager as argument instead?
    final List<OutputHandler> outputHandlers = new ArrayList<>();
    final QueuedStateManager stateManager = closer.register(
        new QueuedStateManager(time, eventProcessingExecutor, storage,
            eventConsumerFactory.apply(environment, stats), eventConsumerExecutor,
            fanOutput(outputHandlers), shardedCounter));

    final Supplier<StyxConfig> styxConfig = new CachedSupplier<>(storage::config, time);
    final Supplier<String> dockerId = () -> styxConfig.get().globalDockerRunnerId();
    final Debug debug = () -> styxConfig.get().debugEnabled();
    final DockerRunner routingDockerRunner = DockerRunner.routing(
        id -> dockerRunnerFactory.create(id, environment, stateManager, tickExecutor, stats, debug),
        dockerId);
    final DockerRunner dockerRunner = MeteredDockerRunnerProxy.instrument(routingDockerRunner, stats, time);

    final RateLimiter dequeueRateLimiter = RateLimiter.create(DEFAULT_SUBMISSION_RATE_PER_SEC);

    outputHandlers.addAll(ImmutableList.of(
        new TransitionLogger(""),
        new DockerRunnerHandler(
            dockerRunner, stateManager),
        new TerminationHandler(retryUtil, stateManager),
        new MonitoringHandler(stats),
        new PublisherHandler(publisher),
        new ExecutionDescriptionHandler(storage, stateManager, new WorkflowValidator(new DockerImageValidator()))));

    final TriggerListener trigger =
        new StateInitializingTrigger(stateManager);
    final TriggerManager triggerManager = new TriggerManager(trigger, time, storage, stats);
    closer.register(triggerManager);

    final BackfillTriggerManager backfillTriggerManager =
        new BackfillTriggerManager(stateManager, storage, trigger, stats, time);

    final Scheduler scheduler = new Scheduler(time, timeoutConfig, stateManager, storage, resourceDecorator, stats,
        dequeueRateLimiter, executionGateFactory.apply(environment, storage), shardedCounter, schedulerExecutor);

    final Cleaner cleaner = new Cleaner(dockerRunner);

    final Duration schedulerTickInterval = config.hasPath(STYX_SCHEDULER_TICK_INTERVAL)
        ? config.getDuration(STYX_SCHEDULER_TICK_INTERVAL)
        : DEFAULT_SCHEDULER_TICK_INTERVAL;

    final Duration triggerTickInterval = config.hasPath(STYX_TRIGGER_TICK_INTERVAL)
        ? config.getDuration(STYX_TRIGGER_TICK_INTERVAL)
        : DEFAULT_TRIGGER_TICK_INTERVAL;

    dockerRunner.restore();
    startTriggerManager(triggerManager, tickExecutor, triggerTickInterval);
    startBackfillTriggerManager(backfillTriggerManager, tickExecutor, triggerTickInterval);
    startScheduler(scheduler, tickExecutor, schedulerTickInterval);
    startRuntimeConfigUpdate(styxConfig, tickExecutor, dequeueRateLimiter);
    startCleaner(cleaner, tickExecutor);

    setupMetrics(stateManager, workflowCache, storage, dequeueRateLimiter, stats);

    final SchedulerResource schedulerResource =
        new SchedulerResource(stateManager, trigger, storage, time,
            new WorkflowValidator(new DockerImageValidator()));

    environment.routingEngine()
        .registerAutoRoute(Route.sync("GET", "/ping", rc -> "pong"))
        .registerRoutes(Api.withCommonMiddleware(schedulerResource.routes(), serviceName));

    this.stateManager = stateManager;
    this.scheduler = scheduler;
    this.triggerManager = triggerManager;
    this.backfillTriggerManager = backfillTriggerManager;
  }

  @VisibleForTesting
  CompletionStage<Void> receive(Event event) throws IsClosedException {
    return stateManager.receive(event);
  }

  @VisibleForTesting
  Optional<RunState> getState(WorkflowInstance workflowInstance) {
    return stateManager.getActiveState(workflowInstance);
  }

  @VisibleForTesting
  void tickScheduler() {
    scheduler.tick();
  }

  @VisibleForTesting
  void tickTriggerManager() {
    triggerManager.tick();
  }

  @VisibleForTesting
  void tickBackfillTriggerManager() {
    backfillTriggerManager.tick();
  }

  private void initializeResources(Storage storage, ShardedCounter shardedCounter,
                                   CounterSnapshotFactory counterSnapshotFactory,
                                   TimeoutConfig timeoutConfig,
                                   Supplier<Map<WorkflowId, Workflow>> workflowCache) {
    try {
      // Initialize resources
      storage.resources().parallelStream().forEach(
          resource -> {
            counterSnapshotFactory.create(resource.id());
            try {
              storage.runInTransaction(tx -> {
                shardedCounter.updateLimit(tx, resource.id(), resource.concurrency());
                return null;
              });
            } catch (IOException e) {
              LOG.error("Error creating a counter limit for {}", resource, e);
              throw new RuntimeException(e);
            }
          });
      LOG.info("Finished initializing resources");

      // Sync resources usage
      if (storage.config().resourcesSyncEnabled()) {
        // first we reset all shards for each resource
        storage.resources().parallelStream().forEach(resource -> resetShards(storage, resource));
        // then we update shards with actual usage
        try {
          final Map<String, Long> resourcesUsageMap = getResourcesUsageMap(storage, timeoutConfig,
              workflowCache, time.get(), resourceDecorator);
          updateShards(storage, resourcesUsageMap);
        } catch (Exception e) {
          LOG.error("Error syncing resources", e);
          throw new RuntimeException(e);
        }
        LOG.info("Finished syncing resources");
      }
    } catch (IOException e) {
      LOG.error("Error while initializing/syncing resources", e);
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  void resetShards(final Storage storage, final Resource resource) {
    LOG.info("Resetting shards of resource {}", resource.id());
    for (int i = 0; i < NUM_SHARDS; i++) {
      final int index = i;
      try {
        storage.runInTransaction(tx -> {
          tx.store(Shard.create(resource.id(), index, 0));
          return null;
        });
      } catch (IOException e) {
        LOG.error("Error resetting shards of resource {}", resource, e);
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * For each resource, distribute usage value evenly across shards.
   * For example, with a usage value of 10 and 3 available shards, the latter will be set to:
   * Shard 1 = 4
   * Shard 2 = 3
   * Shard 3 = 3
   */
  @VisibleForTesting
  void updateShards(final Storage storage,
                    final Map<String, Long> resourceUsage) {
    resourceUsage.entrySet().parallelStream().forEach(entity -> {
      final String resource = entity.getKey();
      final Long usage = entity.getValue();
      LOG.info("Syncing {} -> {}", resource, usage);
      try {
        for (int i = 0; i < NUM_SHARDS; i++) {
          final int index = i;
          final int shardValue = (int) (usage / NUM_SHARDS + (index < usage % NUM_SHARDS ? 1 : 0));
          storage.runInTransaction(tx -> {
            tx.store(Shard.create(resource, index, shardValue));
            return null;
          });
          LOG.info("Stored {}#shard-{} -> {}", resource, index, shardValue);
        }
      } catch (IOException e) {
        LOG.error("Error syncing resource: {}", resource, e);
        throw new RuntimeException(e);
      }
    });
  }

  private static void startCleaner(Cleaner cleaner, ScheduledExecutorService exec) {
    scheduleWithJitter(cleaner::tick, exec, CLEANER_TICK_INTERVAL);
  }

  private static void startTriggerManager(TriggerManager triggerManager, ScheduledExecutorService exec,
      Duration tickInterval) {
    scheduleWithJitter(triggerManager::tick, exec, tickInterval);
  }

  private static void startBackfillTriggerManager(BackfillTriggerManager backfillTriggerManager,
      ScheduledExecutorService exec, Duration tickInterval) {
    scheduleWithJitter(backfillTriggerManager::tick, exec, tickInterval);
  }

  private static void startScheduler(Scheduler scheduler, ScheduledExecutorService exec, Duration tickInterval) {
    scheduleWithJitter(scheduler::tick, exec, tickInterval);
  }

  private static void startRuntimeConfigUpdate(Supplier<StyxConfig> config, ScheduledExecutorService exec,
      RateLimiter submissionRateLimiter) {
    scheduleWithJitter(() -> updateRuntimeConfig(config, submissionRateLimiter), exec, RUNTIME_CONFIG_UPDATE_INTERVAL);
  }

  private static void updateRuntimeConfig(Supplier<StyxConfig> config, RateLimiter rateLimiter) {
    try (Scope ss = tracer.spanBuilder("Styx.StyxScheduler.updateRuntimeConfig")
        .setRecordEvents(true)
        .setSampler(Samplers.alwaysSample())
        .startScopedSpan()) {
      updateRuntimeConfig0(config, rateLimiter);
    }
  }

  private static void updateRuntimeConfig0(Supplier<StyxConfig> config, RateLimiter rateLimiter) {
    try {
      double currentRate = rateLimiter.getRate();
      Double updatedRate = config.get().submissionRateLimit().orElse(
          StyxScheduler.DEFAULT_SUBMISSION_RATE_PER_SEC);
      if (Math.abs(updatedRate - currentRate) >= 0.1) {
        LOG.info("Updating submission rate limit: {} -> {}", currentRate, updatedRate);
        rateLimiter.setRate(updatedRate);
      }
    } catch (Exception e) {
      LOG.warn("Failed to fetch the submission rate config from storage, "
          + "skipping RateLimiter update", e);
    }
  }

  private static void scheduleWithJitter(Runnable runnable, ScheduledExecutorService exec, Duration tickInterval) {
    final double jitter = ThreadLocalRandom.current().nextDouble(0.5, 1.5);
    final long delayMillis = (long) (jitter * tickInterval.toMillis());
    exec.schedule(() -> {
      runGuarded(runnable);
      scheduleWithJitter(runnable, exec, tickInterval);
    }, delayMillis, MILLISECONDS);
  }

  private void setupMetrics(
      QueuedStateManager stateManager,
      Supplier<Map<WorkflowId, Workflow>> workflowCache,
      Storage storage,
      RateLimiter submissionRateLimiter,
      Stats stats) {

    stats.registerQueuedEventsMetric(stateManager::queuedEvents);

    stats.registerWorkflowCountMetric("all", () -> (long) workflowCache.get().size());

    stats.registerWorkflowCountMetric("configured", () -> workflowCache.get().values()
        .stream()
        .filter(workflow -> workflow.configuration().dockerImage().isPresent())
        .count());

    final Supplier<Gauge<Long>> configuredEnabledWorkflowsCountGaugeSupplier = () -> {
      final Supplier<Set<WorkflowId>> enabledWorkflowSupplier =
          new CachedSupplier<>(storage::enabled, Instant::now);
      return () -> workflowCache.get().values()
          .stream()
          .filter(workflow -> workflow.configuration().dockerImage().isPresent())
          .filter(workflow -> enabledWorkflowSupplier.get().contains(WorkflowId.ofWorkflow(workflow)))
          .count();
    };
    stats.registerWorkflowCountMetric("enabled", configuredEnabledWorkflowsCountGaugeSupplier.get());

    stats.registerWorkflowCountMetric("docker_termination_logging_enabled", () ->
        workflowCache.get().values()
            .stream()
            .filter(workflow -> workflow.configuration().dockerImage().isPresent())
            .filter(workflow -> workflow.configuration().dockerTerminationLogging())
            .count());

    Arrays.stream(RunState.State.values()).forEach(state -> {
      TriggerUtil.triggerTypesList().forEach(triggerType ->
          stats.registerActiveStatesMetric(
              state,
              triggerType,
              () -> stateManager.getActiveStates().values().stream()
                  .filter(runState -> runState.state().equals(state))
                  .filter(runState -> runState.data().trigger().isPresent() && triggerType
                      .equals(TriggerUtil.triggerType(runState.data().trigger().get())))
                  .count()));
      stats.registerActiveStatesMetric(
          state,
          "none",
          () -> stateManager.getActiveStates().values().stream()
              .filter(runState -> runState.state().equals(state))
              .filter(runState -> !runState.data().trigger().isPresent())
              .count());
    });

    stats.registerSubmissionRateLimitMetric(submissionRateLimiter::getRate);
  }

  private static Stats stats(Environment environment) {
    return new MetricsStats(environment.resolve(SemanticMetricRegistry.class), Instant::now);
  }

  private static StorageFactory storage(StorageFactory storage) {
    return (environment, stats) -> {
      if (isDevMode(environment.config())) {
        LOG.info("Running Styx in development mode, will use InMemStorage");
        return new InMemStorage();
      } else {
        return storage.apply(environment, stats);
      }
    };
  }

  private static AggregateStorage storage(Environment environment, Stats stats  ) {
    final Config config = environment.config();
    final Closer closer = environment.closer();

    final Connection bigTable = closer.register(createBigTableConnection(config));
    final Datastore datastore = createDatastore(config, stats);
    return new AggregateStorage(bigTable, datastore, DEFAULT_RETRY_BASE_DELAY_BT);
  }

  private static DockerRunner createDockerRunner(
      String id,
      Environment environment,
      StateManager stateManager,
      ScheduledExecutorService scheduler,
      Stats stats,
      Debug debug) {
    final Config config = environment.config();
    final Closer closer = environment.closer();

    if (isDevMode(config)) {
      LOG.info("Creating LocalDockerRunner");
      return closer.register(DockerRunner.local(scheduler, stateManager));
    } else {
      final String styxEnvironment;
      if (!config.hasPath(STYX_ENVIRONMENT)) {
        styxEnvironment = STYX_ENVIRONMENT_PRODUCTION;
      } else {
        styxEnvironment = config.getString(STYX_ENVIRONMENT);
      }
      final NamespacedKubernetesClient kubernetes = closer.register(getKubernetesClient(
          config, id, createGkeClient(), DefaultKubernetesClient::new));
      final ServiceAccountKeyManager serviceAccountKeyManager = createServiceAccountKeyManager();
      return closer.register(DockerRunner.kubernetes(kubernetes, stateManager, stats,
          serviceAccountKeyManager, debug, styxEnvironment));
    }
  }

  private static Container createGkeClient() {
    try {
      final HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      final JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
      final GoogleCredential credential =
          GoogleCredential.getApplicationDefault(httpTransport, jsonFactory)
              .createScoped(ContainerScopes.all());
      return new Container.Builder(httpTransport, jsonFactory, credential)
          .setApplicationName(SERVICE_NAME)
          .build();
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static ServiceAccountKeyManager createServiceAccountKeyManager() {
    try {
      final HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      final JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
      final GoogleCredential credential = GoogleCredential
          .getApplicationDefault(httpTransport, jsonFactory)
          .createScoped(IamScopes.all());
      final Iam iam = new Iam.Builder(
          httpTransport, jsonFactory, credential)
          .setApplicationName(SERVICE_NAME)
          .build();
      return new ServiceAccountKeyManager(iam);
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  static NamespacedKubernetesClient getKubernetesClient(Config rootConfig, String id,
      Container gke, KubernetesClientFactory clientFactory) {
    try {
      final Config config = rootConfig
          .getConfig(GKE_CLUSTER_PATH)
          .getConfig(id);

      final Cluster cluster = gke.projects().locations().clusters()
          .get(String.format("projects/%s/locations/%s/clusters/%s",
                             config.getString(GKE_CLUSTER_PROJECT_ID),
                             config.getString(GKE_CLUSTER_ZONE),
                             config.getString(GKE_CLUSTER_ID))).execute();

      final io.fabric8.kubernetes.client.Config kubeConfig = new ConfigBuilder()
          .withMasterUrl("https://" + cluster.getEndpoint())
          .withCaCertData(cluster.getMasterAuth().getClusterCaCertificate())
          .withClientCertData(cluster.getMasterAuth().getClientCertificate())
          .withClientKeyData(cluster.getMasterAuth().getClientKey())
          .withNamespace(config.getString(GKE_CLUSTER_NAMESPACE))
          .build();

      return clientFactory.apply(kubeConfig);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private static Closeable executorCloser(String name, ExecutorService executor) {
    return () -> {
      LOG.info("Shutting down executor: {}", name);
      executor.shutdown();
      try {
        executor.awaitTermination(1, SECONDS);
      } catch (InterruptedException ignored) {
      }
      final List<Runnable> runnables = executor.shutdownNow();
      if (!runnables.isEmpty()) {
        LOG.warn("{} task(s) in {} did not execute", runnables.size(), name);
      }
    };
  }

  private static boolean isDevMode(Config config) {
    return STYX_MODE_DEVELOPMENT.equals(config.getString(STYX_MODE));
  }

  interface KubernetesClientFactory
      extends Function<io.fabric8.kubernetes.client.Config, NamespacedKubernetesClient> { }
}
