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

import static com.spotify.styx.monitoring.MeteredProxy.instrument;
import static com.spotify.styx.state.OutputHandler.fanOutput;
import static com.spotify.styx.util.Connections.createBigTableConnection;
import static com.spotify.styx.util.Connections.createDatastore;
import static com.spotify.styx.util.GuardedRunnable.guard;
import static com.spotify.styx.util.ReplayEvents.transitionLogger;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MINUTES;
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
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.MetricsStats;
import com.spotify.styx.monitoring.MonitoringHandler;
import com.spotify.styx.monitoring.Stats;
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
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.InMemStorage;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.CachedSupplier;
import com.spotify.styx.util.Debug;
import com.spotify.styx.util.DockerImageValidator;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.RetryUtil;
import com.spotify.styx.util.StorageFactory;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerUtil;
import com.spotify.styx.util.WorkflowValidator;
import com.spotify.styx.workflow.WorkflowInitializer;
import com.typesafe.config.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
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

  public static final int SCHEDULER_TICK_INTERVAL_SECONDS = 2;
  public static final int TRIGGER_MANAGER_TICK_INTERVAL_SECONDS = 1;
  public static final long CLEANER_TICK_INTERVAL_SECONDS = MINUTES.toSeconds(30);
  public static final int RUNTIME_CONFIG_UPDATE_INTERVAL_SECONDS = 5;
  public static final Duration DEFAULT_RETRY_BASE_DELAY = Duration.ofMinutes(3);
  public static final int DEFAULT_RETRY_MAX_EXPONENT = 4;
  public static final Duration DEFAULT_RETRY_BASE_DELAY_BT = Duration.ofSeconds(1);
  public static final RetryUtil DEFAULT_RETRY_UTIL =
      new RetryUtil(DEFAULT_RETRY_BASE_DELAY, DEFAULT_RETRY_MAX_EXPONENT);
  public static final double DEFAULT_SUBMISSION_RATE_PER_SEC = 1000D;

  private static final Logger LOG = LoggerFactory.getLogger(StyxScheduler.class);

  private final Time time;
  private final StorageFactory storageFactory;
  private final DockerRunnerFactory dockerRunnerFactory;
  private final StatsFactory statsFactory;
  private final ExecutorFactory executorFactory;
  private final PublisherFactory publisherFactory;
  private final RetryUtil retryUtil;
  private final WorkflowResourceDecorator resourceDecorator;
  private final EventConsumerFactory eventConsumerFactory;
  private final WorkflowConsumerFactory workflowConsumerFactory;
  private final WorkflowExecutionGateFactory executionGateFactory;

  private StateManager stateManager;
  private Scheduler scheduler;
  private TriggerManager triggerManager;
  private BackfillTriggerManager backfillTriggerManager;

  private Consumer<Workflow> workflowRemoveListener;
  private Consumer<Workflow> workflowChangeListener;

  // === Type aliases for dependency injectors ====================================================
  public interface StateFactory extends Function<WorkflowInstance, RunState> { }
  public interface StatsFactory extends Function<Environment, Stats> { }
  public interface PublisherFactory extends Function<Environment, Publisher> { }
  public interface EventConsumerFactory extends BiFunction<Environment, Stats, BiConsumer<SequenceEvent, RunState>> { }
  public interface WorkflowConsumerFactory
      extends BiFunction<Environment, Stats, BiConsumer<Optional<Workflow>, Optional<Workflow>>> { }
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

    private Time time = Instant::now;
    private StorageFactory storageFactory = storage(StyxScheduler::storage);
    private DockerRunnerFactory dockerRunnerFactory = StyxScheduler::createDockerRunner;
    private StatsFactory statsFactory = StyxScheduler::stats;
    private ExecutorFactory executorFactory = Executors::newScheduledThreadPool;
    private PublisherFactory publisherFactory = (env) -> Publisher.NOOP;
    private RetryUtil retryUtil = DEFAULT_RETRY_UTIL;
    private WorkflowResourceDecorator resourceDecorator = WorkflowResourceDecorator.NOOP;
    private EventConsumerFactory eventConsumerFactory = (env, stats) -> (event, state) -> { };
    private WorkflowConsumerFactory workflowConsumerFactory = (env, stats) -> (oldWorkflow, newWorkflow) -> { };
    private WorkflowExecutionGateFactory executionGateFactory = (env, storage) -> WorkflowExecutionGate.NOOP;

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

    public Builder setWorkflowConsumerFactory(WorkflowConsumerFactory workflowConsumerFactory) {
      this.workflowConsumerFactory = workflowConsumerFactory;
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
    this.time = requireNonNull(builder.time);
    this.storageFactory = requireNonNull(builder.storageFactory);
    this.dockerRunnerFactory = requireNonNull(builder.dockerRunnerFactory);
    this.statsFactory = requireNonNull(builder.statsFactory);
    this.executorFactory = requireNonNull(builder.executorFactory);
    this.publisherFactory = requireNonNull(builder.publisherFactory);
    this.retryUtil = requireNonNull(builder.retryUtil);
    this.resourceDecorator = requireNonNull(builder.resourceDecorator);
    this.eventConsumerFactory = requireNonNull(builder.eventConsumerFactory);
    this.workflowConsumerFactory = requireNonNull(builder.workflowConsumerFactory);
    this.executionGateFactory = requireNonNull(builder.executionGateFactory);
  }

  @Override
  public void create(Environment environment) {
    final Config config = environment.config();
    final Closer closer = environment.closer();

    final Thread.UncaughtExceptionHandler uncaughtExceptionHandler =
        (thread, throwable) -> LOG.error("Thread {} threw {}", thread, throwable);
    final ThreadFactory schedulerTf = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("styx-scheduler-%d")
        .setUncaughtExceptionHandler(uncaughtExceptionHandler)
        .build();
    final ThreadFactory eventTf = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("styx-event-worker-%d")
        .setUncaughtExceptionHandler(uncaughtExceptionHandler)
        .build();

    final Publisher publisher = publisherFactory.apply(environment);
    closer.register(publisher);

    final ScheduledExecutorService executor = executorFactory.create(3, schedulerTf);
    closer.register(executorCloser("scheduler", executor));
    final BlockingQueue<Runnable> outputHandlerExecutorQueue = new LinkedBlockingQueue<>();
    final ExecutorService outputHandlerExecutor = new ThreadPoolExecutor(16, 16,
        0L, TimeUnit.MILLISECONDS,
        outputHandlerExecutorQueue,
        eventTf);
    closer.register(executorCloser("output-handler", outputHandlerExecutor));
    final ExecutorService eventConsumerExecutor = Executors.newSingleThreadExecutor();
    closer.register(executorCloser("event-consumer", eventConsumerExecutor));

    final Stats stats = statsFactory.apply(environment);
    final WorkflowCache workflowCache = new InMemWorkflowCache();
    final Storage storage = instrument(Storage.class, storageFactory.apply(environment), stats, time);

    warmUpCache(workflowCache, storage);

    // TODO: hack to get around circular reference. Change OutputHandler.transitionInto() to
    //       take StateManager as argument instead?
    final List<OutputHandler> outputHandlers = new ArrayList<>();
    final QueuedStateManager stateManager = closer.register(
        new QueuedStateManager(time, outputHandlerExecutor, storage,
            eventConsumerFactory.apply(environment, stats), eventConsumerExecutor, fanOutput(outputHandlers)));

    final Config staleStateTtlConfig = config.getConfig(STYX_STALE_STATE_TTL_CONFIG);
    final TimeoutConfig timeoutConfig = TimeoutConfig.createFromConfig(staleStateTtlConfig);

    final Supplier<StyxConfig> styxConfig = new CachedSupplier<>(storage::config, time);
    final Supplier<String> dockerId = () -> styxConfig.get().globalDockerRunnerId();
    final Debug debug = () -> styxConfig.get().debugEnabled();
    final DockerRunner routingDockerRunner = DockerRunner.routing(
        id -> dockerRunnerFactory.create(id, environment, stateManager, executor, stats, debug),
        dockerId);
    final DockerRunner dockerRunner = instrument(DockerRunner.class, routingDockerRunner, stats, time);

    final RateLimiter dequeueRateLimiter = RateLimiter.create(DEFAULT_SUBMISSION_RATE_PER_SEC);

    outputHandlers.addAll(ImmutableList.of(
        transitionLogger(""),
        new DockerRunnerHandler(
            dockerRunner, stateManager),
        new TerminationHandler(retryUtil, stateManager),
        new MonitoringHandler(stats),
        new PublisherHandler(publisher),
        new ExecutionDescriptionHandler(storage, stateManager, new WorkflowValidator(new DockerImageValidator()))));

    final TriggerListener trigger =
        new StateInitializingTrigger(stateManager);
    final TriggerManager triggerManager = new TriggerManager(trigger, time, storage, stats);
    final BackfillTriggerManager backfillTriggerManager =
        new BackfillTriggerManager(stateManager, workflowCache, storage, trigger);

    final WorkflowInitializer workflowInitializer = new WorkflowInitializer(storage, time);
    final BiConsumer<Optional<Workflow>, Optional<Workflow>> workflowConsumer =
        workflowConsumerFactory.apply(environment, stats);
    final Consumer<Workflow> workflowRemoveListener =
        workflowRemoved(workflowCache, storage, workflowConsumer);
    final Consumer<Workflow> workflowChangeListener =
        workflowChanged(workflowCache, workflowInitializer, stats, stateManager, workflowConsumer);

    final Scheduler scheduler = new Scheduler(time, timeoutConfig, stateManager, workflowCache,
                                              storage, resourceDecorator, stats, dequeueRateLimiter,
                                              executionGateFactory.apply(environment, storage));

    final Cleaner cleaner = new Cleaner(dockerRunner);

    dockerRunner.restore();

    startTriggerManager(triggerManager, executor);
    startBackfillTriggerManager(backfillTriggerManager, executor);
    startScheduler(scheduler, executor);
    startRuntimeConfigUpdate(styxConfig, executor, dequeueRateLimiter);
    startCleaner(cleaner, executor);
    setupMetrics(stateManager, workflowCache, storage, dequeueRateLimiter, stats, outputHandlerExecutorQueue);

    final SchedulerResource schedulerResource =
        new SchedulerResource(stateManager, trigger, workflowChangeListener, workflowRemoveListener,
                              storage, time, new WorkflowValidator(new DockerImageValidator()));

    environment.routingEngine()
        .registerAutoRoute(Route.sync("GET", "/ping", rc -> "pong"))
        .registerRoutes(Api.withCommonMiddleware(schedulerResource.routes()));

    this.stateManager = stateManager;
    this.scheduler = scheduler;
    this.triggerManager = triggerManager;
    this.backfillTriggerManager = backfillTriggerManager;
    this.workflowRemoveListener = workflowRemoveListener;
    this.workflowChangeListener = workflowChangeListener;
  }

  @VisibleForTesting
  CompletionStage<Void> receive(Event event) throws IsClosedException {
    return stateManager.receive(event);
  }

  @VisibleForTesting
  RunState getState(WorkflowInstance workflowInstance) {
    return stateManager.get(workflowInstance);
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

  @VisibleForTesting
  Consumer<Workflow> getWorkflowRemoveListener() {
    return workflowRemoveListener;
  }

  @VisibleForTesting
  Consumer<Workflow> getWorkflowChangeListener() {
    return workflowChangeListener;
  }

  private void warmUpCache(WorkflowCache cache, Storage storage) {
    try {
      storage.workflows().values().forEach(cache::store);
    } catch (IOException e) {
      LOG.error("Failed to get workflows from storage while warming up the cache");
      throw new RuntimeException(e);
    }
  }

  private static void startCleaner(Cleaner cleaner, ScheduledExecutorService exec) {
    exec.scheduleWithFixedDelay(
        guard(cleaner::tick),
        0,
        CLEANER_TICK_INTERVAL_SECONDS,
        SECONDS);
  }

  private static void startTriggerManager(TriggerManager triggerManager, ScheduledExecutorService exec) {
    exec.scheduleWithFixedDelay(
        guard(triggerManager::tick),
        TRIGGER_MANAGER_TICK_INTERVAL_SECONDS,
        TRIGGER_MANAGER_TICK_INTERVAL_SECONDS,
        SECONDS);
  }

  private static void startBackfillTriggerManager(BackfillTriggerManager backfillTriggerManager,
                                                  ScheduledExecutorService exec) {
    exec.scheduleWithFixedDelay(
        guard(backfillTriggerManager::tick),
        TRIGGER_MANAGER_TICK_INTERVAL_SECONDS,
        TRIGGER_MANAGER_TICK_INTERVAL_SECONDS,
        SECONDS);
  }

  private static void startScheduler(Scheduler scheduler, ScheduledExecutorService exec) {
    exec.scheduleAtFixedRate(
        guard(scheduler::tick),
        SCHEDULER_TICK_INTERVAL_SECONDS,
        SCHEDULER_TICK_INTERVAL_SECONDS,
        SECONDS);
  }

  private static void startRuntimeConfigUpdate(Supplier<StyxConfig> config, ScheduledExecutorService exec,
      RateLimiter submissionRateLimiter) {
    exec.scheduleAtFixedRate(
        guard(() -> updateRuntimeConfig(config, submissionRateLimiter)),
        0,
        RUNTIME_CONFIG_UPDATE_INTERVAL_SECONDS,
        SECONDS);
  }

  private static void updateRuntimeConfig(Supplier<StyxConfig> config, RateLimiter rateLimiter) {
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
          + "skipping RateLimiter update");
    }
  }

  private void setupMetrics(
      StateManager stateManager,
      WorkflowCache workflowCache,
      Storage storage,
      RateLimiter submissionRateLimiter,
      Stats stats,
      BlockingQueue<Runnable> outputHandlerExecutorQueue) {

    stats.registerQueuedEventsMetric(() -> (long) outputHandlerExecutorQueue.size());

    stats.registerWorkflowCountMetric("all", () -> (long) workflowCache.all().size());

    stats.registerWorkflowCountMetric("configured", () -> workflowCache.all().stream()
        .filter(workflow -> workflow.configuration().dockerImage().isPresent())
        .count());

    final Supplier<Gauge<Long>> configuredEnabledWorkflowsCountGaugeSupplier = () -> {
      final Supplier<Set<WorkflowId>> enabledWorkflowSupplier =
          new CachedSupplier<>(storage::enabled, Instant::now);
      return () -> workflowCache.all().stream()
          .filter(workflow -> workflow.configuration().dockerImage().isPresent())
          .filter(workflow -> enabledWorkflowSupplier.get().contains(WorkflowId.ofWorkflow(workflow)))
          .count();
    };
    stats.registerWorkflowCountMetric("enabled", configuredEnabledWorkflowsCountGaugeSupplier.get());

    stats.registerWorkflowCountMetric("docker_termination_logging_enabled", () ->
        workflowCache.all().stream()
            .filter(workflow -> workflow.configuration().dockerImage().isPresent())
            .filter(workflow -> workflow.configuration().dockerTerminationLogging())
            .count());

    final Map<WorkflowInstance, RunState> activeStates = stateManager.activeStates();
    Arrays.stream(RunState.State.values()).forEach(state -> {
      TriggerUtil.triggerTypesList().forEach(triggerType ->
          stats.registerActiveStatesMetric(
              state,
              triggerType,
              () -> stateManager.activeStates().values().stream()
                  .filter(runState -> runState.state().equals(state))
                  .filter(runState -> runState.data().trigger().isPresent() && triggerType
                      .equals(TriggerUtil.triggerType(runState.data().trigger().get())))
                  .count()));
      stats.registerActiveStatesMetric(
          state,
          "none", () -> stateManager.activeStates().values().stream()
              .filter(runState -> runState.state().equals(state))
              .filter(runState -> !runState.data().trigger().isPresent())
              .count());
    });

    workflowCache.all().forEach(workflow -> stats.registerActiveStatesMetric(
        workflow.id(),
        () -> activeStates.keySet().stream()
            .filter(wfi -> workflow.id().equals(wfi.workflowId()))
            .count()));

    stats.registerSubmissionRateLimitMetric(submissionRateLimiter::getRate);
  }

  private static Consumer<Workflow> workflowChanged(
      WorkflowCache cache,
      WorkflowInitializer workflowInitializer,
      Stats stats,
      StateManager stateManager,
      BiConsumer<Optional<Workflow>, Optional<Workflow>> workflowConsumer) {
    return (workflow) -> {
      stats.registerActiveStatesMetric(
          workflow.id(),
          () -> stateManager.activeStates().keySet().stream()
              .filter(wfi -> workflow.id().equals(wfi.workflowId()))
              .count());

      final Optional<Workflow> oldWorkflowOptional = cache.workflow(workflow.id());

      workflowInitializer.inspectChange(workflow);
      cache.store(workflow);
      workflowConsumer.accept(oldWorkflowOptional, Optional.of(workflow));
      if (oldWorkflowOptional.isPresent()) {
        LOG.info("Workflow modified, old config: {}, new config: {}", oldWorkflowOptional.get(),
            workflow);
      } else {
        LOG.info("Workflow added: {}", workflow);
      }
    };
  }

  private static Consumer<Workflow> workflowRemoved(
      WorkflowCache cache,
      Storage storage,
      BiConsumer<Optional<Workflow>, Optional<Workflow>> workflowConsumer) {
    return workflow -> cache.workflow(workflow.id()).ifPresent(existingWorkflow -> {
      try {
        storage.delete(workflow.id());
      } catch (IOException e) {
        LOG.warn("Couldn't remove workflow {}. ", workflow.id());
        return;
      }
      cache.remove(workflow);
      workflowConsumer.accept(Optional.of(workflow), Optional.empty());
      LOG.info("Workflow removed: {}", workflow);
    });
  }

  private static Stats stats(Environment environment) {
    return new MetricsStats(environment.resolve(SemanticMetricRegistry.class), Instant::now);
  }

  private static StorageFactory storage(StorageFactory storage) {
    return (environment) -> {
      if (isDevMode(environment.config())) {
        LOG.info("Running Styx in development mode, will use InMemStorage");
        return new InMemStorage();
      } else {
        return storage.apply(environment);
      }
    };
  }

  private static AggregateStorage storage(Environment environment) {
    final Config config = environment.config();
    final Closer closer = environment.closer();

    final Connection bigTable = closer.register(createBigTableConnection(config));
    final Datastore datastore = createDatastore(config);
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
      final NamespacedKubernetesClient kubernetes = closer.register(getKubernetesClient(
          config, id, createGkeClient(), DefaultKubernetesClient::new));
      final ServiceAccountKeyManager serviceAccountKeyManager = createServiceAccountKeyManager();
      return closer.register(DockerRunner.kubernetes(kubernetes, stateManager, stats,
          serviceAccountKeyManager, debug));
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
