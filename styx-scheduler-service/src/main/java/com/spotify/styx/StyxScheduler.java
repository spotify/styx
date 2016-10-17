/*
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
/**
 * Copyright (C) 2015 Spotify AB
 */

package com.spotify.styx;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.container.Container;
import com.google.api.services.container.ContainerScopes;
import com.google.api.services.container.model.Cluster;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.common.base.Throwables;
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.codahale.metrics.Gauge;
import com.spotify.apollo.AppInit;
import com.spotify.apollo.Environment;
import com.spotify.apollo.route.Route;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.styx.api.SchedulerResource;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.docker.WorkflowValidator;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.MeteredDockerRunner;
import com.spotify.styx.monitoring.MeteredEventStorage;
import com.spotify.styx.monitoring.MeteredStorage;
import com.spotify.styx.monitoring.MetricsStats;
import com.spotify.styx.monitoring.MonitoringHandler;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.publisher.Publisher;
import com.spotify.styx.schedule.ScheduleSource;
import com.spotify.styx.schedule.ScheduleSourceFactory;
import com.spotify.styx.state.OutputHandler;
import com.spotify.styx.state.QueuedStateManager;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StaleStateReaper;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.state.StateRetrier;
import com.spotify.styx.state.TimeoutConfig;
import com.spotify.styx.state.handlers.DockerRunnerHandler;
import com.spotify.styx.state.handlers.ExecutionDescriptionHandler;
import com.spotify.styx.state.handlers.PublisherHandler;
import com.spotify.styx.state.handlers.StorageHandler;
import com.spotify.styx.state.handlers.TerminationHandler;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.EventStorage;
import com.spotify.styx.storage.InMemStorage;
import com.spotify.styx.storage.NoopEventStorage;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.util.EventStorageFactory;
import com.spotify.styx.util.Singleton;
import com.spotify.styx.util.StorageFactory;
import com.spotify.styx.util.Time;
import com.typesafe.config.Config;

import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;

import static com.spotify.styx.util.ReplayEvents.replayActiveStates;
import static com.spotify.styx.util.ReplayEvents.transitionLogger;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;

public class StyxScheduler implements AppInit {

  public static final String SERVICE_NAME = "styx-scheduler";

  public static final String GKE_CLUSTER_PROJECT_ID = "styx.gke.project-id";
  public static final String GKE_CLUSTER_ZONE = "styx.gke.cluster-zone";
  public static final String GKE_CLUSTER_ID = "styx.gke.cluster-id";
  public static final String BIGTABLE_PROJECT_ID = "styx.bigtable.project-id";
  public static final String BIGTABLE_INSTANCE_ID = "styx.bigtable.instance-id";
  public static final String DATASTORE_PROJECT = "styx.datastore.project-id";
  public static final String DATASTORE_NAMESPACE = "styx.datastore.namespace";

  public static final String STYX_STALE_STATE_TTL_CONFIG = "styx.stale-state-ttls";
  public static final String STYX_MODE = "styx.mode";
  public static final String STYX_MODE_DEVELOPMENT = "development";

  public static final int STATE_REAP_INTERVAL_SECONDS = 30;
  public static final int STATE_RETRY_CHECK_INTERVAL_SECONDS = 2;
  public static final Duration DEFAULT_RETRY_BASE_DELAY = Duration.ofMinutes(3);
  public static final int DEFAULT_RETRY_MAX_EXPONENT = 6;
  public static final Duration DEFAULT_RETRY_BASE_DELAY_BT = Duration.ofSeconds(1);

  private static final Logger LOG = LoggerFactory.getLogger(StyxScheduler.class);

  // === Type aliases for dependency injectors ====================================================
  public interface StateFactory extends Function<WorkflowInstance, RunState> { }
  public interface ScheduleSources extends Supplier<Iterable<ScheduleSourceFactory>> { }
  public interface StatsFactory extends Function<Environment, Stats> { }
  public interface PublisherFactory extends Function<Environment, Publisher> { }

  @FunctionalInterface
  interface DockerRunnerFactory {
    DockerRunner create(
        Environment environment,
        StateManager stateManager,
        ScheduledExecutorService scheduler,
        Stats stats);
  }

  @FunctionalInterface
  interface ExecutorFactory {
    ScheduledExecutorService create(
        int threads,
        ThreadFactory threadFactory);
  }

  public static class Builder {

    private final Singleton<AggregateStorage> storage = Singleton.create(StyxScheduler::storage);

    private Time time = Instant::now;
    private StorageFactory storageFactory = storage(storage);
    private EventStorageFactory eventStorageFactory = eventStorage(storage);
    private DockerRunnerFactory dockerRunnerFactory = StyxScheduler::createDockerRunner;
    private ScheduleSources scheduleSources = () -> ServiceLoader.load(ScheduleSourceFactory.class);
    private StatsFactory statsFactory = StyxScheduler::stats;
    private ExecutorFactory executorFactory = Executors::newScheduledThreadPool;
    private PublisherFactory publisherFactory = (env) -> Publisher.NOOP;

    public Builder setTime(Time time) {
      this.time = time;
      return this;
    }

    public Builder setStorageFactory(StorageFactory storageFactory) {
      this.storageFactory = storageFactory;
      return this;
    }

    public Builder setEventStorageFactory(EventStorageFactory eventStorageFactory) {
      this.eventStorageFactory = eventStorageFactory;
      return this;
    }

    public Builder setDockerRunnerFactory(DockerRunnerFactory dockerRunnerFactory) {
      this.dockerRunnerFactory = dockerRunnerFactory;
      return this;
    }

    public Builder setScheduleSources(ScheduleSources scheduleSources) {
      this.scheduleSources = scheduleSources;
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

    public StyxScheduler build() {
      return new StyxScheduler(
          time,
          storageFactory,
          eventStorageFactory,
          dockerRunnerFactory,
          scheduleSources,
          statsFactory,
          executorFactory,
          publisherFactory);
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static StyxScheduler createDefault() {
    return newBuilder().build();
  }

  // ==============================================================================================

  private final Time time;
  private final StorageFactory storageFactory;
  private final EventStorageFactory eventStorageFactory;
  private final DockerRunnerFactory dockerRunnerFactory;
  private final ScheduleSources scheduleSources;
  private final StatsFactory statsFactory;
  private final ExecutorFactory executorFactory;
  private final PublisherFactory publisherFactory;

  private StateManager stateManager;

  private StyxScheduler(
      Time time,
      StorageFactory storageFactory,
      EventStorageFactory eventStorageFactory,
      DockerRunnerFactory dockerRunnerFactory,
      ScheduleSources scheduleSources,
      StatsFactory statsFactory,
      ExecutorFactory executorFactory,
      PublisherFactory publisherFactory) {
    this.time = requireNonNull(time);
    this.storageFactory = requireNonNull(storageFactory);
    this.eventStorageFactory = requireNonNull(eventStorageFactory);
    this.dockerRunnerFactory = requireNonNull(dockerRunnerFactory);
    this.scheduleSources = requireNonNull(scheduleSources);
    this.statsFactory = requireNonNull(statsFactory);
    this.executorFactory = requireNonNull(executorFactory);
    this.publisherFactory = requireNonNull(publisherFactory);
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

    final ScheduledExecutorService scheduler = executorFactory.create(3, schedulerTf);
    final ExecutorService eventWorker = Executors.newFixedThreadPool(2, eventTf);
    closer.register(executorCloser("scheduler", scheduler));
    closer.register(executorCloser("event-worker", eventWorker));

    final Stats stats = statsFactory.apply(environment);
    final Storage storage = new MeteredStorage(storageFactory.apply(environment), stats, time);
    final EventStorage eventStorage = new MeteredEventStorage(eventStorageFactory.apply(environment),
                                                              stats, time);

    final Config staleStateTtlConfig = config.getConfig(STYX_STALE_STATE_TTL_CONFIG);
    final TimeoutConfig timeoutConfig = TimeoutConfig.createFromConfig(staleStateTtlConfig);
    final QueuedStateManager stateManager = closer.register(new QueuedStateManager(
        timeoutConfig, time, eventWorker, eventStorage));
    final DockerRunner dockerRunner =
        new MeteredDockerRunner(dockerRunnerFactory.create(environment, stateManager, scheduler, stats),
                                stats, time);
    final Publisher publisher = publisherFactory.apply(environment);

    final OutputHandler[] outputHandlers = new OutputHandler[] {
        transitionLogger(""),
        new DockerRunnerHandler(dockerRunner, stateManager),
        new StorageHandler(storage, Clock.systemUTC()),
        new TerminationHandler(
            DEFAULT_RETRY_BASE_DELAY,
            DEFAULT_RETRY_MAX_EXPONENT,
            stateManager),
        new MonitoringHandler(time, stats),
        new PublisherHandler(publisher),
        new ExecutionDescriptionHandler(storage, stateManager)
    };
    final StateFactory stateFactory =
        (workflowInstance) -> RunState.fresh(workflowInstance, time, outputHandlers);

    final TriggerListener trigger = trigger(storage, stateFactory, stateManager);
    final TickerManager tickerManager = new TickerManager(
        (workflow) -> new TickTock(workflow, trigger, time, scheduler));

    final WorkflowCache cache = new InMemWorkflowCache();
    final Consumer<Workflow> workflowChangeListener = workflowChanged(cache, storage, tickerManager,
                                                                      stats, stateManager);
    final Consumer<Workflow> workflowRemoveListener = tickerManager::removeWorkflow;

    restoreState(eventStorage, outputHandlers, stateManager);
    startScheduleSources(environment, scheduler, workflowChangeListener, workflowRemoveListener);
    startRetryChecker(stateManager, scheduler);
    startStateReaper(stateManager, scheduler);
    setupMetrics(stateManager, cache, storage, stats);

    final SchedulerResource schedulerResource = new SchedulerResource(stateManager, trigger, storage, time);

    environment.routingEngine()
        .registerAutoRoute(Route.sync("GET", "/ping", rc -> "pong"))
        .registerRoutes(schedulerResource.routes());

    this.stateManager = stateManager;
  }

  void receive(Event event) throws StateManager.IsClosed {
    stateManager.receive(event);
  }

  RunState getState(WorkflowInstance workflowInstance) {
    return stateManager.get(workflowInstance);
  }

  private void restoreState(
      EventStorage eventStorage,
      OutputHandler[] outputHandlers,
      StateManager stateManager) {
    try {
      final Map<WorkflowInstance, Long> activeInstances =
          eventStorage.readActiveWorkflowInstances();

      replayActiveStates(activeInstances, eventStorage, true)
          .entrySet().stream()
          .collect(toMap(
              e -> e.getKey()
                  .withHandlers(outputHandlers)
                  .withTime(time),
              Map.Entry::getValue))
          .forEach(stateManager::restore);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void startScheduleSources(
      Environment environment,
      ScheduledExecutorService scheduler,
      Consumer<Workflow> workflowChangeListener,
      Consumer<Workflow> workflowRemoveListener) {
    for (ScheduleSourceFactory sourceFactory : scheduleSources.get()) {
      try {
        LOG.info("Loading auto-discovered ScheduleSource from {}", sourceFactory);
        final ScheduleSource scheduleSource = sourceFactory.create(
            workflowChangeListener, workflowRemoveListener, environment, scheduler);
        scheduleSource.start();
      } catch (Throwable t) {
        LOG.warn("ScheduleSourceFactory {} threw", sourceFactory, t);
      }
    }
  }

  private static void startRetryChecker(StateRetrier retrier, ScheduledExecutorService exec) {
    exec.scheduleWithFixedDelay(
        guard(retrier::triggerRetries),
        STATE_RETRY_CHECK_INTERVAL_SECONDS,
        STATE_RETRY_CHECK_INTERVAL_SECONDS,
        TimeUnit.SECONDS);
  }

  private static void startStateReaper(StaleStateReaper reaper, ScheduledExecutorService exec) {
    exec.scheduleWithFixedDelay(
        guard(reaper::triggerTimeouts),
        STATE_REAP_INTERVAL_SECONDS,
        STATE_REAP_INTERVAL_SECONDS,
        TimeUnit.SECONDS);
  }

  private static Runnable guard(Runnable delegate) {
    return () -> {
      try {
        delegate.run();
      } catch (Throwable t) {
        LOG.warn("Guarded runnable threw", t);
      }
    };
  }

  private void setupMetrics(
      StateManager stateManager,
      WorkflowCache workflowCache,
      Storage storage,
      Stats stats) {

    final Gauge<Long> queuedEventsCount = stateManager::getQueuedEventsCount;
    final Gauge<Long> activeStatesCount = stateManager::getActiveStatesCount;
    final Gauge<Long> allWorkflowsCount = () -> workflowCache.all().stream().count();
    final Gauge<Long> configuredWorkflowsCount = () -> workflowCache.all().stream()
        .filter(WorkflowValidator::hasDockerConfiguration)
        .count();
    final Gauge<Long> configuredEnabledWorkflowsCount = () -> {
      try {
        final Set<WorkflowId> enabledWorkflowsSet = storage.enabled();
        return workflowCache.all().stream()
            .filter(WorkflowValidator::hasDockerConfiguration)
            .filter((workflow) -> enabledWorkflowsSet.contains(WorkflowId.ofWorkflow(workflow)))
            .count();
      } catch (IOException e) {
        LOG.error("Failed to read enabled status from BigTable", e);
        return 0L;
      }
    };

    stats.registerQueuedEvents(queuedEventsCount);
    stats.registerActiveStates(activeStatesCount);
    stats.registerWorkflowCount("all", allWorkflowsCount);
    stats.registerWorkflowCount("configured", configuredWorkflowsCount);
    stats.registerWorkflowCount("enabled", configuredEnabledWorkflowsCount);
  }

  private TriggerListener trigger(
      Storage storage,
      StateFactory stateFactory,
      StateManager stateManager) {
    final TriggerListener stateInitializingTrigger =
        new StateInitializingTrigger(stateFactory, stateManager, storage);

    return (workflow, triggerId, instant) -> {
      try {
        if (!storage.enabled(workflow.id()) || !storage.globalEnabled()) {
          LOG.info("Triggered disabled workflow {}", workflow.endpointId());
          return;
        }
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }

      stateInitializingTrigger.event(workflow, triggerId, instant);
    };
  }

  private static Consumer<Workflow> workflowChanged(
      WorkflowCache cache,
      Storage storage,
      TickerManager tickerManager,
      Stats stats,
      StateManager stateManager) {

    return (workflow) -> {
      stats.registerActiveStates(
          workflow.id(),
          () -> stateManager.getActiveStatesCount(workflow.id()));

      tickerManager.updateWorkflow(workflow);
      cache.store(workflow);
      try {
        storage.store(workflow);
      } catch (IOException e) {
        LOG.warn("Failed to store workflow " + workflow, e);
      }
    };
  }

  private static Stats stats(Environment environment) {
    return new MetricsStats(environment.resolve(SemanticMetricRegistry.class));
  }

  private static StorageFactory storage(Singleton<AggregateStorage> storage) {
    return (environment) -> {
      if (isDevMode(environment.config())) {
        LOG.info("Running Styx in development mode, will use InMemStorage");
        return new InMemStorage();
      } else {
        return storage.apply(environment);
      }
    };
  }

  private static EventStorageFactory eventStorage(Singleton<AggregateStorage> storage) {
    return environment -> {
      if (isDevMode(environment.config())) {
        LOG.info("Running Styx in development mode, will use NoopEventStorage");
        return new NoopEventStorage();
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

  private static Connection createBigTableConnection(Config config) {
    final String projectId = config.getString(BIGTABLE_PROJECT_ID);
    final String instanceId = config.getString(BIGTABLE_INSTANCE_ID);

    LOG.info("Creating Bigtable connection for project:{}, instance:{}",
             projectId, instanceId);

    return BigtableConfiguration.connect(projectId, instanceId);
  }

  static Datastore createDatastore(Config config) {
    final String projectId = config.getString(DATASTORE_PROJECT);
    final String namespace = config.getString(DATASTORE_NAMESPACE);

    return DatastoreOptions.builder()
        .namespace(namespace)
        .projectId(projectId)
        .build()
        .service();
  }

  private static DockerRunner createDockerRunner(
      Environment environment,
      StateManager stateManager,
      ScheduledExecutorService scheduler,
      Stats stats) {
    final Config config = environment.config();
    final Closer closer = environment.closer();

    if (isDevMode(config)) {
      LOG.info("Creating LocalDockerRunner");
      return closer.register(DockerRunner.local(scheduler, stateManager));
    } else {
      final KubernetesClient kubernetes = closer.register(getKubernetesClient(config));
      return closer.register(DockerRunner.kubernetes(kubernetes, stateManager, stats));
    }
  }

  private static KubernetesClient getKubernetesClient(Config config) {
    try {
      final HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      final JsonFactory jsonFactory = Utils.getDefaultJsonFactory();
      final GoogleCredential credential =
          GoogleCredential.getApplicationDefault(httpTransport, jsonFactory)
              .createScoped(ContainerScopes.all());
      final Container gke = new Container.Builder(httpTransport, jsonFactory, credential)
          .setApplicationName(SERVICE_NAME)
          .build();

      final Cluster cluster = gke.projects().zones().clusters()
          .get(config.getString(GKE_CLUSTER_PROJECT_ID),
               config.getString(GKE_CLUSTER_ZONE),
               config.getString(GKE_CLUSTER_ID)).execute();

      final io.fabric8.kubernetes.client.Config kubeConfig = new ConfigBuilder()
          .withMasterUrl("https://" + cluster.getEndpoint())
          .withCaCertData(cluster.getMasterAuth().getClusterCaCertificate())
          .withClientCertData(cluster.getMasterAuth().getClientCertificate())
          .withClientKeyData(cluster.getMasterAuth().getClientKey())
          .build();

      return new DefaultKubernetesClient(kubeConfig);
    } catch (GeneralSecurityException | IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private static Closeable executorCloser(String name, ExecutorService executor) {
    return () -> {
      LOG.info("Shutting down executor: {}", name);
      executor.shutdown();
      try {
        executor.awaitTermination(1, SECONDS);
      } catch (InterruptedException ignore) {
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
}
