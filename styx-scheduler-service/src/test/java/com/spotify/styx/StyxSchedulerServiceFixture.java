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

import static com.spotify.styx.model.WorkflowState.patchEnabled;
import static com.spotify.styx.util.TimeUtil.nextInstant;
import static java.time.temporal.ChronoUnit.HOURS;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.spotify.apollo.test.ServiceHelper;
import com.spotify.styx.api.Authenticator;
import com.spotify.styx.api.AuthenticatorFactory;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.monitoring.StatsFactory;
import com.spotify.styx.publisher.Publisher;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.BigtableMocker;
import com.spotify.styx.storage.BigtableStorage;
import com.spotify.styx.util.EventUtil;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.StorageFactory;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerInstantSpec;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.IntPredicate;
import java.util.logging.Level;
import javaslang.Tuple;
import javaslang.Tuple2;
import org.apache.hadoop.hbase.client.Connection;
import org.awaitility.core.ConditionTimeoutException;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test fixture for system tests that exercise all of Styx in isolation from external systems.
 */
public class StyxSchedulerServiceFixture {

  private static final Logger LOG = LoggerFactory.getLogger(StyxSchedulerServiceFixture.class);

  private Instant now = Instant.parse("1970-01-01T00:00:00Z");

  private LocalDatastoreHelper localDatastore;
  private Time time = () -> now;

  private Datastore datastore;
  private Connection bigtable = setupBigTableMockTable(0);
  protected AggregateStorage storage;
  private DeterministicScheduler executor = new QuietDeterministicScheduler();
  private Set<String> resourceIdsToDecorateWith = Sets.newHashSet();

  // circumstantial fields, set by test cases

  private List<Tuple2<SequenceEvent, RunState.State>> transitionedEvents = Lists.newArrayList();

  // captured fields from fakes
  Queue<Tuple2<WorkflowInstance, DockerRunner.RunSpec>> dockerRuns = new ConcurrentLinkedQueue();
  Queue<String> dockerCleans = new ConcurrentLinkedQueue();
  Queue<RunState> dockerPolls = new ConcurrentLinkedQueue();

  // service and helper
  private StyxScheduler styxScheduler;
  private ServiceHelper serviceHelper;

  @BeforeClass
  public static void setUpClass() throws Exception {
    // Schedule a full GC to run every second to mitigate off-heap/direct memory usage.
    // Without this, the system tests cause the test process memory usage (according to the system) to
    // balloon to several GB even though the JVM itself is configured to and reports that it is only using a few
    // hundred MB.
    // (T-T)
    Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(System::gc, 1, 1, SECONDS);
  }

  @Before
  public void setUp() throws Exception {

    final java.util.logging.Logger datastoreEmulatorLogger =
        java.util.logging.Logger.getLogger(LocalDatastoreHelper.class.getName());
    datastoreEmulatorLogger.setLevel(Level.OFF);

    // TODO: the datastore emulator behavior wrt conflicts etc differs from the real datastore
    localDatastore = LocalDatastoreHelper.create(1.0); // 100% global consistency
    try {
      localDatastore.start();
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException(e);
    }
    datastore = localDatastore.getOptions().getService();
    storage = new AggregateStorage(bigtable, datastore, Duration.ZERO);

    StorageFactory storageFactory = (env, stats) -> storage;
    StatsFactory statsFactory = (env) -> Stats.NOOP;
    StyxScheduler.ExecutorFactory executorFactory = (ts, tf) -> executor;
    StyxScheduler.PublisherFactory publisherFactory = (env) -> Publisher.NOOP;
    StyxScheduler.DockerRunnerFactory dockerRunnerFactory =
        (id, env, states, stats, debug) -> fakeDockerRunner();
    WorkflowResourceDecorator resourceDecorator = (rs, cfg, res) ->
        Sets.union(res, resourceIdsToDecorateWith);
    StyxScheduler.EventConsumerFactory eventConsumerFactory =
        (env, stats) -> (event, state) ->  transitionedEvents.add(Tuple.of(event, state.state()));
    AuthenticatorFactory authenticatorFactory = (cfg) -> mock(Authenticator.class);

    final ServiceAccountUsageAuthorizer.Factory serviceAccountUsageAuthorizerFactory =
        (cfg, name) -> ServiceAccountUsageAuthorizer.nop();
    styxScheduler = StyxScheduler.newBuilder()
        .setTime(time)
        .setStorageFactory(storageFactory)
        .setDockerRunnerFactory(dockerRunnerFactory)
        .setStatsFactory(statsFactory)
        .setExecutorFactory(executorFactory)
        .setPublisherFactory(publisherFactory)
        .setResourceDecorator(resourceDecorator)
        .setEventConsumerFactory(eventConsumerFactory)
        .setAuthenticatorFactory(authenticatorFactory)
        .setServiceAccountUsageAuthorizerFactory(serviceAccountUsageAuthorizerFactory)
        .build();

    serviceHelper = ServiceHelper.create(styxScheduler, StyxScheduler.SERVICE_NAME)
        .startTimeoutSeconds(30);
  }

  @After
  public void tearDown() throws Exception {
    serviceHelper.close();

    if (localDatastore != null) {
      try {
        localDatastore.stop(org.threeten.bp.Duration.ofSeconds(30));
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  void injectEvent(Event event) throws IsClosedException, InterruptedException, ExecutionException, TimeoutException {
    styxScheduler.receive(event).toCompletableFuture().get(1, MINUTES);
  }

  Optional<RunState> getState(WorkflowInstance workflowInstance) {
    return styxScheduler.getState(workflowInstance);
  }

  /**
   * @return a best effort snapshot, without throwing ConcurrentModificationException.
   */
  List<Tuple2<WorkflowInstance, DockerRunner.RunSpec>> getDockerRuns() {
    return Lists.newArrayList(dockerRuns);
  }

  /**
   * @return a best effort snapshot, without throwing ConcurrentModificationException.
   */
  List<Tuple2<SequenceEvent, RunState.State>> getTransitionedEventsByName(String name) {
    return Lists.newArrayList(transitionedEvents).stream()
        .filter(item -> name.equals(EventUtil.name(item._1.event())))
        .collect(toList());
  }

  void tickScheduler() {
    styxScheduler.tickScheduler();
  }

  void tickSchedulerUntil(Runnable asserter) {
    List<AssertionError> errors = Lists.newArrayList();
    try {
      await().atMost(30, SECONDS).until(() -> {
        tickScheduler();
        // Simulation time is sped up to exceed some stale-state-ttls during the entire await, so
        // that those states time out like in production. Needed because of dropped events. (E.g.
        // when submit event can't transition because of a transaction conflict, the state will be
        // stuck and no immediate docker run will result.)
        timePasses(3, MINUTES);
        try {
          asserter.run();
          return true;
        } catch (AssertionError ae) {
          errors.add(ae);
          return false;
        }
      });
    } catch (ConditionTimeoutException cte) {
      if (errors.size() > 0) {
        cte.addSuppressed(errors.get(errors.size() - 1));
      }
      throw cte;
    }
  }

  void tickTriggerManager() {
    styxScheduler.tickTriggerManager();
  }

  void tickBackfillTriggerManager() {
    styxScheduler.tickBackfillTriggerManager();
  }

  void givenTheTimeIs(String time) {
    now = Instant.parse(time);
    printTime();
  }

  void givenWorkflow(Workflow workflow) throws IOException {
    // storing before start causes the WorkflowInitializer not to do anything
    storage.storeWorkflow(workflow);
  }

  void givenQueuedWfisWithResources(String workflowId, int numInstances, Set<String> resourceIds)
      throws Exception {
    final WorkflowConfiguration configuration = WorkflowConfiguration.builder()
        .id(workflowId)
        .schedule(Schedule.HOURS)
        .dockerImage("busybox")
        .dockerArgs(asList("--hour", "{}"))
        .resources(resourceIds)
        .build();
    final Workflow workflow = Workflow.create("styx", configuration);
//    final Instant now = Instant.parse("2018-03-27T16:00:00Z");
//    givenTheTimeIs(now.toString());
    givenWorkflow(workflow);
    givenWorkflowEnabledStateIs(workflow, true);
    for (int i = 0; i < numInstances; i++) {
      givenActiveState(WorkflowInstance.create(workflow.id(),
          now.plus(i + 1, HOURS).toString().substring(0, 13)), 1);
    }
  }

  void givenNextNaturalTrigger(Workflow workflow, String nextNaturalTrigger) throws IOException {
    Instant next = Instant.parse(nextNaturalTrigger);
    Instant offset = workflow.configuration().addOffset(next);
    TriggerInstantSpec spec = TriggerInstantSpec.create(next, offset);

    storage.updateNextNaturalTrigger(workflow.id(), spec);
  }

  void givenBackfill(Backfill backfill) throws IOException {
    storage.storeBackfill(backfill);
  }

  void givenResource(Resource resource) throws IOException {
    storage.storeResource(resource);
  }

  void givenResourceIdsToDecorateWith(Set<String> resourceIds) throws IOException {
    resourceIdsToDecorateWith = resourceIds;
  }

  void workflowChanges(Workflow workflow) throws IOException {
    final TriggerInstantSpec triggerInstantSpec = initializeNaturalTrigger(workflow);
    storage.storeWorkflow(workflow);
    storage.updateNextNaturalTrigger(workflow.id(), triggerInstantSpec);
  }

  void workflowDeleted(Workflow workflow) throws IOException {
    storage.delete(workflow.id());
  }

  private TriggerInstantSpec initializeNaturalTrigger(Workflow workflow) {
    // TODO: duplicate of WorkflowInitializer.initializeNaturalTrigger
    final Instant now = time.get();
    final Instant offsetNow = workflow.configuration().subtractOffset(now);
    final Schedule schedule = workflow.configuration().schedule();
    final Instant nextTrigger = nextInstant(offsetNow, schedule);
    final Instant nextWithOffset = workflow.configuration().addOffset(nextTrigger);
    return TriggerInstantSpec.create(nextTrigger, nextWithOffset);
  }

  /**
   * Fast forwards the time without any execution in-between.
   */
  void timeJumps(int n, TimeUnit unit) {
    LOG.info("{} {} passes", n, unit);
    now = now.plusMillis(unit.toMillis(n));
    printTime();
  }

  /**
   * Fast forwards the time by executing all tasks in-between according to the executor's delay.
   */
  void timePasses(long n, TimeUnit unit) {
    LOG.info("{} {} passes", n, unit);
    now = now.plusMillis(unit.toMillis(n));
    executor.tick(n, unit);
    printTime();
  }

  void givenWorkflowEnabledStateIs(Workflow workflow, boolean enabled) {
    try {
      storage.patchState(workflow.id(), patchEnabled(enabled));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  void givenActiveState(WorkflowInstance workflowInstance, long count) {
    try {
      storage.writeActiveState(workflowInstance, RunState.create(workflowInstance,
          RunState.State.QUEUED, StateData.newBuilder().retryDelayMillis(0L).build(), now, count));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  void givenActiveState(WorkflowInstance workflowInstance, RunState state) {
    try {
      storage.writeActiveState(workflowInstance, state);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  void givenStoredEvent(Event event, long count) {
    try {
      storage.writeEvent(SequenceEvent.create(event, count, now.toEpochMilli()));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  void givenStoredEventAtTime(Event event, long count, long time) {
    try {
      storage.writeEvent(SequenceEvent.create(event, count, time));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  void styxStarts() throws Exception {
    LOG.info("Styx service starts");
    serviceHelper.start();
  }

  long timeOffsetSeconds(int secondsOffset) {
    return now.plusSeconds(secondsOffset).toEpochMilli();
  }

  void awaitBackfillCompleted(String id) {
    await().atMost(30, SECONDS).until(() -> {
      final Optional<Backfill> backfill = storage.backfill(id);
      return backfill.isPresent() && backfill.get().allTriggered();
    });
  }

  void awaitNumberOfDockerRuns(int n) {
    awaitNumberOfDockerRuns(i -> i == n);
  }

  void awaitNumberOfDockerRunsAtLeast(int n) {
    awaitNumberOfDockerRuns(i -> i >= n);
  }

  void awaitNumberOfDockerRuns(IntPredicate predicate) {
    await().atMost(30, SECONDS).until(() -> predicate.test(dockerRuns.size()));
  }

  void awaitWorkflowInstanceState(WorkflowInstance instance, RunState.State state) {
    await().atMost(30, SECONDS).until(() -> {
      final Optional<RunState> runState = getState(instance);
      return runState.isPresent() && runState.get().state() == state;
    });
  }

  void awaitWorkflowInstanceCompletion(WorkflowInstance workflowInstance) {
    await().atMost(30, SECONDS).until(() -> !getState(workflowInstance).isPresent());
  }

  void awaitUntilConsumedEvent(SequenceEvent sequenceEvent, RunState.State state) {
    await().atMost(30, SECONDS).until(() ->
        transitionedEvents.contains(Tuple.of(sequenceEvent, state)));
  }

  private void printTime() {
    LOG.info("The time is {}", now);
  }

  private DockerRunner fakeDockerRunner() {
    return new DockerRunner() {

      @Override
      public void start(WorkflowInstance workflowInstance, RunSpec runSpec) {
        dockerRuns.add(Tuple.of(workflowInstance, runSpec));
      }

      @Override
      public void poll(RunState runState) {
        dockerPolls.add(runState);
      }

      @Override
      public void cleanup() {
        // nop
      }

      @Override
      public void cleanup(WorkflowInstance workflowInstance, String executionId) {
        dockerCleans.add(executionId);
      }

      @Override
      public void close() {
        // nop
      }
    };
  }

  private Connection setupBigTableMockTable(int numFailures) {
    Connection bigtable = mock(Connection.class);
    try {
      new BigtableMocker(bigtable)
          .setNumFailures(numFailures)
          .setupTable(BigtableStorage.EVENTS_TABLE_NAME)
          .finalizeMocking();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return bigtable;
  }
}
