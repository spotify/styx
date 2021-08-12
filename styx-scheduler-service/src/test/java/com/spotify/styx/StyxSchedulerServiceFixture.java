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
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import com.google.cloud.datastore.Datastore;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.spotify.apollo.test.ServiceHelper;
import com.spotify.styx.api.ActionAuthorizer;
import com.spotify.styx.api.Authenticator;
import com.spotify.styx.api.AuthenticatorFactory;
import com.spotify.styx.api.ServiceAccountUsageAuthorizer;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.docker.DockerRunner.RunSpec;
import com.spotify.styx.flyte.FlyteExecutionId;
import com.spotify.styx.flyte.FlyteRunner;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.FlyteExecConf;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.monitoring.StatsFactory;
import com.spotify.styx.publisher.Publisher;
import com.spotify.styx.state.RunState;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.BigtableMocker;
import com.spotify.styx.storage.BigtableStorage;
import com.spotify.styx.storage.DatastoreEmulator;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.StorageFactory;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerInstantSpec;
import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.IntPredicate;
import javaslang.Tuple;
import javaslang.Tuple2;
import org.apache.hadoop.hbase.client.Connection;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test fixture for system tests that exercise all of Styx in isolation from external systems.
 */
public class StyxSchedulerServiceFixture {

  private static final Logger LOG = LoggerFactory.getLogger(StyxSchedulerServiceFixture.class);
  
  private static ScheduledFuture<?> scheduledFuture;

  private Instant now = Instant.parse("1970-01-01T00:00:00Z");

  @Rule public final DatastoreEmulator datastoreEmulator = new DatastoreEmulator();

  private final Time time = () -> now;

  private final Connection bigtable = setupBigTableMockTable(0);
  protected AggregateStorage storage;
  private final DeterministicScheduler executor = new QuietDeterministicScheduler();
  private final Set<String> resourceIdsToDecorateWith = Sets.newHashSet();

  // circumstantial fields, set by test cases

  private final List<Tuple2<SequenceEvent, RunState.State>> transitionedEvents = Lists.newArrayList();

  // captured fields from fakes
  private final Queue<Tuple2<RunState, RunSpec>> dockerRuns = new ConcurrentLinkedQueue<>();
  final Queue<RunState> dockerPolls = new ConcurrentLinkedQueue<>();
  final Queue<FlyteExecutionId> flyteExecCreations = new ConcurrentLinkedQueue<>();

  // service and helper
  private StyxScheduler styxScheduler;
  private ServiceHelper serviceHelper;

  @BeforeClass
  public static void setUpClass() {
    // Schedule a full GC to run every second to mitigate off-heap/direct memory usage.
    // Without this, the system tests cause the test process memory usage (according to the system) to
    // balloon to several GB even though the JVM itself is configured to and reports that it is only using a few
    // hundred MB.
    // (T-T)
    scheduledFuture = Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(System::gc, 1, 1, SECONDS);
  }
  
  @AfterClass
  public static void tearDownClass() {
    scheduledFuture.cancel(true);
  }

  @Before
  public void setUp() {

    final Datastore datastore = datastoreEmulator.client();
    storage = new AggregateStorage(bigtable, datastore);

    StorageFactory storageFactory = (env, stats) -> storage;
    StatsFactory statsFactory = (env) -> Stats.NOOP;
    StyxScheduler.ExecutorFactory executorFactory = (ts, tf) -> executor;
    StyxScheduler.PublisherFactory publisherFactory = (env) -> Publisher.NOOP;
    StyxScheduler.DockerRunnerFactory dockerRunnerFactory =
        (id, env, states, stats, debug, podMutator, time) -> fakeDockerRunner();
    StyxScheduler.FlyteRunnerFactory flyteRunnerFactory =
        (id, config, stateManager, interceptors) -> fakeFlyteRunner();
    WorkflowResourceDecorator resourceDecorator = (rs, cfg, res) ->
        Sets.union(res, resourceIdsToDecorateWith);
    StyxScheduler.EventConsumerFactory eventConsumerFactory =
        (env, stats) -> (event, state) ->  transitionedEvents.add(Tuple.of(event, state.state()));
    AuthenticatorFactory authenticatorFactory = (cfg) -> mock(Authenticator.class);

    final ServiceAccountUsageAuthorizer.Factory serviceAccountUsageAuthorizerFactory =
        (cfg, name) -> ServiceAccountUsageAuthorizer.nop();
    final ActionAuthorizer.Factory actionAuthorizerFactory = ActionAuthorizer.Factory.DEFAULT;
    styxScheduler = StyxScheduler.newBuilder()
        .setTime(time)
        .setStorageFactory(storageFactory)
        .setDockerRunnerFactory(dockerRunnerFactory)
        .setFlyteRunnerFactory(flyteRunnerFactory)
        .setStatsFactory(statsFactory)
        .setExecutorFactory(executorFactory)
        .setPublisherFactory(publisherFactory)
        .setResourceDecorator(resourceDecorator)
        .setEventConsumerFactory(eventConsumerFactory)
        .setAuthenticatorFactory(authenticatorFactory)
        .setServiceAccountUsageAuthorizerFactory(serviceAccountUsageAuthorizerFactory)
        .setActionAuthorizerFactory(actionAuthorizerFactory)
        .build();

    serviceHelper = ServiceHelper.create(styxScheduler, StyxScheduler.SERVICE_NAME)
        .startTimeoutSeconds(30);
  }

  @After
  public void tearDown() throws Exception {
    serviceHelper.close();
  }

  void injectEvent(Event event) throws IsClosedException {
    styxScheduler.receive(event);
  }

  Optional<RunState> getState(WorkflowInstance workflowInstance) {
    return styxScheduler.getState(workflowInstance);
  }

  /**
   * @return a best effort snapshot, without throwing ConcurrentModificationException.
   */
  List<Tuple2<RunState, RunSpec>> getDockerRuns() {
    return Lists.newArrayList(dockerRuns);
  }

  void tickScheduler() {
    styxScheduler.tickScheduler();
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

  void givenNextNaturalTrigger(Workflow workflow, String nextNaturalTrigger) throws IOException {
    Instant next = Instant.parse(nextNaturalTrigger);
    Instant offset = workflow.configuration().addOffset(next);
    TriggerInstantSpec spec = TriggerInstantSpec.create(next, offset);

    storage.updateNextNaturalTrigger(workflow.id(), spec);
  }

  void givenBackfill(Backfill backfill) throws IOException {
    storage.storeBackfill(backfill);
  }

  void workflowChanges(Workflow workflow) throws IOException {
    final TriggerInstantSpec triggerInstantSpec = initializeNaturalTrigger(workflow);
    storage.storeWorkflow(workflow);
    storage.updateNextNaturalTrigger(workflow.id(), triggerInstantSpec);
  }

  void workflowDeleted(Workflow workflow) throws IOException {
    storage.delete(workflow.id());
  }

  // duplicate of WorkflowInitializer.initializeNaturalTrigger
  private TriggerInstantSpec initializeNaturalTrigger(Workflow workflow) {
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
      throw new RuntimeException(e);
    }
  }

  void givenActiveState(WorkflowInstance workflowInstance, RunState state) {
    try {
      storage.writeActiveState(workflowInstance, state);
    } catch (IOException e) {
      throw new RuntimeException(e);
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
    awaitNumberOfDockerRuns0(i -> i == n);
  }

  void awaitNumberOfDockerRunsAtLeast(int n) {
    awaitNumberOfDockerRuns0(i -> i >= n);
  }

  private void awaitNumberOfDockerRuns0(IntPredicate predicate) {
    await().atMost(30, SECONDS).until(() -> predicate.test(dockerRuns.size()));
  }

  void awaitWorkflowInstanceState(WorkflowInstance instance, RunState.State state) {
    await().atMost(30, SECONDS).until(() -> {
      final Optional<RunState> runState = getState(instance);
      return runState.isPresent() && runState.get().state() == state;
    });
  }

  void awaitWorkflowInstanceCompletion(WorkflowInstance workflowInstance) {
    await().atMost(30, SECONDS).until(() -> getState(workflowInstance).isEmpty());
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
      public String start(RunState runState, RunSpec runSpec) {
        dockerRuns.add(Tuple.of(runState, runSpec));
        return "fake";
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
      public void close() {
        // nop
      }
    };
  }

  private FlyteRunner fakeFlyteRunner() {
    return new FlyteRunner() {
      @Override
      public String createExecution(RunState runState, String name, FlyteExecConf flyteExecConf) {
        final FlyteExecutionId response = FlyteExecutionId.create(flyteExecConf.referenceId().project(),
                flyteExecConf.referenceId().domain(), name);
        flyteExecCreations.add(response);
        return "fake-runner-id";
      }

      @Override
      public void terminateExecution(RunState runState, FlyteExecutionId flyteExecutionId) {
        // do nothing
      }

      @Override
      public void poll(FlyteExecutionId flyteExecutionId, RunState runState) {
        // do nothing
      }

      @Override
      public void close() {
        // do nothing
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
      throw new RuntimeException(e);
    }
    return bigtable;
  }
}
