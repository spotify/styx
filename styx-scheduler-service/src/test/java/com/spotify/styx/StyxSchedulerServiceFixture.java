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
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.mock;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.spotify.apollo.test.ServiceHelper;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.publisher.Publisher;
import com.spotify.styx.serialization.PersistentWorkflowInstanceState;
import com.spotify.styx.state.RunState;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.BigtableMocker;
import com.spotify.styx.storage.BigtableStorage;
import com.spotify.styx.util.IsClosedException;
import com.spotify.styx.util.StorageFactory;
import com.spotify.styx.util.Time;
import com.spotify.styx.util.TriggerInstantSpec;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import javaslang.Tuple;
import javaslang.Tuple2;
import org.apache.hadoop.hbase.client.Connection;
import org.jmock.lib.concurrent.DeterministicScheduler;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A test fixture for system tests that exercise all of Styx in isolation from external systems.
 */
public class StyxSchedulerServiceFixture {

  private static final Logger LOG = LoggerFactory.getLogger(StyxSchedulerServiceFixture.class);

  private static LocalDatastoreHelper localDatastore;

  private Datastore datastore = localDatastore.getOptions().getService();
  private Connection bigtable = setupBigTableMockTable(0);
  protected AggregateStorage storage = new AggregateStorage(bigtable, datastore, Duration.ZERO);
  private DeterministicScheduler executor = new QuietDeterministicScheduler();

  // circumstantial fields, set by test cases
  private Instant now = Instant.parse("1970-01-01T00:00:00Z");

  private List<Tuple2<SequenceEvent, RunState.State>> transitionedEvents = Lists.newArrayList();
  private List<Tuple2<Optional<Workflow>, Optional<Workflow>>> workflowChanges = Lists.newArrayList();

  // captured fields from fakes
  List<Tuple2<WorkflowInstance, DockerRunner.RunSpec>> dockerRuns = Lists.newArrayList();
  List<String> dockerCleans = Lists.newArrayList();
  AtomicInteger dockerRestores = new AtomicInteger();

  // service and helper
  private StyxScheduler styxScheduler;
  private ServiceHelper serviceHelper;

  @BeforeClass
  public static void setUpClass() throws Exception {
    // TODO: the datastore emulator behavior wrt conflicts etc differs from the real datastore
    localDatastore = LocalDatastoreHelper.create(1.0); // 100% global consistency
    localDatastore.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (localDatastore != null) {
      try {
        localDatastore.stop(org.threeten.bp.Duration.ofSeconds(30));
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    StorageFactory storageFactory = (env) -> storage;
    Time time = () -> now;
    StyxScheduler.StatsFactory statsFactory = (env) -> Stats.NOOP;
    StyxScheduler.ExecutorFactory executorFactory = (ts, tf) -> executor;
    StyxScheduler.PublisherFactory publisherFactory = (env) -> Publisher.NOOP;
    StyxScheduler.DockerRunnerFactory dockerRunnerFactory =
        (id, env, states, exec, stats, debug) -> fakeDockerRunner();
    StyxScheduler.EventConsumerFactory eventConsumerFactory =
        (env, stats) -> (event, state) ->  transitionedEvents.add(Tuple.of(event, state.state()));
    StyxScheduler.WorkflowConsumerFactory workflowConsumerFactory =
        (env, stats) -> (oldWorkflow, newWorkflow) ->
            workflowChanges.add(Tuple.of(oldWorkflow, newWorkflow));


    styxScheduler = StyxScheduler.newBuilder()
        .setTime(time)
        .setStorageFactory(storageFactory)
        .setDockerRunnerFactory(dockerRunnerFactory)
        .setStatsFactory(statsFactory)
        .setExecutorFactory(executorFactory)
        .setPublisherFactory(publisherFactory)
        .setEventConsumerFactory(eventConsumerFactory)
        .setWorkflowConsumerFactory(workflowConsumerFactory)
        .build();

    serviceHelper = ServiceHelper.create(styxScheduler, StyxScheduler.SERVICE_NAME);
  }

  @After
  public void tearDown() throws Exception {
    serviceHelper.close();

    // clear datastore after each test
    Datastore datastore = localDatastore.getOptions().getService();
    KeyQuery query = Query.newKeyQueryBuilder().build();
    final QueryResults<Key> keys = datastore.run(query);
    while (keys.hasNext()) {
      datastore.delete(keys.next());
    }
  }

  void injectEvent(Event event) throws IsClosedException, InterruptedException, ExecutionException, TimeoutException {
    styxScheduler.receive(event).toCompletableFuture().get(1, MINUTES);
  }

  RunState getState(WorkflowInstance workflowInstance) {
    return styxScheduler.getState(workflowInstance);
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

  void givenResource(Resource resource) throws IOException {
    storage.storeResource(resource);
  }

  void workflowChanges(Workflow workflow) {
    styxScheduler.getWorkflowChangeListener().accept(workflow);
  }

  void workflowDeleted(Workflow workflow) {
    styxScheduler.getWorkflowRemoveListener().accept(workflow);
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
  void timePasses(int n, TimeUnit unit) {
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

  void givenActiveStateAtSequenceCount(WorkflowInstance workflowInstance, long count) {
    try {
      storage.writeActiveState(workflowInstance, PersistentWorkflowInstanceState.of(count));
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  void givenActiveStateAtSequenceCount(WorkflowInstance workflowInstance, PersistentWorkflowInstanceState state) {
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
    await().atMost(30, SECONDS).until(() -> dockerRuns.size() == n);
  }

  void awaitWorkflowInstanceState(WorkflowInstance instance, RunState.State state) {
    await().atMost(30, SECONDS).until(() -> {
      final RunState runState = getState(instance);
      return runState != null && runState.state() == state;
    });
  }

  void awaitWorkflowInstanceCompletion(WorkflowInstance workflowInstance) {
    await().atMost(30, SECONDS).until(() -> getState(workflowInstance) == null);
  }

  void awaitUntilConsumedEvent(SequenceEvent sequenceEvent, RunState.State state) {
    await().atMost(30, SECONDS).until(() ->
        transitionedEvents.contains(Tuple.of(sequenceEvent, state)));
  }

  void awaitUntilConsumedWorkflow(Optional<Workflow> oldWorkflow, Optional<Workflow> newWorkflow) {
    await().atMost(30, SECONDS).until(() ->
        workflowChanges.contains(Tuple.of(oldWorkflow, newWorkflow)));
  }

  private void printTime() {
    LOG.info("The time is {}", now);
  }
  
  private DockerRunner fakeDockerRunner() {
    return new DockerRunner() {
      @Override
      public void restore() {
        dockerRestores.incrementAndGet();
      }

      @Override
      public void start(WorkflowInstance workflowInstance, RunSpec runSpec) {
        dockerRuns.add(Tuple.of(workflowInstance, runSpec));
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
