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
import static java.util.Collections.singletonList;
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
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.publisher.Publisher;
import com.spotify.styx.schedule.ScheduleSourceFactory;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateManager;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.BigtableMocker;
import com.spotify.styx.storage.BigtableStorage;
import com.spotify.styx.util.EventStorageFactory;
import com.spotify.styx.util.StorageFactory;
import com.spotify.styx.util.Time;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
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

  static final String TEST_EXECUTION_ID = "execution_1";

  private static LocalDatastoreHelper localDatastore;

  private Datastore datastore = localDatastore.options().service();
  private Connection bigtable = setupBigTableMockTable(0);
  private AggregateStorage storage = new AggregateStorage(bigtable, datastore, Duration.ZERO);
  private DeterministicScheduler executor = new QuietDeterministicScheduler();
  private Consumer<Workflow> workflowChangeListener;
  private Consumer<Workflow> workflowRemoveListener;

  // circumstantial fields, set by test cases
  private Instant now = Instant.parse("1970-01-01T00:00:00Z");
  private List<Workflow> workflows = Lists.newArrayList();

  // captured fields from fakes
  List<Tuple2<WorkflowInstance, DockerRunner.RunSpec>> dockerRuns = Lists.newArrayList();
  List<String> dockerCleans = Lists.newArrayList();

  // service and helper
  private StyxScheduler styxScheduler;
  private ServiceHelper serviceHelper;

  @BeforeClass
  public static void setUpClass() throws Exception {
    localDatastore = LocalDatastoreHelper.create(1.0); // 100% global consistency
    localDatastore.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (localDatastore != null) {
      localDatastore.stop();
    }
  }

  @Before
  public void setUp() throws Exception {
    StorageFactory storageFactory = (env) -> storage;
    EventStorageFactory eventStorageFactory = (env) -> storage;
    Time time = () -> now;
    StyxScheduler.ScheduleSources scheduleSources = () -> singletonList(fakeScheduleSource());
    StyxScheduler.StatsFactory statsFactory = (env) -> Stats.NOOP;
    StyxScheduler.ExecutorFactory executorFactory = (ts, tf) -> executor;
    StyxScheduler.PublisherFactory publisherFactory = (env) -> Publisher.NOOP;
    StyxScheduler.DockerRunnerFactory dockerRunnerFactory =
        (id, env, states, exec, stats) -> fakeDockerRunner();

    styxScheduler = StyxScheduler.newBuilder()
        .setTime(time)
        .setStorageFactory(storageFactory)
        .setEventStorageFactory(eventStorageFactory)
        .setDockerRunnerFactory(dockerRunnerFactory)
        .setScheduleSources(scheduleSources)
        .setStatsFactory(statsFactory)
        .setExecutorFactory(executorFactory)
        .setPublisherFactory(publisherFactory)
        .build();

    serviceHelper = ServiceHelper.create(styxScheduler, StyxScheduler.SERVICE_NAME);
  }

  @After
  public void tearDown() throws Exception {
    serviceHelper.close();

    // clear datastore after each test
    Datastore datastore = localDatastore.options().service();
    KeyQuery query = Query.keyQueryBuilder().build();
    final QueryResults<Key> keys = datastore.run(query);
    while (keys.hasNext()) {
      datastore.delete(keys.next());
    }
  }

  void injectEvent(Event event) throws StateManager.IsClosed {
    styxScheduler.receive(event);
  }

  RunState getState(WorkflowInstance workflowInstance) {
    return styxScheduler.getState(workflowInstance);
  }

  void givenTheTimeIs(String time) {
    now = Instant.parse(time);
    printTime();
  }

  void givenWorkflow(Workflow workflow) throws IOException {
    workflows.add(workflow);
    storage.store(workflow);
  }

  void givenNextNaturalTrigger(Workflow workflow, String nextNaturalTrigger) throws IOException {
    storage.updateNextNaturalTrigger(workflow.id(), Instant.parse(nextNaturalTrigger));
  }

  void workflowChanges(Workflow workflow) {
    workflowChangeListener.accept(workflow);
  }

  void workflowDeleted(Workflow workflow) {
    workflowRemoveListener.accept(workflow);
  }

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

  void givenTheGlobalEnableFlagIs(boolean enabled) {
    try {
      storage.setGlobalEnabled(enabled);
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  void givenActiveStateAtSequenceCount(WorkflowInstance workflowInstance, long count) {
    try {
      storage.writeActiveState(workflowInstance, count);
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

  void awaitNumberOfDockerRuns(int n) {
    await().until(() -> dockerRuns.size() == n);
  }

  void awaitWorkflowInstanceState(WorkflowInstance instance, RunState.State state) {
    await().until(() -> {
      final RunState runState = getState(instance);
      return runState != null && runState.state() == state;
    });
  }

  private void printTime() {
    LOG.info("The time is {}", now);
  }

  private ScheduleSourceFactory fakeScheduleSource() {
    return (changeListener, removeListener, environment, exec) ->
        /* start */ () -> {
      workflowChangeListener = changeListener;
      workflows.forEach(changeListener);
      workflowRemoveListener = removeListener;
    };
  }

  private DockerRunner fakeDockerRunner() {
    return new DockerRunner() {
      @Override
      public String start(WorkflowInstance workflowInstance, RunSpec runSpec) {
        dockerRuns.add(Tuple.of(workflowInstance, runSpec));
        return TEST_EXECUTION_ID;
      }

      @Override
      public void cleanup(String executionId) {
        dockerCleans.add(executionId);
      }

      @Override
      public void close() throws IOException {
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
