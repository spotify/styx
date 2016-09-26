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
package com.spotify.styx;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.datastore.Datastore;

import com.spotify.apollo.test.ServiceHelper;
import com.spotify.styx.docker.DockerRunner;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.publisher.Publisher;
import com.spotify.styx.schedule.ScheduleSourceFactory;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.util.EventStorageFactory;
import com.spotify.styx.util.Singleton;
import com.spotify.styx.util.StorageFactory;
import com.spotify.styx.util.Time;

import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

import static java.util.Collections.singletonList;

/**
 * A test program to ensure that Styx can restore all the state and events that are in Bigtable.
 *
 * Run this through styx-scheduler-service/bin/test-prod-restore.sh
 * Ensure that you have a GOOGLE_APPLICATION_CREDENTIALS env var set pointing to a credentials
 * file that has access to the cluster configured in {@link #createBigTableConnection()}.
 */
public class ProductionStateRestore {

  private static final Logger LOG = LoggerFactory.getLogger(ProductionStateRestore.class);
  private static final String TEST_EXECUTION_ID = "execution_1";

  // circumstantial fields, set by test cases
  protected Instant now = Instant.parse("2016-06-01T00:00:00Z");

  // service and helper
  private StyxScheduler styxScheduler;
  private ServiceHelper serviceHelper;

  public static void main(String[] args) throws Exception {
    ProductionStateRestore test = new ProductionStateRestore();

    test.setUp();
    try {
      test.testStartsAndRestoresStatesFromBigTable();
    } finally {
      test.tearDown();
    }
  }

  public void setUp() throws Exception {
    Singleton<AggregateStorage> storage = Singleton.create((env) -> {
      Connection bigtable = createBigTableConnection();
      Datastore datastore = StyxScheduler.createDatastore(env.config());
      return new AggregateStorage(bigtable, datastore, Duration.ZERO);
    });

    StorageFactory storageFactory = storage::apply;
    EventStorageFactory eventStorageFactory = storage::apply;
    StyxScheduler.DockerRunnerFactory dockerRunnerFactory = (env, states, exec, stats) -> fakeDockerRunner();
    StyxScheduler.ScheduleSources scheduleSources = () -> singletonList(fakeScheduleSource());
    StyxScheduler.StatsFactory statsFactory = (env) -> Stats.NOOP;
    StyxScheduler.ExecutorFactory executorFactory = (ts, tf) -> new QuietDeterministicScheduler();
    StyxScheduler.PublisherFactory publisherFactory = (env) -> Publisher.NOOP;
    Time time = () -> now;

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

    serviceHelper = ServiceHelper.create(styxScheduler, "styx-scheduler")
        .conf(StyxScheduler.DATASTORE_NAMESPACE, "styx-test")
        .startTimeoutSeconds(5 * 60);
  }

  public void tearDown() throws Exception {
    serviceHelper.close();
  }

  public void testStartsAndRestoresStatesFromBigTable() throws Exception {
    LOG.info("Styx service starts");
    serviceHelper.start();
  }

  private Connection createBigTableConnection() {
    final String projectId = "steel-ridge-91615";
    final String clusterId = "styx-test";

    LOG.info("Creating Bigtable connection for project:{}, cluster:{}",
             projectId, clusterId);

    return BigtableConfiguration.connect(projectId, clusterId);
  }

  private ScheduleSourceFactory fakeScheduleSource() {
    return (changeListener, removeListener, environment, exec) -> () -> {};
  }

  private DockerRunner fakeDockerRunner() {
    return new DockerRunner() {
      @Override
      public String start(WorkflowInstance workflowInstance, RunSpec runSpec) {
        return TEST_EXECUTION_ID;
      }

      @Override
      public void cleanup(String executionId) {
      }

      @Override
      public void close() throws IOException {
      }
    };
  }
}
