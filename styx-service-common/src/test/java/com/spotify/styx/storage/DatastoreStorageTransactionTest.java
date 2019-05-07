/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

package com.spotify.styx.storage;

import static com.spotify.styx.model.Schedule.DAYS;
import static com.spotify.styx.state.RunState.State.NEW;
import static com.spotify.styx.storage.DatastoreStorageTest.RUN_STATE;
import static com.spotify.styx.storage.DatastoreStorageTest.RUN_STATE1;
import static com.spotify.styx.storage.DatastoreStorageTest.TIMESTAMP;
import static com.spotify.styx.storage.DatastoreStorageTest.WORKFLOW;
import static com.spotify.styx.storage.DatastoreStorageTest.WORKFLOW_INSTANCE1;
import static com.spotify.styx.testdata.TestData.FULL_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.WORKFLOW_ID;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.ServiceOptions;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.StateData;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.Shard;
import com.spotify.styx.util.TriggerInstantSpec;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.threeten.bp.Duration;

@RunWith(MockitoJUnitRunner.class)
public class DatastoreStorageTransactionTest {

  private static final RetrySettings RETRY_SETTINGS = ServiceOptions.getDefaultRetrySettings().toBuilder()
      .setInitialRetryDelay(Duration.ofMillis(1L))
      .setTotalTimeout(Duration.ofSeconds(5))
      .setMaxAttempts(3)
      .build();

  private static LocalDatastoreHelper helper;
  private static CheckedDatastore datastore;
  private DatastoreStorage storage;

  private ExecutorService executor = Executors.newCachedThreadPool();

  @BeforeClass
  public static void setUpClass() throws Exception {
    final java.util.logging.Logger datastoreEmulatorLogger =
        java.util.logging.Logger.getLogger(LocalDatastoreHelper.class.getName());
    datastoreEmulatorLogger.setLevel(Level.OFF);

    // TODO: the datastore emulator behavior wrt conflicts etc differs from the real datastore
    helper = LocalDatastoreHelper.create(1.0); // 100% global consistency
    helper.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (helper != null) {
      try {
        helper.stop(org.threeten.bp.Duration.ofSeconds(30));
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    datastore = new CheckedDatastore(helper.getOptions().toBuilder()
        .setRetrySettings(RETRY_SETTINGS)
        .build()
        .getService());
    storage = new DatastoreStorage(datastore);
  }

  @After
  public void tearDown() throws Exception {
    helper.reset();
    executor.shutdownNow();
  }

  @Test
  public void shouldRetryOnConflict() throws Exception {
    var workflow = TestData.WORKFLOW_WITH_RESOURCES;

    // Store workflow
    storage.runInTransactionWithRetries(tx -> {
      tx.store(workflow);
      return null;
    });

    // Start a losing transaction that reads, waits for barrier and then stores the workflow
    var runs = new AtomicInteger();
    var barrier = new CompletableFuture<Void>();
    var future = executor.submit(() -> storage.runInTransactionWithRetries(tx -> {
      runs.incrementAndGet();
      var wf = tx.workflow(workflow.id());
      barrier.join();
      tx.store(wf.orElseThrow());
      return null;
    }));

    // Execute a winning read-store transaction
    storage.runInTransactionWithRetries(tx -> {
      var wf = tx.workflow(workflow.id());
      tx.store(wf.orElseThrow());
      return null;
    });
    barrier.complete(null);

    // Wait for first transaction to also complete and verify that it ran twice
    future.get(30, SECONDS);
    assertThat(runs.get(), is(2));
  }

  @Test
  public void shouldGiveUpOnTimeout() throws Exception {
    var workflow = TestData.WORKFLOW_WITH_RESOURCES;

    // Store workflow
    storage.runInTransactionWithRetries(tx -> {
      tx.store(workflow);
      return null;
    });

    // Start a losing transaction that reads, waits for barrier and then stores the workflow
    var runs = new AtomicInteger();
    var barrier = new CompletableFuture<Void>();
    var future = executor.submit(() -> storage.runInTransactionWithRetries(tx -> {
      runs.incrementAndGet();
      var wf = tx.workflow(workflow.id());
      barrier.join();
      Thread.sleep(RETRY_SETTINGS.getTotalTimeout().toMillis());
      tx.store(wf.orElseThrow());
      return null;
    }));

    // Execute a winning read-store transaction
    storage.runInTransactionWithRetries(tx -> {
      var wf = tx.workflow(workflow.id());
      tx.store(wf.orElseThrow());
      return null;
    });
    barrier.complete(null);

    // Wait for first transaction to fail
    future.get(30, SECONDS);
    assertThat(runs.get(), is(1));
  }

  @Test
  public void shouldRetryUntilMaxAttempts() throws Exception {
    var workflow = TestData.WORKFLOW_WITH_RESOURCES;

    // Store workflow
    storage.runInTransactionWithRetries(tx -> {
      tx.store(workflow);
      return null;
    });

    // Start a losing transaction that reads, waits for barrier and then stores the workflow
    var runs = new AtomicInteger();
    var waiting = new LinkedBlockingQueue<Boolean>();
    var proceed = new LinkedBlockingQueue<Boolean>();
    var future = executor.submit(() -> storage.runInTransactionWithRetries(tx -> {
      runs.incrementAndGet();
      var wf = tx.workflow(workflow.id());
      waiting.put(true);
      proceed.take();
      tx.store(wf.orElseThrow());
      return null;
    }));

    // Execute winning read-store transactions to make each retry of the above transaction fail
    for (int i = 0; i < RETRY_SETTINGS.getMaxAttempts(); i++) {
      waiting.take();
      storage.runInTransactionWithRetries(tx -> {
        var wf = tx.workflow(workflow.id());
        tx.store(wf.orElseThrow());
        return null;
      });
      proceed.put(true);
    }

    // Wait for first transaction to give up
    try {
      future.get(300, SECONDS);
      fail("Expected transaction to fail");
    } catch (ExecutionException e) {
      var cause = e.getCause();
      assertThat(cause, instanceOf(DatastoreIOException.class));
      var dex = ((DatastoreIOException) cause).getCause();
      assertThat(dex.getCode(), is(10));
    }

    // Verify that it retried as many times as expected before giving up
    assertThat(runs.get(), is(RETRY_SETTINGS.getMaxAttempts()));
  }

  @Test
  public void shouldStoreAndDeleteWorkflow() throws Exception {
    var workflow = Workflow.create("test", FULL_WORKFLOW_CONFIGURATION);
    storage.runInTransactionWithRetries(tx -> tx.store(workflow));
    assertThat(storage.workflow(workflow.id()), is(Optional.of(workflow)));

    storage.runInTransactionWithRetries(tx -> tx.deleteWorkflow(workflow.id()));

    var deleted = storage.runInTransactionWithRetries(tx -> tx.workflow(workflow.id()));
    assertThat(deleted, is(Optional.empty()));
  }

  @Test
  public void shouldStoreWorkflowWithNextNaturalTrigger() throws Exception {
    var instant = Instant.parse("2016-03-14T14:00:00Z");
    var offset = instant.plus(1, ChronoUnit.DAYS);
    var spec = TriggerInstantSpec.create(instant, offset);

    var workflow = Workflow.create("test", FULL_WORKFLOW_CONFIGURATION);
    storage.runInTransactionWithRetries(tx -> {
      tx.storeWorkflowWithNextNaturalTrigger(workflow, spec);
      return null;
    });

    var workflows = storage.workflowsWithNextNaturalTrigger();
    assertThat(workflows.values().size(), is(1));
    assertThat(workflows, hasEntry(workflow, spec));
  }


  @Test
  public void shouldStoreShards() throws IOException {
    storage.runInTransactionWithRetries(tx -> {
      Shard shard1 = Shard.create("res1", 0, 1);
      Shard shard2 = Shard.create("res1", 1, 1);
      tx.store(shard1);
      tx.store(shard2);
      return null;
    });
    assertEquals(storage.shardsForCounter("res1").size(), 2);
  }

  @Test
  public void shouldGetWorkflow() throws Exception {
    var workflow = TestData.WORKFLOW_WITH_RESOURCES;
    storage.store(workflow);
    var read = storage.runInTransactionWithRetries(tx -> tx.workflow(WORKFLOW_ID));
    assertThat(read, is(Optional.of(workflow)));
  }

  @Test
  public void shouldPatchState() throws Exception {
    storage.store(WORKFLOW);
    Instant instant = Instant.parse("2016-03-14T14:00:00Z");
    Instant offset = instant.plus(1, ChronoUnit.DAYS);
    WorkflowState state = WorkflowState.builder()
        .enabled(true)
        .nextNaturalTrigger(instant)
        .nextNaturalOffsetTrigger(offset)
        .build();
    storage.runInTransactionWithRetries(tx -> tx.patchState(WORKFLOW.id(), state));
    WorkflowState retrieved = storage.workflowState(WORKFLOW.id());

    assertThat(retrieved, is(state));
  }

  @Test
  public void shouldReturnAllActiveStateForWFI() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE1, RUN_STATE);
    var activeStates = storage.runInTransactionWithRetries(tx -> tx.readActiveState(WORKFLOW_INSTANCE1));
    assertThat(activeStates, is(Optional.of(RUN_STATE)));
  }

  @Test
  public void shouldInsertActiveState() throws Exception {
    storage.runInTransactionWithRetries(tx -> tx.writeActiveState(WORKFLOW_INSTANCE1, RUN_STATE));
    assertThat(storage.readActiveState(WORKFLOW_INSTANCE1), is(Optional.of(RUN_STATE)));
  }

  @Test
  public void shouldUpdateActiveState() throws Exception {
    var runState = RunState.create(WORKFLOW_INSTANCE1, NEW, StateData.zero(), TIMESTAMP, 42L);
    storage.runInTransactionWithRetries(tx -> tx.writeActiveState(WORKFLOW_INSTANCE1, runState));
    var newRunState = RunState.create(WORKFLOW_INSTANCE1, NEW, StateData.zero(), TIMESTAMP, 43L);
    storage.runInTransactionWithRetries(tx -> tx.updateActiveState(WORKFLOW_INSTANCE1, newRunState));
    assertThat(storage.readActiveState(WORKFLOW_INSTANCE1), is(Optional.of(newRunState)));
  }

  @Test
  public void shouldDeleteActiveState() throws Exception {
    storage.runInTransactionWithRetries(tx -> tx.writeActiveState(WORKFLOW_INSTANCE, RUN_STATE1));
    storage.runInTransactionWithRetries(tx -> tx.deleteActiveState(WORKFLOW_INSTANCE));
    assertThat(storage.readActiveState(WORKFLOW_INSTANCE), is(Optional.empty()));
  }

  @Test
  public void shouldStoreAndGetBackfill() throws Exception {
    var backfill = Backfill.newBuilder()
        .id("backfill-1")
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-02T00:00:00Z"))
        .workflowId(WorkflowId.create("component", "workflow1"))
        .concurrency(2)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(DAYS)
        .build();
    storage.runInTransactionWithRetries(tx -> tx.store(backfill));
    var read = storage.runInTransactionWithRetries(tx -> tx.backfill(backfill.id()));
    assertThat(read, is(Optional.of(backfill)));
  }
}
