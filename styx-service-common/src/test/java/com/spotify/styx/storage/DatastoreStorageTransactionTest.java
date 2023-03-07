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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.spy;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DatastoreStorageTransactionTest {

  @ClassRule public static final DatastoreEmulator datastoreEmulator = new DatastoreEmulator();

  private DatastoreStorage storage;

  private ExecutorService executor = Executors.newCachedThreadPool();

  @Before
  public void setUp() throws Exception {
    var datastore = spy(new CheckedDatastore(datastoreEmulator.client()));
    storage = new DatastoreStorage(datastore);
  }

  @After
  public void tearDown() {
    datastoreEmulator.reset();
    executor.shutdownNow();
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
    assertThat(storage.shardsForCounter("res1").size(), is(2));
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
        .created(Instant.parse("2019-01-01T00:00:00Z"))
        .lastModified(Instant.parse("2019-06-01T00:00:00Z"))
        .build();
    storage.runInTransactionWithRetries(tx -> tx.store(backfill));
    var read = storage.runInTransactionWithRetries(tx -> tx.backfill(backfill.id()));
    assertThat(read, is(Optional.of(backfill)));
  }
}
