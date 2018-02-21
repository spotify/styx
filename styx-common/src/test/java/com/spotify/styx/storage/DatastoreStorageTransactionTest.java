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
import static com.spotify.styx.storage.DatastoreStorageTest.PERSISTENT_STATE;
import static com.spotify.styx.storage.DatastoreStorageTest.PERSISTENT_STATE1;
import static com.spotify.styx.storage.DatastoreStorageTest.WORKFLOW;
import static com.spotify.styx.storage.DatastoreStorageTest.WORKFLOW_INSTANCE1;
import static com.spotify.styx.testdata.TestData.FULL_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.Transaction;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.serialization.PersistentWorkflowInstanceState;
import com.spotify.styx.util.TriggerInstantSpec;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DatastoreStorageTransactionTest {

  private static LocalDatastoreHelper helper;
  private static Datastore datastore;
  private DatastoreStorage storage;

  @BeforeClass
  public static void setUpClass() throws Exception {
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
    datastore = helper.getOptions().getService();
    storage = new DatastoreStorage(datastore, Duration.ZERO);
  }

  @After
  public void tearDown() throws Exception {
    // clear datastore after each test
    KeyQuery query = Query.newKeyQueryBuilder().build();
    final QueryResults<Key> keys = datastore.run(query);
    while (keys.hasNext()) {
      datastore.delete(keys.next());
    }
  }

  @Test
  public void shouldCommitEmptyTransaction() throws IOException {
    final Transaction transaction = datastore.newTransaction();
    DatastoreStorageTransaction storageTransaction = new DatastoreStorageTransaction(transaction);
    storageTransaction.commit();
    assertFalse(transaction.isActive());
  }

  @Test
  public void shouldThrowIfUnexpectedDatastoreError() throws IOException {
    final Transaction transaction = datastore.newTransaction();
    DatastoreStorageTransaction storageTransaction = new DatastoreStorageTransaction(transaction);

    transaction.rollback();
    try {
      storageTransaction.commit();
      fail("Expected exception!");
    } catch (TransactionException e) {
      assertFalse(e.isConflict());
    }
  }

  @Test
  public void shouldThrowIfRollbackFails() throws IOException {
    final Transaction transaction = datastore.newTransaction();
    DatastoreStorageTransaction storageTransaction = new DatastoreStorageTransaction(transaction);

    storageTransaction.commit();
    try {
      storageTransaction.rollback();
      fail("Expected exception!");
    } catch (TransactionException e) {
      assertFalse(e.isConflict());
    }
  }

  @Test
  public void shouldThrowIfTransactionFailed() throws IOException, InterruptedException {
    final Transaction transaction1 = datastore.newTransaction();
    final Transaction transaction2 = datastore.newTransaction();
    final Transaction transaction3 = datastore.newTransaction();
    DatastoreStorageTransaction storageTransaction1 = new DatastoreStorageTransaction(transaction1);
    DatastoreStorageTransaction storageTransaction2 = new DatastoreStorageTransaction(transaction2);
    DatastoreStorageTransaction storageTransaction3 = new DatastoreStorageTransaction(transaction3);

    // Store first entity
    KeyFactory keyFactory1 = datastore.newKeyFactory().setKind("MyKind");
    Key key1 = datastore.allocateId(keyFactory1.newKey());
    Entity entity1 = Entity.newBuilder(key1).set("key", "firstWrite").build();
    transaction1.put(entity1);
    storageTransaction1.commit();

    // Read first entity then commit a change to the first entity
    Entity entity1read = transaction3.get(key1);
    Entity entity1modified = Entity.newBuilder(key1).set("key", "secondWrite").build();
    transaction2.put(entity1modified);
    storageTransaction2.commit();

    // Used the read first entity to generate a second entity (copy of the first entity)
    KeyFactory keyFactory2 = datastore.newKeyFactory().setKind("MyKindCopy");
    Key key2 = datastore.allocateId(keyFactory2.newKey());
    Entity copyOfEntity1 = Entity.newBuilder(key2).set("key", entity1read.getString("key")).build();
    transaction3.put(copyOfEntity1);

    try {
      storageTransaction3.commit();
      fail("Expected exception!");
    } catch (TransactionException e) {
      assertTrue(e.isConflict());
      transaction3.rollback();
    }
  }

  @Test
  public void insertActiveStateShouldFailIfAlreadyExists() throws Exception {
    DatastoreStorageTransaction tx = new DatastoreStorageTransaction(datastore.newTransaction());
    tx.insertActiveState(WORKFLOW_INSTANCE1, PERSISTENT_STATE1);
    tx.commit();
    tx = new DatastoreStorageTransaction(datastore.newTransaction());
    tx.insertActiveState(WORKFLOW_INSTANCE1, PERSISTENT_STATE1);
    try {
      tx.commit();
      fail("Expected exception!");
    } catch (TransactionException e) {
      assertThat(e.isAlreadyExists(), is(true));
    }
  }

  @Test
  public void updateActiveStateShouldFailIfNotFound() throws Exception {
    DatastoreStorageTransaction tx = new DatastoreStorageTransaction(datastore.newTransaction());
    tx.updateActiveState(WORKFLOW_INSTANCE1, PERSISTENT_STATE1);
    try {
      tx.commit();
      fail("Expected exception!");
    } catch (TransactionException e) {
      assertThat(e.isNotFound(), is(true));
    }
  }

  @Test
  public void shouldStoreWorkflow() throws IOException {
    DatastoreStorageTransaction tx = new DatastoreStorageTransaction(datastore.newTransaction());
    Workflow workflow = Workflow.create("test", FULL_WORKFLOW_CONFIGURATION);
    tx.store(workflow);
    tx.commit();
    assertThat(storage.workflow(workflow.id()), is(Optional.of(workflow)));
  }

  @Test
  public void shouldGetWorkflow() throws IOException {
    Workflow workflow = Workflow.create("test", FULL_WORKFLOW_CONFIGURATION);
    storage.store(workflow);
    DatastoreStorageTransaction tx = new DatastoreStorageTransaction(datastore.newTransaction());
    Optional<Workflow> workflowOptional = tx.workflow(workflow.id());
    tx.commit();
    assertThat(workflowOptional, is(Optional.of(workflow)));
  }

  @Test
  public void shouldPersistNextScheduledRun() throws Exception {
    Instant instant = Instant.parse("2016-03-14T14:00:00Z");
    Instant offset = instant.plus(1, ChronoUnit.DAYS);
    TriggerInstantSpec spec = TriggerInstantSpec.create(instant, offset);
    DatastoreStorageTransaction tx = new DatastoreStorageTransaction(datastore.newTransaction());
    tx.store(WORKFLOW);
    tx.commit();
    tx = new DatastoreStorageTransaction(datastore.newTransaction());
    tx.updateNextNaturalTrigger(WORKFLOW.id(), spec);
    tx.commit();

    Map<Workflow, TriggerInstantSpec> result = storage.workflowsWithNextNaturalTrigger();
    assertThat(result.values().size(), is(1));
    assertThat(result, hasEntry(WORKFLOW, spec));
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
    DatastoreStorageTransaction tx = new DatastoreStorageTransaction(datastore.newTransaction());
    tx.patchState(WORKFLOW.id(), state);
    tx.commit();
    WorkflowState retrieved = storage.workflowState(WORKFLOW.id());

    assertThat(retrieved, is(state));
  }

  @Test
  public void shouldReturnAllActiveStateForWFI() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE, PERSISTENT_STATE);
    DatastoreStorageTransaction tx = new DatastoreStorageTransaction(datastore.newTransaction());
    Optional<PersistentWorkflowInstanceState> activeStates =
        tx.activeState(WORKFLOW_INSTANCE);
    tx.commit();

    assertThat(activeStates, is(Optional.of(PERSISTENT_STATE)));
  }

  @Test
  public void shouldInsertActiveState() throws Exception {
    DatastoreStorageTransaction tx = new DatastoreStorageTransaction(datastore.newTransaction());
    tx.insertActiveState(WORKFLOW_INSTANCE, PERSISTENT_STATE);
    tx.commit();

    assertThat(storage.activeState(WORKFLOW_INSTANCE), is(Optional.of(PERSISTENT_STATE)));
  }

  @Test
  public void shouldUpdateActiveState() throws Exception {
    DatastoreStorageTransaction tx = new DatastoreStorageTransaction(datastore.newTransaction());
    tx.insertActiveState(WORKFLOW_INSTANCE, PERSISTENT_STATE);
    tx.commit();
    tx = new DatastoreStorageTransaction(datastore.newTransaction());
    PersistentWorkflowInstanceState newPersistedState =
        PERSISTENT_STATE.toBuilder().counter(PERSISTENT_STATE.counter() + 1).build();
    tx.updateActiveState(WORKFLOW_INSTANCE, newPersistedState);
    tx.commit();

    assertThat(storage.activeState(WORKFLOW_INSTANCE), is(Optional.of(newPersistedState)));
  }

  @Test
  public void shouldDeleteActiveState() throws Exception {
    DatastoreStorageTransaction tx = new DatastoreStorageTransaction(datastore.newTransaction());
    tx.insertActiveState(WORKFLOW_INSTANCE, PERSISTENT_STATE);
    tx.commit();
    tx = new DatastoreStorageTransaction(datastore.newTransaction());
    tx.deleteActiveState(WORKFLOW_INSTANCE);
    tx.commit();

    assertThat(storage.activeState(WORKFLOW_INSTANCE), is(Optional.empty()));
  }

  @Test
  public void shouldStoreBackfill() throws IOException {
    DatastoreStorageTransaction tx = new DatastoreStorageTransaction(datastore.newTransaction());
    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-1")
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-02T00:00:00Z"))
        .workflowId(WorkflowId.create("component", "workflow1"))
        .concurrency(2)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(DAYS)
        .build();
    tx.store(backfill);
    tx.commit();
    assertThat(storage.getBackfill(backfill.id()), is(Optional.of(backfill)));
  }
}
