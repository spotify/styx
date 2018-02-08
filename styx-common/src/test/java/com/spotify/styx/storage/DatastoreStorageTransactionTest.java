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

import static com.spotify.styx.storage.DatastoreStorageTest.PERSISTENT_STATE1;
import static com.spotify.styx.storage.DatastoreStorageTest.WORKFLOW_INSTANCE1;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Transaction;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import java.io.IOException;
import java.time.Duration;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DatastoreStorageTransactionTest {

  private static LocalDatastoreHelper helper;
  private static Datastore datastore;

  @BeforeClass
  public static void setUpClass() throws Exception {
    // TODO: the datastore emulator behavior wrt conflicts etc differs from the real datastore
    helper = LocalDatastoreHelper.create(1.0); // 100% global consistency
    helper.start();
  }

  @Before
  public void setUp() {
    datastore = helper.getOptions().getService();
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
    Transaction transaction1 = datastore.newTransaction();
    Transaction transaction2 = datastore.newTransaction();
    Transaction transaction3 = datastore.newTransaction();
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
    }

    // To remove lingering transactions from the Datastore emulator
    resetDatastore();
  }

  @Test
  public void insertActiveStateShouldFailIfAlreadyExists() throws Exception {
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO);
    storage.runInTransaction(tx -> tx.insertActiveState(WORKFLOW_INSTANCE1, PERSISTENT_STATE1));
    try {
      storage.runInTransaction(tx -> tx.insertActiveState(WORKFLOW_INSTANCE1, PERSISTENT_STATE1));
      fail("Expected exception!");
    } catch (TransactionException e) {
      assertThat(e.isAlreadyExists(), is(true));
    }
  }

  @Test
  public void updateActiveStateShouldFailIfNotAlreadyExists() throws Exception {
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO);
    try {
      storage.runInTransaction(tx -> tx.updateActiveState(WORKFLOW_INSTANCE1, PERSISTENT_STATE1));
      fail("Expected exception!");
    } catch (TransactionException e) {
      assertThat(e.isNotFound(), is(true));
    }
  }

  private void resetDatastore() throws IOException, InterruptedException {
    if (helper != null) {
      try {
        helper.stop(org.threeten.bp.Duration.ofSeconds(30));
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
    helper = LocalDatastoreHelper.create(1.0); // 100% global consistency
    helper.start();
  }
}
