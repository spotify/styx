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

import static org.junit.Assert.assertFalse;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Transaction;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import java.io.IOException;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DatastoreTransactionalStorageTest {

  private static LocalDatastoreHelper helper;
  private static Datastore datastore;

  @Mock DatastoreStorage storage;

  @BeforeClass
  public static void setUpClass() throws Exception {
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
    DatastoreTransactionalStorage transactionalStorage = new DatastoreTransactionalStorage(storage, transaction);
    transactionalStorage.commit();
    assertFalse(transaction.isActive());
  }

  @Test(expected = TransactionException.class)
  public void shouldThrowIfUnexpectedDatastoreError() throws IOException {
    final Transaction transaction = datastore.newTransaction();
    DatastoreTransactionalStorage transactionalStorage = new DatastoreTransactionalStorage(storage, transaction);

    transaction.rollback();
    transactionalStorage.commit();
  }

  @Test(expected = TransactionException.class)
  public void shouldThrowIfTransactionFailed() throws TransactionException {
    Transaction transaction1 = datastore.newTransaction();
    Transaction transaction2 = datastore.newTransaction();
    Transaction transaction3 = datastore.newTransaction();
    DatastoreTransactionalStorage transactionalStorage1 = new DatastoreTransactionalStorage(storage, transaction1);
    DatastoreTransactionalStorage transactionalStorage2 = new DatastoreTransactionalStorage(storage, transaction2);
    DatastoreTransactionalStorage transactionalStorage3 = new DatastoreTransactionalStorage(storage, transaction3);

    // Store first entity
    KeyFactory keyFactory1 = datastore.newKeyFactory().setKind("MyKind");
    Key key1 = datastore.allocateId(keyFactory1.newKey());
    Entity entity1 = Entity.newBuilder(key1).set("key", "firstWrite").build();
    transaction1.put(entity1);
    transactionalStorage1.commit();

    // Read first entity then commit a change to the first entity
    Entity entity1read = transaction3.get(key1);
    Entity entity1modified = Entity.newBuilder(key1).set("key", "secondWrite").build();
    transaction2.put(entity1modified);
    transactionalStorage2.commit();

    // Used the read first entity to generate a second entity (copy of the first entity)
    KeyFactory keyFactory2 = datastore.newKeyFactory().setKind("MyKindCopy");
    Key key2 = datastore.allocateId(keyFactory2.newKey());
    Entity copyOfEntity1 = Entity.newBuilder(key2).set("key", entity1read.getString("key")).build();
    transaction3.put(copyOfEntity1);
    transactionalStorage3.commit();
  }
}
