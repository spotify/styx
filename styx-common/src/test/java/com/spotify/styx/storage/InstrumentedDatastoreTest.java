/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.datastore.Batch;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Datastore.TransactionCallable;
import com.google.cloud.datastore.DatastoreReader;
import com.google.cloud.datastore.DatastoreReaderWriter;
import com.google.cloud.datastore.DatastoreWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.Transaction;
import com.google.common.collect.ImmutableList;
import com.google.datastore.v1.TransactionOptions;
import com.spotify.styx.monitoring.Stats;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@SuppressWarnings("ResultOfMethodCallIgnored")
@RunWith(MockitoJUnitRunner.class)
public class InstrumentedDatastoreTest {

  private static final String TEST_KIND = "test-kind";
  private static final String TEST_KIND_1 = "test-kind-1";
  private static final String TEST_KIND_2 = "test-kind-2";
  private static final Key TEST_KEY = Key.newBuilder("test-project", TEST_KIND, "test").build();
  private static final Key TEST_KEY_1 = Key.newBuilder("test-project", TEST_KIND_1, "test-1").build();
  private static final Key TEST_KEY_2 = Key.newBuilder("test-project", TEST_KIND_2, "test-2").build();
  private static final Entity TEST_ENTITY = Entity.newBuilder(TEST_KEY).build();
  private static final Entity TEST_ENTITY_1 = Entity.newBuilder(TEST_KEY_1).build();
  private static final Entity TEST_ENTITY_2 = Entity.newBuilder(TEST_KEY_2).build();
  private static final EntityQuery TEST_ENTITY_QUERY = EntityQuery.newEntityQueryBuilder().setKind(TEST_KIND).build();

  private Datastore instrumentedDatastore;

  @Mock Datastore datastore;
  @Mock Stats stats;
  @Mock QueryResults<Entity> entityQueryResults;
  @Mock Transaction transaction;
  @Mock Batch batch;

  @Before
  public void setUp() throws Exception {
    instrumentedDatastore = InstrumentedDatastore.of(datastore, stats);
  }

  @Test
  public void testDatastore() {
    testReaderWriter(instrumentedDatastore, datastore);
  }

  @Test
  public void newTransaction() {
    when(datastore.newTransaction()).thenReturn(transaction);
    when(datastore.newTransaction(any(TransactionOptions.class))).thenReturn(transaction);

    // Transaction newTransaction(TransactionOptions options);
    final TransactionOptions transactionOptions = TransactionOptions.newBuilder().build();
    final Transaction instrumented1 = instrumentedDatastore.newTransaction(transactionOptions);
    verify(datastore).newTransaction(transactionOptions);
    testTransaction(instrumented1);

    // Transaction newTransaction();
    final Transaction instrumented2 = instrumentedDatastore.newTransaction();
    verify(datastore).newTransaction();
    testTransaction(instrumented2);
  }

  @Test
  public void runInTransaction() {
    when(datastore.runInTransaction(any())).then(a ->
        a.getArgumentAt(0, TransactionCallable.class)
            .run(transaction));

    final String foobar = instrumentedDatastore.runInTransaction(tx -> {
      testReaderWriter(tx, transaction);
      return "foobar";
    });

    assertThat(foobar, is("foobar"));
  }

  @Test
  public void newBatch() {
    when(datastore.newBatch()).thenReturn(batch);
    final Batch instrumentedBatch = instrumentedDatastore.newBatch();
    testBatchWriter(instrumentedBatch, batch);

    instrumentedBatch.submit();
    verify(batch).submit();

    when(batch.getDatastore()).thenReturn(datastore);
    assertThat(instrumentedBatch.getDatastore(), is(datastore));
    verify(batch).getDatastore();
  }

  private void testTransaction(Transaction instrumented) {
    testReaderWriter(instrumented, transaction);

    // void putWithDeferredIdAllocation(FullEntity<?>... entities);
    instrumented.putWithDeferredIdAllocation(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(transaction).putWithDeferredIdAllocation(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats);
    reset(stats);

    // boolean isActive();
    when(transaction.isActive()).thenReturn(true);
    assertThat(instrumented.isActive(), is(true));
    verify(transaction).isActive();

    // void rollback();
    instrumented.rollback();
    verify(transaction).rollback();

    // Response commit();
    instrumented.commit();
    verify(transaction).commit();

    verifyNoMoreInteractions(transaction);
    reset(transaction);
  }

  private void testReaderWriter(DatastoreReaderWriter instrumented, DatastoreReaderWriter delegate) {
    testReader(instrumented, delegate);
    testWriter(instrumented, delegate);
  }

  private void testWriter(DatastoreWriter instrumented, DatastoreWriter delegate) {

    // Entity add(FullEntity<?> entity);
    instrumented.add(TEST_ENTITY);
    verify(delegate).add(TEST_ENTITY);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND, 1);
    verifyNoMoreInteractions(stats);
    reset(stats);

    // List<Entity> add(FullEntity<?>... entities);
    instrumented.add(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(delegate).add(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats);
    reset(stats);

    // void update(Entity... entities);
    instrumented.update(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(delegate).update(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats);
    reset(stats);

    // Entity put(FullEntity<?> entity);
    instrumented.put(TEST_ENTITY);
    verify(delegate).put(TEST_ENTITY);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND, 1);
    verifyNoMoreInteractions(stats);
    reset(stats);

    // List<Entity> put(FullEntity<?>... entities);
    instrumented.put(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(delegate).put(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats);
    reset(stats);

    // void delete(Key... keys);
    instrumented.delete(TEST_KEY_1, TEST_KEY_2);
    verify(delegate).delete(TEST_KEY_1, TEST_KEY_2);
    verify(stats).recordDatastoreEntityDeletes(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityDeletes(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats);
    reset(stats);

    reset(delegate);
  }

  private void testReader(DatastoreReader instrumented, DatastoreReader delegate) {
    // Entity get(Key key);
    instrumented.get(TEST_KEY);
    verify(delegate).get(TEST_KEY);
    verify(stats).recordDatastoreEntityReads(TEST_KIND, 1);
    verifyNoMoreInteractions(stats);
    reset(stats);

    // Iterator<Entity> get(Key... keys);
    instrumented.get(TEST_KEY_1, TEST_KEY_2);
    verify(delegate).get(TEST_KEY_1, TEST_KEY_2);
    verify(stats).recordDatastoreEntityReads(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityReads(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats);
    reset(stats);

    // List<Entity> fetch(Key... keys);
    instrumented.fetch(TEST_KEY_1, TEST_KEY_2);
    verify(delegate).fetch(TEST_KEY_1, TEST_KEY_2);
    verify(stats).recordDatastoreEntityReads(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityReads(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats);
    reset(stats);

    // <T> QueryResults<T> run(Query<T> query);
    when(delegate.run(TEST_ENTITY_QUERY)).thenReturn(entityQueryResults);
    final QueryResults<Entity> results = instrumented.run(TEST_ENTITY_QUERY);
    verify(delegate).run(TEST_ENTITY_QUERY);
    verify(stats).recordDatastoreQueries(TEST_KIND, 1);
    when(entityQueryResults.hasNext()).thenReturn(true);
    when(entityQueryResults.next()).thenReturn(TEST_ENTITY);
    assertThat(results.hasNext(), is(true));
    verify(entityQueryResults).hasNext();
    assertThat(results.next(), is(TEST_ENTITY));
    verify(entityQueryResults).next();
    verify(stats).recordDatastoreEntityReads(TEST_KIND, 1);
    verifyNoMoreInteractions(stats);
    reset(stats);
    reset(entityQueryResults);

    reset(delegate);
  }

  private void testBatchWriter(Batch instrumented, Batch delegate) {
    testWriter(instrumented, delegate);

    // void putWithDeferredIdAllocation(FullEntity<?>... entities);
    instrumented.putWithDeferredIdAllocation(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(delegate).putWithDeferredIdAllocation(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats);
    reset(stats);

    // boolean isActive();
    when(delegate.isActive()).thenReturn(true);
    assertThat(instrumented.isActive(), is(true));
    verify(delegate).isActive();

    reset(delegate);
  }
}
