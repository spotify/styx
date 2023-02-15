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

import static com.google.datastore.v1.QueryResultBatch.MoreResultsType.MORE_RESULTS_AFTER_CURSOR;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.cloud.datastore.BaseEntity;
import com.google.cloud.datastore.Batch;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Datastore.TransactionCallable;
import com.google.cloud.datastore.DatastoreBatchWriter;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.DatastoreReader;
import com.google.cloud.datastore.DatastoreReaderWriter;
import com.google.cloud.datastore.DatastoreWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.ProjectionEntity;
import com.google.cloud.datastore.ProjectionEntityQuery;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.ReadOption;
import com.google.cloud.datastore.Transaction;
import com.google.datastore.v1.TransactionOptions;
import com.spotify.styx.monitoring.Stats;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings({"ResultOfMethodCallIgnored", "unchecked"})
@RunWith(MockitoJUnitRunner.class)
public class InstrumentedDatastoreTest {

  private static final String TEST_PROJECT = "test-project";
  private static final String TEST_KIND = "test-kind";
  private static final String TEST_KIND_1 = "test-kind-1";
  private static final String TEST_KIND_2 = "test-kind-2";
  private static final Key TEST_KEY = Key.newBuilder(TEST_PROJECT, TEST_KIND, "test").build();
  private static final Key TEST_KEY_1 = Key.newBuilder(TEST_PROJECT, TEST_KIND_1, "test-1").build();
  private static final Key TEST_KEY_2 = Key.newBuilder(TEST_PROJECT, TEST_KIND_2, "test-2").build();
  private static final Entity TEST_ENTITY = Entity.newBuilder(TEST_KEY).build();
  private static final Entity TEST_ENTITY_1 = Entity.newBuilder(TEST_KEY_1).build();
  private static final Entity TEST_ENTITY_2 = Entity.newBuilder(TEST_KEY_2).build();
  private static final EntityQuery TEST_ENTITY_QUERY = EntityQuery.newEntityQueryBuilder().setKind(TEST_KIND).build();
  private static final KeyQuery TEST_KEY_QUERY = EntityQuery.newKeyQueryBuilder().setKind(TEST_KIND).build();
  private static final ProjectionEntityQuery TEST_PROJECTION_QUERY_WITHOUT_DISTINCT =
      EntityQuery.newProjectionEntityQueryBuilder().setKind(TEST_KIND).build();
  private static final ProjectionEntityQuery TEST_PROJECTION_QUERY_WITH_DISTINCT =
      EntityQuery.newProjectionEntityQueryBuilder().setKind(TEST_KIND).addDistinctOn("foo", "bar").build();
  private static final KeyFactory KEY_FACTORY = new KeyFactory(TEST_PROJECT);

  private InstrumentedDatastore instrumentedDatastore;

  @Mock Datastore datastore;
  @Mock Stats stats;
  @Mock QueryResults<Entity> entityQueryResults;
  @Mock QueryResults<Key> keyQueryResults;
  @Mock QueryResults<ProjectionEntity> projectionQueryResults;
  @Mock Transaction transaction;
  @Mock Batch batch;
  @Mock ReadOption readOption1;
  @Mock ReadOption readOption2;
  @Mock IncompleteKey incompleteKey1;
  @Mock IncompleteKey incompleteKey2;
  @Mock DatastoreOptions options;
  @Mock BaseEntity<?> entity;

  @Before
  public void setUp() throws Exception {
    instrumentedDatastore = InstrumentedDatastore.of(datastore, stats);
  }

  @Test
  public void testDatastore() {
    testReaderWriter(instrumentedDatastore, datastore);

    // Entity get(Key key, ReadOption... options);
    instrumentedDatastore.get(TEST_KEY, readOption1, readOption2);
    verify(datastore).get(TEST_KEY, readOption1, readOption2);
    verify(stats).recordDatastoreEntityReads(TEST_KIND, 1);
    verifyNoMoreInteractions(stats, datastore);
    reset(stats, datastore);

    // Iterator<Entity> get(Iterable<Key> keys, ReadOption... options);
    instrumentedDatastore.get(asList(TEST_KEY_1, TEST_KEY_2), readOption1, readOption2);
    verify(datastore).get(asList(TEST_KEY_1, TEST_KEY_2), readOption1, readOption2);
    verify(stats).recordDatastoreEntityReads(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityReads(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats, datastore);
    reset(stats, datastore);

    // List<Entity> fetch(Iterable<Key> keys, ReadOption... options);
    instrumentedDatastore.fetch(asList(TEST_KEY_1, TEST_KEY_2), readOption1, readOption2);
    verify(datastore).fetch(asList(TEST_KEY_1, TEST_KEY_2), readOption1, readOption2);
    verify(stats).recordDatastoreEntityReads(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityReads(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats, datastore);
    reset(stats, datastore);

    // <T> QueryResults<T> run(Query<T> query, ReadOption... options);

    // Entity Query
    when(datastore.run(TEST_ENTITY_QUERY, readOption1, readOption2)).thenReturn(entityQueryResults);
    var instrumentedEntityQueryResults =
        instrumentedDatastore.run(TEST_ENTITY_QUERY, readOption1, readOption2);
    verify(datastore).run(TEST_ENTITY_QUERY, readOption1, readOption2);
    verify(stats).recordDatastoreQueries(TEST_KIND, 1);
    testInstrumentedQueryResults(instrumentedEntityQueryResults, entityQueryResults);
    verifyNoMoreInteractions(stats, datastore, entityQueryResults);
    reset(stats, datastore, entityQueryResults);

    // Key Query
    when(datastore.run(TEST_KEY_QUERY, readOption1, readOption2)).thenReturn(keyQueryResults);
    assertThat(instrumentedDatastore.run(TEST_KEY_QUERY, readOption1, readOption2), sameInstance(keyQueryResults));
    verify(datastore).run(TEST_KEY_QUERY, readOption1, readOption2);
    verify(stats).recordDatastoreQueries(TEST_KIND, 1);
    verifyNoMoreInteractions(stats, datastore, keyQueryResults);
    reset(stats, datastore, keyQueryResults);

    // Projection Query - With Distinct
    when(datastore.run(TEST_PROJECTION_QUERY_WITH_DISTINCT, readOption1, readOption2))
        .thenReturn(projectionQueryResults);
    var instrumentedProjectionQueryResults =
        instrumentedDatastore.run(TEST_PROJECTION_QUERY_WITH_DISTINCT, readOption1, readOption2);
    verify(datastore).run(TEST_PROJECTION_QUERY_WITH_DISTINCT, readOption1, readOption2);
    verify(stats).recordDatastoreQueries(TEST_KIND, 1);
    testInstrumentedQueryResults(instrumentedProjectionQueryResults, projectionQueryResults);
    verifyNoMoreInteractions(stats, datastore);
    reset(stats, datastore, projectionQueryResults);

    // Project Query - Without Distinct
    when(datastore.run(TEST_PROJECTION_QUERY_WITHOUT_DISTINCT, readOption1, readOption2))
        .thenReturn(projectionQueryResults);
    assertThat(instrumentedDatastore.run(TEST_PROJECTION_QUERY_WITHOUT_DISTINCT, readOption1, readOption2),
        is(sameInstance(projectionQueryResults)));
    verify(stats).recordDatastoreQueries(TEST_KIND, 1);
    verify(datastore).run(TEST_PROJECTION_QUERY_WITHOUT_DISTINCT, readOption1, readOption2);
    verifyNoMoreInteractions(stats, datastore);
    reset(stats, datastore);

    // Key allocateId(IncompleteKey key);
    when(instrumentedDatastore.allocateId(incompleteKey1)).thenReturn(TEST_KEY_1);
    assertThat(instrumentedDatastore.allocateId(incompleteKey1), is(TEST_KEY_1));
    verify(datastore).allocateId(incompleteKey1);
    verifyNoMoreInteractions(stats, datastore);
    reset(stats, datastore);

    // List<Key> allocateId(IncompleteKey... keys);
    when(instrumentedDatastore.allocateId(incompleteKey1, incompleteKey2)).thenReturn(asList(TEST_KEY_1, TEST_KEY_2));
    assertThat(instrumentedDatastore.allocateId(incompleteKey1, incompleteKey2), contains(TEST_KEY_1, TEST_KEY_2));
    verify(datastore).allocateId(incompleteKey1, incompleteKey2);
    verifyNoMoreInteractions(stats, datastore);
    reset(stats, datastore);

    // KeyFactory newKeyFactory();
    when(datastore.newKeyFactory()).thenReturn(KEY_FACTORY);
    assertThat(instrumentedDatastore.newKeyFactory(), is(KEY_FACTORY));
    verify(datastore).newKeyFactory();
    verifyNoMoreInteractions(stats, datastore);
    reset(stats, datastore);

    // OptionsT getOptions();
    when(datastore.getOptions()).thenReturn(options);
    assertThat(instrumentedDatastore.getOptions(), is(options));
    verify(datastore).getOptions();
    verifyNoMoreInteractions(stats, datastore);
    reset(stats, datastore);
  }

  @Test
  public void newTransaction() {
    // Transaction newTransaction(TransactionOptions options);
    when(datastore.newTransaction(any(TransactionOptions.class))).thenReturn(transaction);
    var transactionOptions = TransactionOptions.newBuilder().build();
    var instrumented1 = instrumentedDatastore.newTransaction(transactionOptions);
    verify(datastore).newTransaction(transactionOptions);
    verifyNoMoreInteractions(datastore);
    reset(datastore);
    testTransaction(instrumented1);

    // Transaction newTransaction();
    when(datastore.newTransaction()).thenReturn(transaction);
    var instrumented2 = instrumentedDatastore.newTransaction();
    verify(datastore).newTransaction();
    verifyNoMoreInteractions(datastore);
    reset(datastore);
    testTransaction(instrumented2);
  }

  @Test
  public void runInTransaction() {
    when(datastore.runInTransaction(any())).then(a ->
        a.<TransactionCallable<String>>getArgument(0)
            .run(transaction));

    var foobar = instrumentedDatastore.runInTransaction(tx -> {
      testReaderWriter(tx, transaction);
      return "foobar";
    });

    verify(datastore).runInTransaction(any());
    verifyNoMoreInteractions(datastore);

    assertThat(foobar, is("foobar"));
  }

  @Test
  public void runInTransactionWithOptions() {
    when(datastore.runInTransaction(any(), any())).then(a ->
        a.<TransactionCallable<String>>getArgument(0)
            .run(transaction));

    var transactionOptions = TransactionOptions.newBuilder().build();

    var foobar = instrumentedDatastore.runInTransaction(tx -> {
      testReaderWriter(tx, transaction);
      return "foobar";
    }, transactionOptions);

    verify(datastore).runInTransaction(any(), eq(transactionOptions));
    verifyNoMoreInteractions(datastore);

    assertThat(foobar, is("foobar"));
  }

  @Test
  public void newBatch() {
    when(datastore.newBatch()).thenReturn(batch);
    var instrumentedBatch = instrumentedDatastore.newBatch();
    verify(datastore).newBatch();
    verifyNoMoreInteractions(datastore);
    reset(datastore);
    testBatchWriter(instrumentedBatch, batch);

    instrumentedBatch.submit();
    verify(batch).submit();
    verifyNoMoreInteractions(batch);
    reset(batch);

    when(batch.getDatastore()).thenReturn(datastore);
    var instrumentedDatastore = instrumentedBatch.getDatastore();
    assertThat(instrumentedDatastore, instanceOf(InstrumentedDatastore.class));
    assertThat(((InstrumentedDatastore) instrumentedDatastore).delegate(), is(datastore));
    verify(batch).getDatastore();
    verifyNoMoreInteractions(batch);
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

    // Datastore getDatastore();
    when(transaction.getDatastore()).thenReturn(datastore);
    var instrumentedDatastore = instrumented.getDatastore();
    assertThat(instrumentedDatastore, instanceOf(InstrumentedDatastore.class));
    assertThat(((InstrumentedDatastore) instrumentedDatastore).delegate(), is(datastore));
    verify(transaction).getDatastore();

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
    verifyNoMoreInteractions(stats, delegate);
    reset(stats, delegate);

    // List<Entity> add(FullEntity<?>... entities);
    instrumented.add(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(delegate).add(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats, delegate);
    reset(stats, delegate);

    // void update(Entity... entities);
    instrumented.update(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(delegate).update(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats, delegate);
    reset(stats, delegate);

    // Entity put(FullEntity<?> entity);
    instrumented.put(TEST_ENTITY);
    verify(delegate).put(TEST_ENTITY);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND, 1);
    verifyNoMoreInteractions(stats, delegate);
    reset(stats, delegate);

    // List<Entity> put(FullEntity<?>... entities);
    instrumented.put(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(delegate).put(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats, delegate);
    reset(stats, delegate);

    // void delete(Key... keys);
    instrumented.delete(TEST_KEY_1, TEST_KEY_2);
    verify(delegate).delete(TEST_KEY_1, TEST_KEY_2);
    verify(stats).recordDatastoreEntityDeletes(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityDeletes(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats, delegate);
    reset(stats, delegate);
  }

  private void testReader(DatastoreReader instrumented, DatastoreReader delegate) {
    // Entity get(Key key);
    instrumented.get(TEST_KEY);
    verify(delegate).get(TEST_KEY);
    verify(stats).recordDatastoreEntityReads(TEST_KIND, 1);
    verifyNoMoreInteractions(stats, delegate);
    reset(stats, delegate);

    // Iterator<Entity> get(Key... keys);
    instrumented.get(TEST_KEY_1, TEST_KEY_2);
    verify(delegate).get(TEST_KEY_1, TEST_KEY_2);
    verify(stats).recordDatastoreEntityReads(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityReads(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats, delegate);
    reset(stats, delegate);

    // List<Entity> fetch(Key... keys);
    instrumented.fetch(TEST_KEY_1, TEST_KEY_2);
    verify(delegate).fetch(TEST_KEY_1, TEST_KEY_2);
    verify(stats).recordDatastoreEntityReads(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityReads(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats, delegate);
    reset(stats, delegate);

    // <T> QueryResults<T> run(Query<T> query);

    // Entity Query
    when(delegate.run(TEST_ENTITY_QUERY)).thenReturn(entityQueryResults);
    var instrumentedEntityQueryResults = instrumented.run(TEST_ENTITY_QUERY);
    verify(delegate).run(TEST_ENTITY_QUERY);
    verify(stats).recordDatastoreQueries(TEST_KIND, 1);
    testInstrumentedQueryResults(instrumentedEntityQueryResults, entityQueryResults);
    verifyNoMoreInteractions(stats, delegate, entityQueryResults);
    reset(stats, delegate, entityQueryResults);

    // Key Query
    when(delegate.run(TEST_KEY_QUERY)).thenReturn(keyQueryResults);
    assertThat(instrumented.run(TEST_KEY_QUERY), sameInstance(keyQueryResults));
    verify(delegate).run(TEST_KEY_QUERY);
    verify(stats).recordDatastoreQueries(TEST_KIND, 1);
    verifyNoMoreInteractions(stats, delegate, keyQueryResults);
    reset(stats, delegate, keyQueryResults);

    // Projection Query - With Distinct
    when(delegate.run(TEST_PROJECTION_QUERY_WITH_DISTINCT)).thenReturn(projectionQueryResults);
    var instrumentedProjectionQueryResults =
        instrumented.run(TEST_PROJECTION_QUERY_WITH_DISTINCT);
    verify(delegate).run(TEST_PROJECTION_QUERY_WITH_DISTINCT);
    verify(stats).recordDatastoreQueries(TEST_KIND, 1);
    testInstrumentedQueryResults(instrumentedProjectionQueryResults, projectionQueryResults);
    verifyNoMoreInteractions(stats, delegate);
    reset(stats, delegate, projectionQueryResults);

    // Project Query - Without Distinct
    when(delegate.run(TEST_PROJECTION_QUERY_WITHOUT_DISTINCT)).thenReturn(projectionQueryResults);
    assertThat(instrumented.run(TEST_PROJECTION_QUERY_WITHOUT_DISTINCT), is(sameInstance(projectionQueryResults)));
    verify(stats).recordDatastoreQueries(TEST_KIND, 1);
    verify(delegate).run(TEST_PROJECTION_QUERY_WITHOUT_DISTINCT);
    verifyNoMoreInteractions(stats, delegate);
    reset(stats, delegate);
  }

  private <T extends BaseEntity<?>> void testInstrumentedQueryResults(QueryResults<T> instrumented,
      QueryResults<T> delegate) {
    when(delegate.hasNext()).thenReturn(true);
    when(delegate.next()).thenReturn((T) entity);
    when(delegate.getSkippedResults()).thenReturn(17);
    when(delegate.getMoreResults()).thenReturn(MORE_RESULTS_AFTER_CURSOR);
    assertThat(instrumented.hasNext(), is(true));
    verify(delegate).hasNext();
    assertThat(instrumented.next(), is(entity));
    verify(delegate).next();
    assertThat(instrumented.getSkippedResults(), is(17));
    assertThat(instrumented.getMoreResults(), is(MORE_RESULTS_AFTER_CURSOR));
    verify(delegate).getSkippedResults();
    verify(delegate).getMoreResults();
    verify(stats).recordDatastoreEntityReads(TEST_KIND, 1);
  }

  private void testBatchWriter(DatastoreBatchWriter instrumented, DatastoreBatchWriter delegate) {
    testWriter(instrumented, delegate);

    // void addWithDeferredIdAllocation(FullEntity<?>... entities);
    instrumented.addWithDeferredIdAllocation(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(delegate).addWithDeferredIdAllocation(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats, delegate);
    reset(stats, delegate);

    // void putWithDeferredIdAllocation(FullEntity<?>... entities);
    instrumented.putWithDeferredIdAllocation(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(delegate).putWithDeferredIdAllocation(TEST_ENTITY_1, TEST_ENTITY_2);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_1, 1);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND_2, 1);
    verifyNoMoreInteractions(stats, delegate);
    reset(stats, delegate);

    // boolean isActive();
    when(delegate.isActive()).thenReturn(true);
    assertThat(instrumented.isActive(), is(true));
    verify(delegate).isActive();
    verifyNoMoreInteractions(stats, delegate);
    reset(stats, delegate);
  }
}
