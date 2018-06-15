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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Datastore.TransactionCallable;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.Transaction;
import com.google.common.collect.ImmutableList;
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
  private static final Key TEST_KEY = Key.newBuilder("test-project", TEST_KIND, "test").build();
  private static final Entity TEST_ENTITY = Entity.newBuilder(TEST_KEY).build();
  private static final EntityQuery TEST_ENTITY_QUERY = EntityQuery.newEntityQueryBuilder().setKind(TEST_KIND).build();

  private Datastore instrumentedDatastore;

  @Mock Datastore datastore;
  @Mock Stats stats;
  @Mock QueryResults<Entity> entityQueryResults;
  @Mock Transaction transaction;

  @Before
  public void setUp() throws Exception {
    instrumentedDatastore = InstrumentedDatastore.of(datastore, stats);
  }

  @Test
  public void newTransaction() {
    when(datastore.newTransaction()).thenReturn(transaction);
    final Transaction instrumentedTransaction = instrumentedDatastore.newTransaction();
    verify(datastore).newTransaction();

    instrumentedTransaction.get(TEST_KEY);
    verify(stats).recordDatastoreEntityReads(TEST_KIND, 1);

    instrumentedTransaction.put(TEST_ENTITY);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND, 1);

    instrumentedTransaction.delete(TEST_KEY);
    verify(stats).recordDatastoreEntityDeletes(TEST_KIND, 1);

    instrumentedTransaction.rollback();
    verify(transaction).rollback();

    instrumentedTransaction.commit();
    verify(transaction).commit();
  }

  @Test
  public void runInTransaction() {
    when(datastore.runInTransaction(any())).then(a ->
        a.getArgumentAt(0, TransactionCallable.class)
            .run(transaction));

    final String foobar = instrumentedDatastore.runInTransaction(tx -> {
      tx.get(TEST_KEY);
      verify(stats, times(1)).recordDatastoreEntityReads(TEST_KIND, 1);
      tx.get(TEST_KEY, TEST_KEY);
      verify(stats, times(3)).recordDatastoreEntityReads(TEST_KIND, 1);
      tx.put(TEST_ENTITY);
      verify(stats, times(1)).recordDatastoreEntityWrites(TEST_KIND, 1);
      tx.put(TEST_ENTITY, TEST_ENTITY);
      verify(stats, times(3)).recordDatastoreEntityWrites(TEST_KIND, 1);
      tx.delete(TEST_KEY);
      verify(stats, times(1)).recordDatastoreEntityDeletes(TEST_KIND, 1);
      tx.delete(TEST_KEY, TEST_KEY);
      verify(stats, times(3)).recordDatastoreEntityDeletes(TEST_KIND, 1);
      return "foobar";
    });

    assertThat(foobar, is("foobar"));
  }

  @Test
  public void getSingle() {
    when(datastore.get(TEST_KEY)).thenReturn(TEST_ENTITY);
    assertThat(instrumentedDatastore.get(TEST_KEY), is(TEST_ENTITY));
    verify(datastore).get(TEST_KEY);
    verify(stats).recordDatastoreEntityReads(TEST_KIND, 1);
  }

  @Test
  public void getMany() {
    final List<Entity> entities = ImmutableList.of(TEST_ENTITY, TEST_ENTITY, TEST_ENTITY);
    when(datastore.get(TEST_KEY, TEST_KEY, TEST_KEY)).thenReturn(entities.iterator());
    assertThat(ImmutableList.copyOf(instrumentedDatastore.get(TEST_KEY, TEST_KEY, TEST_KEY)), is(entities));
    verify(datastore).get(TEST_KEY, TEST_KEY, TEST_KEY);
    verify(stats, times(3)).recordDatastoreEntityReads(TEST_KIND, 1);
  }

  @Test
  public void run() {
    when(datastore.run(TEST_ENTITY_QUERY)).thenReturn(entityQueryResults);
    final QueryResults<Entity> results = instrumentedDatastore.run(TEST_ENTITY_QUERY);
    verify(datastore).run(TEST_ENTITY_QUERY);
    verify(stats).recordDatastoreQueries(TEST_KIND, 1);

    when(entityQueryResults.hasNext()).thenReturn(true);
    when(entityQueryResults.next()).thenReturn(TEST_ENTITY);
    assertThat(results.hasNext(), is(true));
    verify(entityQueryResults).hasNext();
    assertThat(results.next(), is(TEST_ENTITY));
    verify(entityQueryResults).next();
    verify(stats).recordDatastoreEntityReads(TEST_KIND, 1);
  }

  @Test
  public void update() {
    instrumentedDatastore.update(TEST_ENTITY);
    verify(datastore).update(TEST_ENTITY);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND, 1);
  }

  @Test
  public void put() {
    instrumentedDatastore.put(TEST_ENTITY);
    verify(datastore).put(TEST_ENTITY);
    verify(stats).recordDatastoreEntityWrites(TEST_KIND, 1);
  }
}
