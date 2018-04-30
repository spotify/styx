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

package com.spotify.styx.util;

import static com.spotify.styx.util.ShardedCounter.KIND_COUNTER_LIMIT;
import static com.spotify.styx.util.ShardedCounter.KIND_COUNTER_SHARD;
import static com.spotify.styx.util.ShardedCounter.PROPERTY_COUNTER_ID;
import static com.spotify.styx.util.ShardedCounter.PROPERTY_LIMIT;
import static com.spotify.styx.util.ShardedCounter.PROPERTY_SHARD_INDEX;
import static com.spotify.styx.util.ShardedCounter.PROPERTY_SHARD_VALUE;
import static org.hamcrest.CoreMatchers.either;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.stream.IntStream;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ShardedCounterTest {

  private static final String COUNTER_ID1 = "resource_counter_1";
  private static final String COUNTER_ID2 = "resource_counter_2";

  private static LocalDatastoreHelper helper;
  private static CounterSnapshotFactory counterSnapshotFactory;
  private static ShardedCounter shardedCounter;
  private static Datastore datastore;
  private static Storage storage;
  private static Connection connection;

  @BeforeClass
  public static void setUpClass() throws IOException, InterruptedException {
    final java.util.logging.Logger datastoreEmulatorLogger =
        java.util.logging.Logger.getLogger(LocalDatastoreHelper.class.getName());
    datastoreEmulatorLogger.setLevel(Level.OFF);

    helper = LocalDatastoreHelper.create(1.0);
    helper.start();
    datastore = helper.getOptions().getService();
    connection = mock(Connection.class);
    storage = new AggregateStorage(connection, datastore, Duration.ZERO);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    connection.close();
    if (helper != null) {
      try {
        helper.stop(org.threeten.bp.Duration.ofSeconds(30));
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  @Before
  public void setUp() {
    counterSnapshotFactory = spy(new ShardedCounterSnapshotFactory(storage));
    shardedCounter = new ShardedCounter(storage, counterSnapshotFactory);
  }

  @After
  public void tearDown() throws IOException {
    helper.reset();
  }

  @Test
  public void shouldCreateCounterEmpty() {
    assertEquals(shardedCounter.getCounter(COUNTER_ID1), 0L);
    QueryResults<Entity> results = getShardsForCounter(COUNTER_ID1);

    // assert all shards exist
    assertThat(shardedCounter.getCounterSnapshot(COUNTER_ID1).getShards().size(), is(128));
    IntStream.range(0, ShardedCounter.NUM_SHARDS).forEach(i -> {
      assertTrue(results.hasNext());
      results.next();
    });
  }

  @Test
  public void updateCounterShouldInitializeShardsIfAbsent() {
    // Make sure shards are not initialized
    QueryResults<Entity> shardsForCounter = getShardsForCounter(COUNTER_ID1);
    assertThat(shardsForCounter.hasNext(), is(false));

    updateCounterInTransaction(COUNTER_ID1, 1L);

    // Make sure updating counter initializes shards if required
    shardsForCounter = getShardsForCounter(COUNTER_ID1);
    assertThat(shardsForCounter.hasNext(), is(true));
  }

  @Test
  public void shouldCreateLimit() throws IOException {
    assertNull(getLimitFromStorage(COUNTER_ID1));

    storage.runInTransaction(transaction -> {
      shardedCounter.updateLimit(transaction, COUNTER_ID1, 500);
      return null;
    });

    assertEquals(500L, getLimitFromStorage(COUNTER_ID1).getLong(PROPERTY_LIMIT));
  }

  @Test
  public void shouldIncrementCounter() {
    // init counter
    assertEquals(0, shardedCounter.getCounter(COUNTER_ID1));

    //increment counter by 1
    updateCounterInTransaction(COUNTER_ID1, 1L);

    QueryResults<Entity> results = datastore.run(EntityQuery.newEntityQueryBuilder()
        .setKind(KIND_COUNTER_SHARD)
        .setFilter(CompositeFilter.and(PropertyFilter.eq(PROPERTY_COUNTER_ID, COUNTER_ID1),
            PropertyFilter.eq(PROPERTY_SHARD_VALUE,1)))
        .build());
    // assert there's one and only one shard with the value set to 1
    assertEquals(1L, results.next().getLong(PROPERTY_SHARD_VALUE));
    assertFalse(results.hasNext());

    // assert the correct value is fetched after cache expiry
    shardedCounter.inMemSnapshot.invalidate(COUNTER_ID1);
    assertEquals(1L, shardedCounter.getCounter(COUNTER_ID1));
  }

  @Test
  public void shouldDecrementCounter() {
    // init counter
    assertEquals(0, shardedCounter.getCounter(COUNTER_ID1));

    //increment counter shard by 1
    updateShard(COUNTER_ID1,3, 1);

    shardedCounter.inMemSnapshot.invalidate(COUNTER_ID1);
    // assert cache is updated with the new value
    assertEquals(1L, shardedCounter.getCounter(COUNTER_ID1));

    //decrement counter by 1
    updateCounterInTransaction(COUNTER_ID1, -1L);

    // assert that the only eligible shard was chosen to be decremented
    assertEquals(0L, datastore.get(getKey(COUNTER_ID1, 3)).getLong(PROPERTY_SHARD_VALUE));

    // assert cache is updated with the new value
    shardedCounter.inMemSnapshot.invalidate(COUNTER_ID1);
    assertEquals(0L, shardedCounter.getCounter(COUNTER_ID1));
  }

  @Test
  public void shouldDecrementShardWithExcessUsage() {
    // init counter
    assertEquals(0, shardedCounter.getCounter(COUNTER_ID1));

    updateShard(COUNTER_ID1, 0, 1);
    updateShard(COUNTER_ID1, 1, 1);
    updateLimitInStorage(COUNTER_ID1, 1);

    // Invalidate snapshot to force pull changes from Datastore emulator
    shardedCounter.inMemSnapshot.invalidate(COUNTER_ID1);
    // assert cache is updated with the new value
    assertEquals(2L, shardedCounter.getCounter(COUNTER_ID1));

    //decrement counter by 1
    updateCounterInTransaction(COUNTER_ID1, -1L);

    // assert that the only shard in excess was chosen to be decremented
    assertEquals(1L, datastore.get(getKey(COUNTER_ID1, 0))
        .getLong(PROPERTY_SHARD_VALUE));
    assertEquals(0L, datastore.get(getKey(COUNTER_ID1, 1))
        .getLong(PROPERTY_SHARD_VALUE));

    // assert cache is updated with the new value
    shardedCounter.inMemSnapshot.invalidate(COUNTER_ID1);
    assertEquals(1L, shardedCounter.getCounter(COUNTER_ID1));
  }

  @Test
  public void shouldDecrementShardWithALotOfExcessUsage() {
    // init counter
    assertEquals(0, shardedCounter.getCounter(COUNTER_ID1));

    updateShard(COUNTER_ID1, 0, 10);
    updateShard(COUNTER_ID1, 1, 0);
    updateLimitInStorage(COUNTER_ID1, 1);

    // Invalidate snapshot to force pull changes from Datastore emulator
    shardedCounter.inMemSnapshot.invalidate(COUNTER_ID1);
    // assert cache is updated with the new value
    assertEquals(10L, shardedCounter.getCounter(COUNTER_ID1));

    //decrement counter by 1
    updateCounterInTransaction(COUNTER_ID1, -1L);

    // assert that the only shard in excess was chosen to be decremented
    assertEquals(9L, datastore.get(getKey(COUNTER_ID1, 0))
        .getLong(PROPERTY_SHARD_VALUE));
    assertEquals(0L, datastore.get(getKey(COUNTER_ID1, 1))
        .getLong(PROPERTY_SHARD_VALUE));

    // assert cache is updated with the new value
    shardedCounter.inMemSnapshot.invalidate(COUNTER_ID1);
    assertEquals(9L, shardedCounter.getCounter(COUNTER_ID1));
  }

  @Test
  public void shouldDecrementShardWithNoExcessUsage() {
    // init counter
    assertEquals(0, shardedCounter.getCounter(COUNTER_ID1));

    updateShard(COUNTER_ID1, 0, 1);
    updateShard(COUNTER_ID1, 1, 1);
    updateLimitInStorage(COUNTER_ID1, 2);

    // Invalidate snapshot to force pull changes from Datastore emulator
    shardedCounter.inMemSnapshot.invalidate(COUNTER_ID1);
    // assert cache is updated with the new value
    assertEquals(2L, shardedCounter.getCounter(COUNTER_ID1));

    //decrement counter by 1
    updateCounterInTransaction(COUNTER_ID1, -1L);

    assertThat(1L,
        either(is(datastore.get(getKey(COUNTER_ID1, 0)).getLong(PROPERTY_SHARD_VALUE)))
            .or(is(datastore.get(getKey(COUNTER_ID1, 1)).getLong(PROPERTY_SHARD_VALUE))));

    // assert cache is updated with the new value
    shardedCounter.inMemSnapshot.invalidate(COUNTER_ID1);
    assertEquals(1L, shardedCounter.getCounter(COUNTER_ID1));
  }

  @Test(expected = CounterCapacityException.class)
  public void shouldFailWhenIncreasingIfChosenShardIsFilledConcurrently() {
    // init counter and limit
    updateLimitInStorage(COUNTER_ID1, 1);
    assertEquals(0, shardedCounter.getCounter(COUNTER_ID1));

    shardedCounter = spy(shardedCounter);

    doAnswer(invocation -> {
      final Integer shardIndex = invocation.getArgumentAt(3, Integer.class);
      final String counterId = invocation.getArgumentAt(1, String.class);
      // Fill the chosen shard just before attempting to increment in storage
      updateShard(counterId, shardIndex, 1L);
      invocation.callRealMethod();
      return null;
    }).when(shardedCounter).updateCounterShard(any(StorageTransaction.class), anyString(),
        anyLong(), anyInt(), anyLong());

    //increment counter by 1
    updateCounterInTransaction(COUNTER_ID1, 1L);
  }

  @Test(expected = ShardNotFoundException.class)
  public void shouldFailWhenIncreasingIfChosenShardIsMissing() {
    // init counter and limit
    updateLimitInStorage(COUNTER_ID1, 1);
    assertEquals(0, shardedCounter.getCounter(COUNTER_ID1));

    shardedCounter = spy(shardedCounter);

    doAnswer(invocation -> {
      final Integer shardIndex = invocation.getArgumentAt(3, Integer.class);
      datastore.delete(getKey(COUNTER_ID1, shardIndex));
      invocation.callRealMethod();
      return null;
    }).when(shardedCounter).updateCounterShard(any(StorageTransaction.class), anyString(),
        anyLong(), anyInt(), anyLong());

    //increment counter by 1
    updateCounterInTransaction(COUNTER_ID1, 1L);
  }

  @Test(expected = CounterCapacityException.class)
  public void shouldNotIncrementIfUsageIsAboveLimitAndShardsHaveExcessUsage() {
    // init counter
    assertEquals(0, shardedCounter.getCounter(COUNTER_ID1));

    updateLimitInStorage(COUNTER_ID1, 1);
    // Update second shard despite limit being set to 1, possible if users decrease resource limit
    updateShard(COUNTER_ID1, 1, 1);

    // Invalidate snapshot to force pull changes from Datastore emulator
    shardedCounter.inMemSnapshot.invalidate(COUNTER_ID1);
    // assert cache is updated with the new value
    assertEquals(1L, shardedCounter.getCounter(COUNTER_ID1));

    updateCounterInTransaction(COUNTER_ID1, 1L);
  }

  @Test(expected = CounterCapacityException.class)
  public void shouldFailDecrementingEmptyCounter() {
    //increment counter by 1
    updateCounterInTransaction(COUNTER_ID1, -1L);
  }

  @Test(expected = CounterCapacityException.class)
  public void shouldFailIncrementingFullShard() throws IOException {
    shardedCounter.getCounter(COUNTER_ID1);
    updateShard(COUNTER_ID1, 0, 10);
    storage.runInTransaction(tx -> {
      shardedCounter.updateCounterShard(tx, COUNTER_ID1, 1, 0, 10);
      return null;
    });
  }

  @Test(expected = CounterCapacityException.class)
  public void shouldFailDecrementingEmptyShard() throws IOException {
    shardedCounter.getCounter(COUNTER_ID1);

    storage.runInTransaction(tx -> {
      shardedCounter.updateCounterShard(tx, COUNTER_ID1, -1, 0, 10);
      return null;
    });
  }

  @Test(expected = ShardNotFoundException.class)
  public void shouldThrowExceptionOnUninitializedShards() {
    when(counterSnapshotFactory.create(COUNTER_ID1)).thenReturn(new ShardedCounter.Snapshot(COUNTER_ID1, 100, new HashMap<>()));
    updateCounterInTransaction(COUNTER_ID1, -1L);
  }

  @Test
  public void shouldFailIncrementingFullCounter() throws IOException {
    assertEquals(0L, shardedCounter.getCounter(COUNTER_ID1));
    storage.runInTransaction(transaction -> {
      shardedCounter.updateLimit(transaction, COUNTER_ID1, 10);
      return null;
    });

    //invalidate cache so that the new limit value gets picked up
    shardedCounter.inMemSnapshot.invalidate(COUNTER_ID1);

    //increment counter by 1 until counter value gets to 10
    IntStream.range(0, 10).forEach(i -> {
      updateCounterInTransaction(COUNTER_ID1, 1L);
      shardedCounter.inMemSnapshot.invalidate(COUNTER_ID1);
    });

    // try another 10 times to update the counter
    IntStream.range(0, 10).forEach(i -> {
      try {
        updateCounterInTransaction(COUNTER_ID1, 1L);
        // if the update goes through, fail the test
        fail();
      } catch (DatastoreException | CounterCapacityException ignored) {
      }
    });
    shardedCounter.inMemSnapshot.invalidate(COUNTER_ID1);
    assertEquals(10L, shardedCounter.getCounter(COUNTER_ID1));
  }

  @Test
  public void shouldDeleteCounterAndLimit() throws IOException {
    //init counter
    assertEquals(0L, shardedCounter.getCounter(COUNTER_ID1));
    // create limit
    storage.runInTransaction(transaction -> {
      shardedCounter.updateLimit(transaction, COUNTER_ID1, 10);
      return null;
    });

    shardedCounter.deleteCounter(COUNTER_ID1);

    QueryResults<Entity> results = getShardsForCounter(COUNTER_ID1);

    assertFalse(results.hasNext());
    assertNull(getLimitFromStorage(COUNTER_ID1));
  }

  @Test
  public void shouldDeleteOnlySpecifiedCounterAndLimit() throws IOException {
    //init counter
    assertEquals(0L, shardedCounter.getCounter(COUNTER_ID1));
    assertEquals(0L, shardedCounter.getCounter(COUNTER_ID2));

    // create limit
    storage.runInTransaction(transaction -> {
      shardedCounter.updateLimit(transaction, COUNTER_ID1, 10);
      shardedCounter.updateLimit(transaction, COUNTER_ID2, 10);
      return null;
    });

    shardedCounter.deleteCounter(COUNTER_ID1);

    QueryResults<Entity> shardsCounter1 = getShardsForCounter(COUNTER_ID1);
    QueryResults<Entity> shardsCounter2 = getShardsForCounter(COUNTER_ID2);

    assertFalse(shardsCounter1.hasNext());
    assertNull(getLimitFromStorage(COUNTER_ID1));

    assertTrue(shardsCounter2.hasNext());
    assertNotNull(getLimitFromStorage(COUNTER_ID2));
  }

  @Test
  public void shouldPassDeletingNonExistingCounterAndLimit() throws IOException {
    shardedCounter.deleteCounter(COUNTER_ID1);

    QueryResults<Entity> results = getShardsForCounter(COUNTER_ID1);
    assertFalse(results.hasNext());
    assertNull(getLimitFromStorage(COUNTER_ID1));
  }

  private void updateCounterInTransaction(String counterId, long delta) {
    try {
      storage.runInTransaction(tx -> {
        shardedCounter.updateCounter(tx, counterId, delta);
        return null;
      });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void updateShard(String counterId, int shardIndex, long usage) {
    final Key key = getKey(counterId, shardIndex);
    datastore.put(Entity.newBuilder(key)
        .set(PROPERTY_COUNTER_ID, counterId)
        .set(PROPERTY_SHARD_INDEX, shardIndex)
        .set(PROPERTY_SHARD_VALUE, usage)
        .build());
  }

  private Key getKey(String counterId, int shardIndex) {
    return datastore.newKeyFactory().setKind(KIND_COUNTER_SHARD)
        .newKey(counterId + "-" + shardIndex);
  }

  private static void deleteAllOfKind(Datastore datastore, String kind) {
    QueryResults<Entity> results = datastore.run(EntityQuery.newEntityQueryBuilder()
        .setKind(kind)
        .build());
    while (results.hasNext()) {
      datastore.delete(results.next().getKey());
    }
  }

  private Entity getLimitFromStorage(String counterId) {
    return datastore.get(datastore.newKeyFactory().setKind(KIND_COUNTER_LIMIT).newKey(counterId));
  }

  private void updateLimitInStorage(String counterId, long limit) {
    datastore.put(Entity.newBuilder((datastore.newKeyFactory().setKind(KIND_COUNTER_LIMIT).newKey
        (counterId)))
        .set(PROPERTY_LIMIT, limit)
        .build());
  }

  private QueryResults<Entity> getShardsForCounter(String counterId) {
    return datastore.run(EntityQuery.newEntityQueryBuilder()
        .setKind(KIND_COUNTER_SHARD)
        .setFilter(PropertyFilter.eq(PROPERTY_COUNTER_ID, counterId)).build());
  }
}
