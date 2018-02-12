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

import static com.spotify.styx.util.ShardedCounter.CACHE_EXPIRY_DURATION;
import static com.spotify.styx.util.ShardedCounter.KIND_COUNTER_LIMIT;
import static com.spotify.styx.util.ShardedCounter.KIND_COUNTER_SHARD;
import static com.spotify.styx.util.ShardedCounter.PROPERTY_COUNTER_ID;
import static com.spotify.styx.util.ShardedCounter.PROPERTY_LIMIT;
import static com.spotify.styx.util.ShardedCounter.PROPERTY_SHARD_INDEX;
import static com.spotify.styx.util.ShardedCounter.PROPERTY_SHARD_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ShardedCounterTest {

  private static Instant now = Instant.now();
  private static final Time TIME = () -> now;
  private static final String COUNTER_ID1 = "resource_counter_1";
  private static final String COUNTER_ID2 = "resource_counter_2";


  private static LocalDatastoreHelper helper;
  private static ShardedCounter shardedCounter;
  private static Datastore datastore;

  @BeforeClass
  public static void setUpClass() throws IOException, InterruptedException {
    helper = LocalDatastoreHelper.create(1.0);
    helper.start();
    datastore = helper.getOptions().getService();
  }

  @Before
  public void setUp() throws IOException, InterruptedException {
    shardedCounter = new ShardedCounter(datastore, TIME);
  }

  @After
  public void tearDown() throws IOException, InterruptedException {
    clearDatastore();
  }


  @Test
  public void shouldCreateCounterEmpty() {
    assertEquals(shardedCounter.getCounter(COUNTER_ID1), 0L);

    QueryResults<Entity> results = getShardsForCounter(COUNTER_ID1);
    // assert all shards exist
    IntStream.range(0, ShardedCounter.NUM_SHARDS).forEach(i -> {
      assertTrue(results.hasNext());
      results.next();
    });

  }

  @Test
  public void shouldCreateLimit() {
    assertNull(getLimitForCounter(COUNTER_ID1));

    datastore.runInTransaction(transaction -> {
      shardedCounter.updateLimit(transaction, COUNTER_ID1, 500);
      return null;
    });

    assertEquals(500L, getLimitForCounter(COUNTER_ID1).getLong(PROPERTY_LIMIT));
  }

  @Test
  public void shoudIncrementCounter() {
    now = Instant.parse("2018-01-01T00:00:00.000Z");
    assertEquals(0, shardedCounter.getCounter(COUNTER_ID1));

    //increment counter by 1
    updateCounterInTransaction(COUNTER_ID1, 1L);

    QueryResults<Entity> results = datastore.run(EntityQuery.newEntityQueryBuilder()
                                                     .setKind(KIND_COUNTER_SHARD)
                                                     .setFilter(CompositeFilter.and(
                                                         PropertyFilter
                                                             .eq(PROPERTY_COUNTER_ID,
                                                                 COUNTER_ID1),
                                                         PropertyFilter
                                                             .eq(PROPERTY_SHARD_VALUE,
                                                                 1)))
                                                     .build());
    // assert there's one and only one shard with the value set to 1
    assertEquals(1L, results.next().getLong(PROPERTY_SHARD_VALUE));
    assertFalse(results.hasNext());

    // assert the correct value is fetched after cache expiry
    now = afterCacheExpiryDuration(now);
    assertEquals(1L, shardedCounter.getCounter(COUNTER_ID1));
  }

  @Test
  public void shoudDecrementCounter() {
    now = Instant.parse("2018-01-01T00:00:00.000Z");
    // init counter
    assertEquals(0, shardedCounter.getCounter(COUNTER_ID1));

    final int shardIndex = 3;
    final Key shardKey = datastore.newKeyFactory().setKind(KIND_COUNTER_SHARD)
        .newKey(COUNTER_ID1 + "-" + shardIndex);
    //increment counter shard by 1
    datastore.put(Entity.newBuilder(shardKey)
                      .set(PROPERTY_COUNTER_ID, COUNTER_ID1)
                      .set(PROPERTY_SHARD_INDEX, shardIndex)
                      .set(PROPERTY_SHARD_VALUE, 1)
                      .build());

    now = afterCacheExpiryDuration(now);
    // assert cache is updated with the new value
    assertEquals(1L, shardedCounter.getCounter(COUNTER_ID1));

    //decrement counter by 1
    updateCounterInTransaction(COUNTER_ID1, -1L);

    // assert that the only eligible shard was chosen to be decremented
    assertEquals(0L, datastore.get(shardKey).getLong(PROPERTY_SHARD_VALUE));

    // assert cache is updated with the new value
    now = afterCacheExpiryDuration(now);
    assertEquals(0L, shardedCounter.getCounter(COUNTER_ID1));
  }


  @Test(expected = DatastoreException.class)
  public void shoudFailDecrementingEmptyCounter() {
    now = Instant.parse("2018-01-01T00:00:00.000Z");
    //increment counter by 1
    updateCounterInTransaction(COUNTER_ID1, -1L);
  }

  @Test
  public void shoudFailIncrementingFullCounter() {
    now = Instant.parse("2018-01-01T00:00:00.000Z");
    assertEquals(0L, shardedCounter.getCounter(COUNTER_ID1));
    datastore.runInTransaction(transaction -> {
      shardedCounter.updateLimit(transaction, COUNTER_ID1, 10);
      return null;
    });

    //expire cache so that the new limit value gets picked up
    now = afterCacheExpiryDuration(now);

    //increment counter by 1 until counter value gets to 10
    while (shardedCounter.getCounter(COUNTER_ID1) < 10) {
      try {
        IntStream.range(0, 10).forEach(i -> updateCounterInTransaction(COUNTER_ID1, 1L));
      } catch (DatastoreException ignored) {
      }
      now = afterCacheExpiryDuration(now);
    }

    // try another 10 times to update the counter
    IntStream.range(0, 10).forEach(i -> {
      try {
        updateCounterInTransaction(COUNTER_ID1, 1L);
        // if the update goes through, fail the test
        fail();
      } catch (DatastoreException ignored) {
      }
    });
    now = afterCacheExpiryDuration(now);

    assertEquals(10L, shardedCounter.getCounter(COUNTER_ID1));
  }

  @Test
  public void shouldDeleteCounterAndLimit() {
    now = Instant.parse("2018-01-01T00:00:00.000Z");
    //init counter
    assertEquals(0L, shardedCounter.getCounter(COUNTER_ID1));
    // create limit
    datastore.runInTransaction(transaction -> {
      shardedCounter.updateLimit(transaction, COUNTER_ID1, 10);
      return null;
    });

    shardedCounter.deleteCounter(datastore, COUNTER_ID1);

    QueryResults<Entity> results = getShardsForCounter(COUNTER_ID1);

    assertFalse(results.hasNext());
    assertNull(getLimitForCounter(COUNTER_ID1));
  }

  @Test
  public void shouldDeleteOnlySpecifiedCounterAndLimit() {
    now = Instant.parse("2018-01-01T00:00:00.000Z");
    //init counter
    assertEquals(0L, shardedCounter.getCounter(COUNTER_ID1));
    assertEquals(0L, shardedCounter.getCounter(COUNTER_ID2));

    // create limit
    datastore.runInTransaction(transaction -> {
      shardedCounter.updateLimit(transaction, COUNTER_ID1, 10);
      shardedCounter.updateLimit(transaction, COUNTER_ID2, 10);
      return null;
    });

    shardedCounter.deleteCounter(datastore, COUNTER_ID1);

    QueryResults<Entity> shardsCounter1 = getShardsForCounter(COUNTER_ID1);
    QueryResults<Entity> shardsCounter2 = getShardsForCounter(COUNTER_ID2);

    assertFalse(shardsCounter1.hasNext());
    assertNull(getLimitForCounter(COUNTER_ID1));

    assertTrue(shardsCounter2.hasNext());
    assertNotNull(getLimitForCounter(COUNTER_ID2));
  }

  @Test
  public void shouldPassDeletingNonExistingCounterAndLimit() {
    now = Instant.parse("2018-01-01T00:00:00.000Z");

    shardedCounter.deleteCounter(datastore, COUNTER_ID1);

    QueryResults<Entity> results = getShardsForCounter(COUNTER_ID1);
    assertFalse(results.hasNext());
    assertNull(getLimitForCounter(COUNTER_ID1));
  }


  /**
   * TODO: We should be able to decrease a counter limit and keep a valid state for the counter shards.
   *
   * <p>Ex. Counter is at 75% usage. Decrease the limit for 50%.
   * That leaves the counter at 25% extra usage (now 50% relative to the new limit).
   * This state is still valid, but we should not allow further increases, only decreases.
   * When the counter usage goes below the new limit, then we should allow increase operations again.
   */
  @Test
  public void decreaseLimitBelowCurrentCounterValue() {
    //1. increase counter to an X value
    //2. lower the limit to a L < X value
    //3. fail further increases, but allow for decreases of the counter
  }

  private void updateCounterInTransaction(String counterId, long delta) {
    datastore.runInTransaction(rw -> {
      shardedCounter.updateCounter(rw, counterId, delta);
      return null;
    });
  }

  private Instant afterCacheExpiryDuration(Instant now) {
    return now.plus(CACHE_EXPIRY_DURATION.toMillis(), ChronoUnit.MILLIS);
  }


  private static void clearDatastore() {
    deleteAllOfKind(KIND_COUNTER_SHARD);
    deleteAllOfKind(KIND_COUNTER_LIMIT);
  }

  private static void deleteAllOfKind(String kind) {
    QueryResults<Entity> results = datastore.run(EntityQuery.newEntityQueryBuilder()
                                                     .setKind(kind)
                                                     .build());
    while (results.hasNext()) {
      datastore.delete(results.next().getKey());
    }
  }

  private Entity getLimitForCounter(String counterId) {
    return datastore.get(datastore.newKeyFactory().setKind(KIND_COUNTER_LIMIT).newKey(counterId));
  }

  private QueryResults<Entity> getShardsForCounter(String counterId) {
    return datastore.run(EntityQuery.newEntityQueryBuilder()
                             .setKind(KIND_COUNTER_SHARD)
                             .setFilter(PropertyFilter
                                            .eq(PROPERTY_COUNTER_ID,
                                                counterId)).build());
  }

}
