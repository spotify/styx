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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.QueryResults;
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
  }

  @Test
  public void shoudIncrementCounter() {
    now = Instant.parse("2018-01-01T00:00:00.000Z");
    assertEquals(0, shardedCounter.getCounter(COUNTER_ID1));

    //increment counter by 1
    updateCounterInTransaction(COUNTER_ID1, 1L);

    now = afterCacheExpiryDuration(now);

    assertEquals(1L, shardedCounter.getCounter(COUNTER_ID1));
  }

  private void updateCounterInTransaction(String counterId1, long delta) {
    datastore.runInTransaction(rw -> {
      shardedCounter.updateCounter(rw, COUNTER_ID1, delta);
      return null;
    });
  }

  @Test
  public void shoudDecrementCounter() {
    now = Instant.parse("2018-01-01T00:00:00.000Z");
    //increment counter by 1
    updateCounterInTransaction(COUNTER_ID1, 1L);

    now = afterCacheExpiryDuration(now);

    assertEquals(1L, shardedCounter.getCounter(COUNTER_ID1));

    //decrement counter by 1
    updateCounterInTransaction(COUNTER_ID1, -1L);

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


    //increment counter by 1, x 10 times
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
  public void decreaseLimitAndGracefullyDecreaseCounter() {
    //1. increase counter to an X value
    //2. lower the limit to a L < X value
    //3. fail further increases, but allow for decreases of the counter
  }


  private Instant afterCacheExpiryDuration(Instant now) {
    return now.plus(ShardedCounter.CACHE_EXPIRY_DURATION.toEpochMilli(), ChronoUnit.MILLIS);
  }


  private static void clearDatastore() {
    deleteAllOfKind(ShardedCounter.KIND_COUNTER_SHARD);
    deleteAllOfKind(ShardedCounter.KIND_COUNTER_LIMIT);
  }

  private static void deleteAllOfKind(String kind) {
    QueryResults<Entity> results = datastore.run(EntityQuery.newEntityQueryBuilder()
                                                     .setKind(kind)
                                                     .build());
    while (results.hasNext()) {
      Entity entity = results.next();
      datastore.delete(entity.getKey());
    }
  }

}
