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

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreReaderWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.OrderBy;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Range;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of a resource counter on Datastore. Sharded in a way to support increment,
 * decrement and capping of a counter to a limit, with increased throughput (compared to the 1 write
 * per second limit on a single entity group) and strong consistency.
 *
 * <p>Note that getCounter is supported specifically _without_ strong consistency.
 *
 * <p>Note that the ShardedCounter stores state in the same Datastore database that the caller of this
 * class can access, too; guarantees don't apply if something else than ShardedCounter updates the
 * stored state.
 */
public class ShardedCounter {

  private static final Logger LOG = LoggerFactory.getLogger(ShardedCounter.class);

  // Ought to be enough (parallelism) for everyone. We could make it dynamic with extra effort.
  public static final int NUM_SHARDS = 128;
  public static final Instant CACHE_EXPIRY_DURATION = Instant.ofEpochMilli(1000);

  public static final String KIND_COUNTER_LIMIT = "CounterLimit";
  public static final String PROPERTY_LIMIT = "limit";

  public static final String KIND_COUNTER_SHARD = "CounterShard";
  public static final String PROPERTY_SHARD_VALUE = "value";
  public static final String PROPERTY_SHARD_INDEX = "index";
  public static final String PROPERTY_COUNTER_ID = "counterId";

  private final Time time;
  private final Datastore datastore;

  /**
   * A weakly consistent view of the state in Datastore, refreshed by ShardedCounter on demand.
   */
  private final Cache<String, CounterSnapshot> cache = CacheBuilder.newBuilder()
      .maximumSize(100_000)
      .build();

  private static class CounterSnapshot {

    final private String counterId;
    final private Instant updatedAt;
    final private Long limit;
    final private Map<Integer, Long> shards;
    final private Time time;

    public CounterSnapshot(Datastore datastore, String counterId, Time time) {
      limit = getLimit(datastore, counterId);

      Map<Integer, Long> fetchedShards = fetchShards(datastore, counterId);
      if (fetchedShards.size() < NUM_SHARDS) {
        // The counter probably has not been initialized (so we have empty QueryResults). Also
        // possible that a prior initialize() crashed halfway, or we got a partial list of shards in
        // QueryResults due to eventual consistency. In any case, repeated initialization eventually
        // creates all NUM_SHARDS shards.
        initialize(datastore, counterId);
        fetchedShards = fetchShards(datastore, counterId);
      }
      this.counterId = counterId;
      this.time = time;
      shards = fetchedShards;
      updatedAt = time.get();
    }

    /**
     * Idempotent initialization, so that we don't reset an existing shard to zero - counterId may
     * have already been initialized and incremented by another process.
     */
    private static void initialize(Datastore datastore, String counterId) {
      for (int i = 0; i < NUM_SHARDS; i++) {
        final int shardIndex = i;
        final Key shardKey = datastore.newKeyFactory().setKind(KIND_COUNTER_SHARD)
            .newKey(counterId + "-" + shardIndex);
        datastore.runInTransaction(transaction -> {
          final Entity shard = transaction.get(shardKey);
          if (shard == null) {
            transaction.put(Entity.newBuilder(shardKey)
                                .set(PROPERTY_COUNTER_ID, counterId)
                                .set(PROPERTY_SHARD_INDEX, shardIndex)
                                .set(PROPERTY_SHARD_VALUE, 0)
                                .build());
          }
          return null;
        });
      }
    }

    /**
     * Gets the counter state. Creates it in Datastore if it doesn't exist yet.
     */
    private static CounterSnapshot fromDatastore(Datastore datastore, String counterId, Time time) {
      return new CounterSnapshot(datastore, counterId, time);
    }

    private static Map<Integer, Long> fetchShards(Datastore datastore, String counterId) {
      final EntityQuery queryShards = EntityQuery.newEntityQueryBuilder()
          .setKind(KIND_COUNTER_SHARD)
          .setFilter(PropertyFilter.eq(PROPERTY_COUNTER_ID, counterId))
          .setOrderBy(OrderBy.asc("__key__"))
          .setLimit(NUM_SHARDS)
          .build();
      final QueryResults<Entity> shards = datastore.run(queryShards);
      final Map<Integer, Long> fetchedShards = new HashMap<>();
      while (shards.hasNext()) {
        Entity shard = shards.next();
        fetchedShards
            .put((int) shard.getLong(PROPERTY_SHARD_INDEX), shard.getLong(PROPERTY_SHARD_VALUE));
      }
      return fetchedShards;
    }

    private boolean isRecent() {
      return updatedAt.plus(CACHE_EXPIRY_DURATION.toEpochMilli(), ChronoUnit.MILLIS)
          .isAfter(time.get());
    }

    /**
     * The shard's capacity is calculated as:
     * 1/NUM_SHARDS part of the total count capacity
     * plus an extra 1 unit of the remainder (whenever limit % NUM_SHARDS > 0) depending on the shardIndex
     * The first (limit % NUM_SHARDS) shards get 1 unit extra, and the rest get 0.
     *
     * </p>ex. If limit=5 for a given counter and NUM_SHARDS=3,
     * then the distribution of capacity between the 3 shards will be [2, 2, 1]
     */
    private long shardCapacity(int shardIndex) {
      // The shard's capacity is calculated as:
      // 1/NUM_SHARDS part of the total count capacity
      // plus an extra 1 unit of the remainder (whenever limit % NUM_SHARDS > 0) depending on the shardIndex
      return limit / NUM_SHARDS + (shardIndex < limit % NUM_SHARDS ? 1 : 0);
    }

    /**
     * Returns shard index which _likely_ could be successfully updated by delta, according to our
     * cached view of the state in Datastore.
     */
    private int pickShardWithSpareCapacity(long delta) throws InvalidShardStateException {
      List<Integer> candidates = new ArrayList<>();

      for (int i : shards.keySet()) {
        if (shards.containsKey(i) && Range
            .closed(0L, shardCapacity(i)).contains(shards.get(i) + delta)) {
          candidates.add(i);
        } else if (!shards.containsKey(i)) {
          LOG.warn("Shard {} for counter {} is not present in local cache", i, counterId);
        }
      }

      if (candidates.isEmpty()) {
        if (shards.size() == 0) {
          throw new InvalidShardStateException(
              "Trying to operate with a potentially uninitialized counter. Cache needs to be updated first.");
        } else {
          throw new CounterCapacityException("No shard for counter %s has capacity for delta %s",
                                             counterId, delta);
        }
        // Or return -1 (and use that to abort the transaction early)?
      } else {
        return candidates.get(new Random().nextInt(candidates.size()));
      }
    }
  }

  public ShardedCounter(Datastore datastore, Time time) {
    this.datastore = Objects.requireNonNull(datastore);
    this.time = time;
  }

  /**
   * Returns a recent snapshot.
   */
  private CounterSnapshot getCounterSnapshot(String counterId) {
    final CounterSnapshot snapshot = cache.getIfPresent(counterId);
    if (snapshot != null && snapshot.isRecent()) {
      return snapshot;
    }
    return refreshCounterSnapshot(counterId);
  }

  /**
   * Update cached snapshot with most recent state of counter in Datastore
   */
  private CounterSnapshot refreshCounterSnapshot(String counterId) {
    final CounterSnapshot newSnapshot = CounterSnapshot.fromDatastore(datastore, counterId, time);
    cache.put(counterId, newSnapshot);
    return newSnapshot;
  }

  /**
   * Must be called within a TransactionCallable. (?)
   *
   * <p>Augments the transaction with operations to persist the given limit in Datastore. So long as
   * there has been no preceding successful updateLimit operation, no limit is applied in
   * updateCounter operations on this counter.
   */
  public void updateLimit(DatastoreReaderWriter transaction, String counterId, long limit) {
    final Key limitKey = datastore.newKeyFactory().setKind(KIND_COUNTER_LIMIT).newKey(counterId);

    transaction.put(Entity.newBuilder(limitKey).set(PROPERTY_LIMIT, limit).build());
  }

  /**
   * Reads the latest limit value from Datastore, for the specified {@param counterId}
   */
  public static long getLimit(Datastore datastore, String counterId) {
    final Key limitKey = datastore.newKeyFactory().setKind(KIND_COUNTER_LIMIT).newKey(counterId);
    final Entity limitEntity = datastore.get(limitKey);
    if (limitEntity == null) {
      return Long.MAX_VALUE;
      // Or IllegalStateException("No limit found in Datastore for " + counterId);?
    } else {
      return limitEntity.getLong(PROPERTY_LIMIT);
    }
  }

  /**
   * Must be called within a TransactionCallable. Augments the transaction with certain operations
   * that strongly consistently increment resp. decrement the counter referred to by counterId, and
   * cause the transaction to fail to commit if the counter's associated limit is exceeded. Also
   * spurious failures are possible.
   *
   * <p>Delta should be +/-1 for graceful behavior, due to how sharding is currently implemented.
   * Updates with a larger delta are prone to spuriously fail even when the counter is not near to
   * exceeding its limit. Failures are certain when delta >= limit / NUM_SHARDS + 1.
   */
  public void updateCounter(DatastoreReaderWriter transaction, String counterId, long delta) {
    CounterSnapshot snapshot = getCounterSnapshot(counterId);
    int shardIndex;
    try {
      shardIndex = snapshot.pickShardWithSpareCapacity(delta);
    } catch (InvalidShardStateException e) {
      try {
        snapshot = refreshCounterSnapshot(counterId);
        shardIndex = snapshot.pickShardWithSpareCapacity(delta);
      } catch (InvalidShardStateException e1) {
        LOG.warn(
            "Invalid shard state for counter {} but cache has already been reloaded for counter. {}",
            counterId, e1);
        return;
      }
    }
    final Key shardKey = datastore.newKeyFactory().setKind(KIND_COUNTER_SHARD)
        .newKey(counterId + "-" + shardIndex);
    final Entity shard = transaction.get(shardKey);

    final long newCounterValue = shard.getLong(PROPERTY_SHARD_VALUE) + delta;
    if (shard != null && Range.closed(0L, snapshot.shardCapacity(shardIndex))
        .contains(newCounterValue) && newCounterValue <= getLimit(datastore, counterId)) {
      transaction.put(Entity.newBuilder(shard)
                          .set(PROPERTY_SHARD_VALUE,
                               shard.getLong(PROPERTY_SHARD_VALUE) + delta)
                          .build());
    } else {
      // TODO fail (rollback) the transaction
      throw new ConcurrentModificationException(
          "Chosen shard has no spare capacity anymoar, pls rollback kths");
    }
  }

  /**
   * Returns the current value of the counter referred to by counterId, a weakly consistent
   * estimate. (May have not truly been the counter value at any point in time. Even a return value
   * larger than the corresponding limit might be possible without error.)
   */
  public long getCounter(String counterId) {
    final CounterSnapshot snapshot = getCounterSnapshot(counterId);
    long result = 0;

    for (long shard : snapshot.shards.values()) {
      result += shard;
    }
    return result;
  }

  /**
   * Delete counter by counterId. Deletes both counter shards and counter limit if it exists.
   *
   * <p>There may be inconsistencies between instances, as each stores an in-memory cache, whose
   * state may differ from what's stored in Datastore. Only after a cache expiry will other instances
   * notice that a counter has been deleted.
   *
   * <p>Due to Datastore limitations (modify max 25 entity groups per transaction),
   * deletion of shards is done in batches of 25 entity groups.
   *
   * <p>Behaviour is non-determined if other instances try to access the same counter in the meantime.
   * Best results are achieved if all usages of the given resource - which the counter is associated with -
   * are removed before calling deleteCounter. This is so, in order to avoid a scenario where one instance
   * is trying to delete all shards, while another is creating/updating shards in between the
   * multiple transactions made by this method.
   */
  public void deleteCounter(Datastore datastore, String counterId) {
    QueryResults<Entity> results = datastore.run(EntityQuery.newEntityQueryBuilder()
                                                     .setKind(KIND_COUNTER_SHARD)
                                                     .setFilter(PropertyFilter
                                                                    .eq(PROPERTY_COUNTER_ID,
                                                                        counterId))
                                                     .build());
    while (results.hasNext()) {
      removeEntities(datastore, results);
    }

    // delete limit entry too
    datastore.runInTransaction(transaction -> {
      transaction.delete(datastore.newKeyFactory().setKind(KIND_COUNTER_LIMIT).newKey(counterId));
      return null;
    });
  }

  private void removeEntities(Datastore datastore, QueryResults<Entity> results) {
    datastore.runInTransaction(transaction -> {
      IntStream.range(0, 25).forEach(i -> {
        if (results.hasNext()) {
          transaction.delete(results.next().getKey());
        }
      });
      return null;
    });
  }
}
