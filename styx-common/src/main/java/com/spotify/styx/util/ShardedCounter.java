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
import com.google.cloud.datastore.StructuredQuery.CompositeFilter;
import com.google.cloud.datastore.StructuredQuery.OrderBy;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * Implementation of a resource counter on Datastore. Sharded in a way to support increment,
 * decrement and capping of a counter to a limit, with increased throughput (compared to the 1 write
 * per second limit on a single entity group) and strong consistency.
 *
 * Note that getCounter is supported specifically _without_ strong consistency.
 *
 * Note that the ShardedCounter stores state in the same Datastore database that the caller of this
 * class can access, too; guarantees don't apply if something else than ShardedCounter updates the
 * stored state.
 */
public class ShardedCounter {

  // Ought to be enough (parallelism) for everyone. We could make it dynamic with extra effort.
  private static final int NUM_SHARDS = 128;
  private static final long CACHE_EXPIRY_MILLIS = 1000;

  private final Datastore datastore;
  public static final String KIND_COUNTER_LIMIT = "CounterLimit";
  public static final String PROPERTY_LIMIT = "limit";
  public static final String KIND_COUNTER_SHARD = "CounterShard";
  public static final String PROPERTY_SHARD = "shard";

  private static class CounterSnapshot {
    Instant updatedAt;
    Long limit;
    ArrayList<Long> shards;

    private static CounterSnapshot fromDatastore(Datastore datastore, String counterId) {
      CounterSnapshot snapshot = new CounterSnapshot();

      final Key limitKey = datastore.newKeyFactory().setKind(KIND_COUNTER_LIMIT).newKey(counterId);
      final Entity limit = datastore.get(limitKey);
      if (limit == null) {
        snapshot.limit = Long.MAX_VALUE;
        // Or IllegalStateException("No limit found in Datastore for " + counterId);?
      } else {
        snapshot.limit = limit.getLong(PROPERTY_LIMIT);
      }

      final EntityQuery queryShards = EntityQuery.newEntityQueryBuilder()
          .setKind(KIND_COUNTER_SHARD)
          .setFilter(CompositeFilter.and(
              PropertyFilter.ge("__key__", counterId + "-0"),
              PropertyFilter.lt("__key__", counterId + "-" + NUM_SHARDS)))
          .setOrderBy(OrderBy.asc("__key__"))
          .setLimit(NUM_SHARDS + 1)
          .build();
      final QueryResults<Entity> shards = datastore.run(queryShards);
      while (shards.hasNext()) {
        long nextShard = shards.next().getLong(PROPERTY_SHARD);
        snapshot.shards.add(nextShard);
      }
      if (snapshot.shards.size() != NUM_SHARDS) {
        // This could occur as part of normal operation, due to eventual consistency. Do nothing?
        // Or IllegalStateException("Wrong number of shards found in Datastore for " + counterId);?
      }

      snapshot.updatedAt = Instant.now();
      return snapshot;
    }

    private boolean isRecent() {
      return updatedAt.plus(CACHE_EXPIRY_MILLIS, ChronoUnit.MILLIS).isAfter(Instant.now());
    }

    private long shardCapacity(int shardIndex) {
      return limit / NUM_SHARDS + (shardIndex < limit % NUM_SHARDS ? 1 : 0);
    }

    private int pickShardWithSpareCapacity() {
      List<Integer> candidates = new ArrayList<>();

      for (int i = 0; i < shards.size(); i++) {
        if (shards.get(i) < shardCapacity(i)) {
          candidates.add(i);
        }
      }

      if (candidates.isEmpty()) {
        return new Random().nextInt(NUM_SHARDS);
        // Or return -1 (and use that to abort the transaction early)?
      } else {
        return candidates.get(new Random().nextInt(candidates.size()));
      }
    }
  }

  /**
   * A weakly consistent view of the state in Datastore, refreshed by ShardedCounter on demand.
   */
  private Map<String, CounterSnapshot> counterCache;

  public ShardedCounter(Datastore datastore) {
    this.datastore = Objects.requireNonNull(datastore);
    this.counterCache = new HashMap<>();
  }

  /**
   * Returns a recent snapshot.
   */
  private CounterSnapshot getCounterSnapshot(String counterId) {
    if (counterCache.containsKey(counterId) && counterCache.get(counterId).isRecent()) {
      return counterCache.get(counterId);
    }
    return counterCache.put(counterId, CounterSnapshot.fromDatastore(datastore, counterId));
  }

  /**
   * Must be called within a TransactionCallable. (?)
   *
   * Augments the transaction with operations to persist the given limit in Datastore. So long as
   * there has been no preceding successful updateLimit operation, no limit is applied in
   * updateCounter operations on this counter.
   */
  public void updateLimit(DatastoreReaderWriter transaction, String counterId, long limit) {
    final Key limitKey = datastore.newKeyFactory().setKind(KIND_COUNTER_LIMIT).newKey(counterId);

    transaction.put(Entity.newBuilder(limitKey).set(PROPERTY_LIMIT, limit).build());
  }

  /**
   * Must be called within a TransactionCallable. Augments the transaction with certain operations
   * that strongly consistently increment resp. decrement the counter referred to by counterId, and
   * cause the transaction to fail to commit if the counter's associated limit is exceeded. Also
   * spurious failures are possible.
   *
   * Delta should be +/-1 for graceful behavior, due to how sharding is currently implemented.
   * Updates with a larger delta are prone to spuriously fail even when counter is not near to
   * exceeding its limit. Failures are certain when delta >= limit / NUM_SHARDS + 1.
   *
   * (TODO: what about decrements below zero - make fail?)
   */
  public void updateCounter(DatastoreReaderWriter transaction, String counterId, long delta) {
    final CounterSnapshot snapshot = getCounterSnapshot(counterId);
    final int shardIndex = snapshot.pickShardWithSpareCapacity();
    final Key shardKey = datastore.newKeyFactory().setKind(KIND_COUNTER_SHARD)
        .newKey(counterId + "-" + shardIndex);
    final Entity shard = transaction.get(shardKey);

    if (shard != null && shard.getLong(PROPERTY_SHARD) < snapshot.shardCapacity(shardIndex)) {
      transaction.put(Entity.newBuilder(shard)
          .set(PROPERTY_SHARD, shard.getLong(PROPERTY_SHARD) + delta)
          .build());
    } else {
      // TODO fail the transaction
    }
  }

  /**
   * Returns the current value of the counter referred to by counterId, a weakly consistent
   * estimate. (May have not truly been the counter value at any point in time. Even a return value
   * larger than the corresponding limit is possible without error.)
   */
  public long getCounter(String counterId) {
    final CounterSnapshot snapshot = getCounterSnapshot(counterId);
    long result = 0;

    for (long shard: snapshot.shards) {
      result += shard;
    }
    return result;
  }
}
