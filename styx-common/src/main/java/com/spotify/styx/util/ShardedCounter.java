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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Range;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
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
  public static final Duration CACHE_EXPIRY_DURATION = Duration.ofMillis(1000);

  public static final String KIND_COUNTER_LIMIT = "CounterLimit";
  public static final String PROPERTY_LIMIT = "limit";

  public static final String KIND_COUNTER_SHARD = "CounterShard";
  public static final String PROPERTY_SHARD_VALUE = "value";
  public static final String PROPERTY_SHARD_INDEX = "index";
  public static final String PROPERTY_COUNTER_ID = "counterId";

  private final Storage storage;

  /**
   * A weakly consistent view of the state in Datastore, refreshed by ShardedCounter on demand.
   */
  @VisibleForTesting final Cache<String, CounterSnapshot> inMemSnapshot = CacheBuilder.newBuilder()
      .maximumSize(100_000)
      .expireAfterWrite(CACHE_EXPIRY_DURATION.toMillis(), TimeUnit.MILLISECONDS)
      .build();

  private CounterSnapshotFactory counterSnapshotFactory;

  public static class Snapshot implements CounterSnapshot {

    private final String counterId;
    private final Long limit;
    private final Map<Integer, Long> shards;

    public Snapshot(Storage storage, String counterId, Map<Integer, Long> shards) {
      this.limit = getLimit(storage, counterId);
      this.shards = shards;
      this.counterId = counterId;
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
    public long shardCapacity(int shardIndex) {
      return limit / NUM_SHARDS + (shardIndex < limit % NUM_SHARDS ? 1 : 0);
    }

    @Override
    public Map<Integer, Long> getShards() {
      return shards;
    }

    /**
     * Returns shard index which _likely_ could be successfully updated by delta, according to our
     * cached view of the state in Datastore.
     */
    public int pickShardWithSpareCapacity(long delta) {
      List<Integer> candidates = new ArrayList<>();

      shards.keySet().forEach(index -> {
        if (shards.containsKey(index) && Range
            .closed(0L, shardCapacity(index)).contains(shards.get(index) + delta)) {
          candidates.add(index);
        }
      });

      if (candidates.isEmpty()) {
        if (shards.size() == 0) {
          LOG.info(
              "Trying to operate with a potentially uninitialized counter {}. Cache needs to be updated first.",
              counterId);
          return new Random().nextInt((int) Math.min(NUM_SHARDS, limit));
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

  public ShardedCounter(Storage storage, CounterSnapshotFactory counterSnapshotFactory) {
    this.storage = storage;
    this.counterSnapshotFactory = counterSnapshotFactory;
  }

  /**
   * Returns a recent snapshot, possibly read from inMemSnapshot.
   */
  private CounterSnapshot getCounterSnapshot(String counterId) {
    final CounterSnapshot snapshot = inMemSnapshot.getIfPresent(counterId);
    if (snapshot != null) {
      return snapshot;
    }
    return refreshCounterSnapshot(counterId);
  }

  /**
   * Update cached snapshot with most recent state of counter in Datastore.
   */
  private CounterSnapshot refreshCounterSnapshot(String counterId) {
    final CounterSnapshot newSnapshot = counterSnapshotFactory.create(counterId);
    inMemSnapshot.put(counterId, newSnapshot);
    return newSnapshot;
  }

  /**
   * Must be called within a TransactionCallable. (?)
   *
   * <p>Augments the transaction with operations to persist the given limit in Datastore. So long as
   * there has been no preceding successful updateLimit operation, no limit is applied in
   * updateCounter operations on this counter.
   */
  public void updateLimit(StorageTransaction tx, String counterId, long limit) {
    tx.updateLimitForCounter(counterId, limit);
  }

  /**
   * Reads the latest limit value from Datastore, for the specified {@param counterId}
   */
  public static long getLimit(Storage storage, String counterId) {
    return storage.getLimitForCounter(counterId);
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
  public void updateCounter(StorageTransaction transaction, String counterId, long delta) {
    CounterSnapshot snapshot = getCounterSnapshot(counterId);
    int shardIndex = snapshot.pickShardWithSpareCapacity(delta);

    final Optional<Shard> shard = transaction.shard(counterId, shardIndex);

    if (shard.isPresent()) {
      final long newShardValue = shard.get().value() + delta;
      if (Range.closed(0L, snapshot.shardCapacity(shardIndex))
          .contains(newShardValue)) {
        transaction.store(Shard.create(counterId, shardIndex, (int) (shard.get().value() + delta)));
      } else {
        throw new CounterCapacityException("Chosen shard %s-%s has no more capacity.",
                                           counterId, shardIndex);
      }
    } else {
      throw new ShardNotFoundException(
          "Could not find shard %s-%s. Unexpected Datastore corruption or our bug - the code should've "
          + "called initialize() before reaching this point, and any particular shard should "
          + "strongly be get()-able thereafter",
          counterId, shardIndex);
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

    for (long shardValue : snapshot.getShards().values()) {
      result += shardValue;
    }
    return result;
  }

  /**
   * Delete counter by counterId. Deletes both counter shards and counter limit if it exists.
   *
   * <p>Due to Datastore limitations (modify max 25 entity groups per transaction),
   * deletion of shards is done in batches of 25 shards.
   *
   * <p>Behaviour is best-effort and non-determined if other instances try to access the same counter in the meantime.
   * Best results are achieved if all usages of the given resource - which the counter is associated with -
   * are removed before calling deleteCounter. This is so, in order to avoid a scenario where one instance
   * is trying to delete all shards, while another is creating/updating shards in between the
   * multiple transactions made by this method.
   */
  //TODO: remove storage parameter. use class field
  public void deleteCounter(Storage storage, String counterId) throws IOException {
    storage.deleteShardsForCounter(counterId);
    storage.deleteLimitForCounter(counterId);
  }
}
