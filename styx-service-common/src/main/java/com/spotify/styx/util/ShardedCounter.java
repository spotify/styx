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

import static java.util.stream.Collectors.toList;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Range;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.storage.Storage;
import com.spotify.styx.storage.StorageTransaction;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  private static final Duration CACHE_EXPIRY_DURATION = Duration.ofMillis(1000);

  public static final String KIND_COUNTER_LIMIT = "CounterLimit";
  public static final String PROPERTY_LIMIT = "limit";

  public static final String KIND_COUNTER_SHARD = "CounterShard";
  public static final String PROPERTY_SHARD_VALUE = "value";
  public static final String PROPERTY_SHARD_INDEX = "index";
  public static final String PROPERTY_COUNTER_ID = "counterId";

  private final Stats stats;
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

    Snapshot(String counterId, long limit, Map<Integer, Long> shards) {
      this.counterId = Objects.requireNonNull(counterId);
      this.limit = limit;
      this.shards = Objects.requireNonNull(shards);
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
    public long getLimit() {
      return limit;
    }

    /**
     * Returns the current value of the counter referred to by counterId, a weakly consistent
     * estimate. (May have not truly been the counter value at any point in time. Even a return value
     * larger than the corresponding limit might be possible without error.)
     */
    @Override
    public long getTotalUsage() {
      return shards.values().stream().mapToLong(i -> i).sum();
    }

    /**
     * Returns shard index which _likely_ has excess usage, as per our cached view of the state in Datastore.
     */
    @Override
    public Optional<Integer> pickShardWithExcessUsage(long delta) {
      final List<Integer> candidates = shards.keySet().stream()
          .filter(index -> shards.get(index) > shardCapacity(index))
          .filter(index -> shards.get(index) + delta >= 0)
          .collect(toList());

      if (candidates.isEmpty()) {
        return Optional.empty();
      } else {
        return Optional.of(candidates.get(new Random().nextInt(candidates.size())));
      }
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
      if (delta > 0 && getTotalUsage() >= getLimit()) {
        final String message = String.format("No shard for counter %s has capacity for delta %s",
            counterId, delta);
        LOG.info(message);
        throw new CounterCapacityException(message);
      }

      List<Integer> candidates = shards.keySet().stream()
          .filter(index -> Range.closed(0L, shardCapacity(index)).contains(shards.get(index) + delta))
          .collect(toList());

      if (candidates.isEmpty()) {
        if (shards.size() == 0) {
          final String message = "Trying to operate with a potentially uninitialized counter "
                                 + counterId + ". Cache needs to be updated first.";
          LOG.error(message);
          throw new ShardNotFoundException(message);
        } else {
          final String message = String.format("No shard for counter %s has capacity for delta %s",
              counterId, delta);
          LOG.info(message);
          throw new CounterCapacityException(message);
        }
        // Or return -1 (and use that to abort the transaction early)?
      } else {
        return candidates.get(new Random().nextInt(candidates.size()));
      }
    }
  }

  public ShardedCounter(Stats stats, CounterSnapshotFactory counterSnapshotFactory) {
    this.stats = Objects.requireNonNull(stats);
    this.counterSnapshotFactory = Objects.requireNonNull(counterSnapshotFactory);
  }

  /**
   * Returns a recent snapshot, possibly read from inMemSnapshot.
   */
  CounterSnapshot getCounterSnapshot(String counterId) throws IOException {
    final CounterSnapshot snapshot = inMemSnapshot.getIfPresent(counterId);
    if (snapshot != null) {
      stats.recordCounterCacheHit();
      return snapshot;
    }
    stats.recordCounterCacheMiss();
    return refreshCounterSnapshot(counterId);
  }

  /**
   * Update cached snapshot with most recent state of counter in Datastore.
   */
  private CounterSnapshot refreshCounterSnapshot(String counterId) throws IOException {
    final CounterSnapshot newSnapshot = counterSnapshotFactory.create(counterId);
    inMemSnapshot.put(counterId, newSnapshot);
    return newSnapshot;
  }

  /**
   * Check if a resource counter has capacity to spare. Can be used as a cheaper check before starting an expensive
   * operation, e.g. workflow instance dequeue. Note that even if this method returns true,
   * {@link #updateCounter(StorageTransaction, String, long)} might throw {@link CounterCapacityException}.
   *
   * @throws RuntimeException if the resource does not exist or reading from storage fails.
   * todo Throw checked exceptions for expected failures like resource not existing.
   */
  public boolean counterHasSpareCapacity(String resourceId) throws IOException {
    try {
      final CounterSnapshot counterSnapshot = getCounterSnapshot(resourceId);
      counterSnapshot.pickShardWithSpareCapacity(1);
      return true;
    } catch (CounterCapacityException e) {
      return false;
    }
  }

  /**
   * Must be called within a TransactionCallable. (?)
   *
   * <p>Augments the transaction with operations to persist the given limit in Datastore. So long as
   * there has been no preceding successful updateLimit operation, no limit is applied in
   * updateCounter operations on this counter.
   */
  void updateLimit(StorageTransaction tx, String counterId, long limit) throws IOException {
    tx.updateLimitForCounter(counterId, limit);
  }

  /**
   * Reads the latest limit value from Datastore, for the specified {@param counterId}
   */
  static long getLimit(Storage storage, String counterId) throws IOException {
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
  public void updateCounter(StorageTransaction transaction, String counterId, long delta) throws IOException {
    CounterSnapshot snapshot = getCounterSnapshot(counterId);

    // If delta is negative, try to update shards with excess usage first
    if (delta < 0) {
      Optional<Integer> shardIndex = snapshot.pickShardWithExcessUsage(delta);
      if (shardIndex.isPresent()) {
        updateCounterShard(transaction, counterId, delta, shardIndex.get(),
            snapshot.shardCapacity(shardIndex.get()));
        return;
      }
    }

    int shardIndex = snapshot.pickShardWithSpareCapacity(delta);
    updateCounterShard(transaction, counterId, delta, shardIndex, snapshot.shardCapacity(shardIndex));
  }

  @VisibleForTesting
  void updateCounterShard(StorageTransaction transaction, String counterId, long delta,
                          int shardIndex, long shardCapacity) throws IOException {
    final Optional<Shard> shard = transaction.shard(counterId, shardIndex);

    if (shard.isPresent()) {
      final long newShardValue = shard.get().value() + delta;
      // when decrementing, we only care that the newShardValue is >=0
      // if we bound it up to shardCapacity then we fail to decrement. This is especially important in cases where
      // the limit was significantly decreased and therefore the new shardCapacity will be decreased and
      // the newShardValue could be > shardCapacity even after decrementing.
      // ex. Limit = 20, shards = [7, 7, 6], shardCapacity = [7, 7, 6]
      // ex. newLimit = 2, shards = [7, 7, 6], shardCapacity = [1, 1, 0]
      // then a decrement of shards[0] will try to set it to 6. The new value = 6 > 1 = the shardCapacity.
      // This is therefore a valid scenario and should not fail
      if (delta < 0 && newShardValue >= 0) {
        transaction.store(Shard.create(counterId, shardIndex, (int) newShardValue));
      } else if (delta > 0 && Range.closed(0L, shardCapacity).contains(newShardValue)) {
        // when incrementing we want to make sure that the newShardValue is within [0, shardCapacity]
        transaction.store(Shard.create(counterId, shardIndex, (int) newShardValue));
      } else {
        final String message = String.format(
            "Chosen shard %s-%s has no more capacity: capacity=%d, value=%d, delta=%d, newValue=%d",
            counterId, shardIndex, shardCapacity, shard.get().value(), delta, newShardValue);
        LOG.info(message);
        throw new CounterCapacityException(message);
      }
      final String operation = delta > 0 ? "increment" : "decrement";
      LOG.debug("Updating counter shard ({}): {}-{}: capacity={}, value={}, delta={}, newValue={}",
          operation, counterId, shardIndex, shardCapacity, shard.get().value(), delta, newShardValue);
    } else {
      final String message =
          String.format("Could not find shard %s-%s. Unexpected Datastore corruption or our"
                        + "bug - the code should've called initialize() before reaching this"
                        + "point, and any particular shard should strongly be get()-able"
                        + "thereafter",
              counterId, shardIndex);
      LOG.error(message);
      throw new ShardNotFoundException(message);
    }
  }

  /**
   * Returns the current value of the counter referred to by counterId, a weakly consistent
   * estimate. (May have not truly been the counter value at any point in time. Even a return value
   * larger than the corresponding limit might be possible without error.)
   */
  long getCounter(String counterId) throws IOException {
    return getCounterSnapshot(counterId).getTotalUsage();
  }
}
