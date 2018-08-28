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

import static com.spotify.styx.util.ShardedCounter.NUM_SHARDS;

import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory to create a Snapshot with initialized shards
 */
public class ShardedCounterSnapshotFactory implements CounterSnapshotFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ShardedCounterSnapshotFactory.class);

  /**
   * Maximum number of entity groups that can be accessed in a transaction in Datastore.
   */
  static final int TRANSACTION_GROUP_SIZE = 25;
  private final Storage storage;

  public ShardedCounterSnapshotFactory(Storage storage) {
    this.storage = Objects.requireNonNull(storage);
  }

  public ShardedCounter.Snapshot create(String counterId) {
    final Map<Integer, AtomicLong> shards = getShards(storage, counterId).entrySet().stream()
        .collect(Collectors.toMap(Entry::getKey, e -> new AtomicLong(e.getValue())));
    return new ShardedCounter.Snapshot(counterId, ShardedCounter.getLimit(storage, counterId), shards);
  }

  private static Map<Integer, Long> getShards(Storage storage, String counterId) {
    Map<Integer, Long> fetchedShards = storage.shardsForCounter(counterId);
    if (fetchedShards.size() < NUM_SHARDS) {
      // The counter probably has not been initialized (so we have empty QueryResults). Also
      // possible that a prior initialize() crashed halfway, or we got a partial list of shards in
      // QueryResults due to eventual consistency. In any case, repeated initialization eventually
      // creates all NUM_SHARDS shards.
      initialize(storage, counterId);
      fetchedShards = storage.shardsForCounter(counterId);
    }
    return fetchedShards;
  }

  /**
   * Idempotent initialization, so that we don't reset an existing shard to zero - counterId may
   * have already been initialized and incremented by another process.
   */
  private static void initialize(Storage storage, String counterId) {
    LOG.debug("Initializing counter shards for resource {}", counterId);
    for (int startIndex = 0; startIndex < NUM_SHARDS; startIndex += TRANSACTION_GROUP_SIZE) {
      initShardRange(storage, counterId, startIndex,
                     Math.min(NUM_SHARDS, startIndex + TRANSACTION_GROUP_SIZE));
    }
  }

  private static void initShardRange(Storage storage, String counterId, int startIndex,
                                     int endIndex) {
    try {
      storage.runInTransaction(tx -> {
        IntStream.range(startIndex, endIndex).forEach(index -> {
          final Optional<Shard> shard = tx.shard(counterId, index);
          if (!shard.isPresent()) {
            tx.store(Shard.create(counterId, index, 0));
          }
        });
        return null;
      });
    } catch (IOException e) {
      LOG.warn("Error when trying to create a group of shards in Datastore: ", e);
    }
  }
}
