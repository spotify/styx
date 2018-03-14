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

import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory to create a CounterSnapshot with initialized shards
 */
public class CounterSnapshotFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CounterSnapshotFactory.class);

  private static final int TRANSACTION_GROUP_SIZE = 15;

  private CounterSnapshotFactory() {
  }
  public static ShardedCounter.CounterSnapshot create(Storage storage, String counterId,
                                                      ExecutorService executorService) {
    return new ShardedCounter.CounterSnapshot(storage, counterId,
                                              getShards(storage, counterId, executorService));
  }

  private static Map<Integer, Long> getShards(Storage storage, String counterId,
                                              ExecutorService executorService) {
    Map<Integer, Long> fetchedShards = storage.shardsForCounter(counterId);
    if (fetchedShards.size() < ShardedCounter.NUM_SHARDS) {
      // The counter probably has not been initialized (so we have empty QueryResults). Also
      // possible that a prior initialize() crashed halfway, or we got a partial list of shards in
      // QueryResults due to eventual consistency. In any case, repeated initialization eventually
      // creates all NUM_SHARDS shards.
      initialize(storage, counterId, executorService);
      fetchedShards = storage.shardsForCounter(counterId);
    }
    return fetchedShards;
  }

  /**
   * Idempotent initialization, so that we don't reset an existing shard to zero - counterId may
   * have already been initialized and incremented by another process.
   */
  private static void initialize(Storage storage, String counterId,
                                 ExecutorService executorService) {

    for (int i = 0; i < ShardedCounter.NUM_SHARDS; i += TRANSACTION_GROUP_SIZE) {
      final int startIndex = i;
      executorService.execute(
          () -> initShardRange(storage, counterId, startIndex,
                               startIndex + TRANSACTION_GROUP_SIZE));
    }
    try {
      executorService.awaitTermination(20, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.warn("Error when initializing shards: ", e);
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
