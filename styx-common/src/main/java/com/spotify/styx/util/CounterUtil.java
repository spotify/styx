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

/**
 * Implementation of a resource counter on Datastore. Sharded in a way to support increment,
 * decrement and capping of a counter to a limit, with high parallelism and strong consistency.
 *
 * Note that getCounter is supported specifically without strong consistency.
 */
public class CounterUtil {

  public CounterUtil(Datastore datastore) {
  }

  /**
   * Must be called within a TransactionCallable. (?)
   *
   * The limits are persisted in Datastore. So long as there has been no preceding successful
   * updateLimit operation, no limit is applied in updateCounter operations on this counter.
   */
  public void updateLimit(DatastoreReaderWriter transaction, String counterName, long limit) {
    transaction.put()
  }

  /**
   * Must be called within a TransactionCallable. Augments the transaction with certain operations
   * that strongly consistently increment resp. decrement the counter referred to by counterName,
   * and cause the transaction to fail to commit if the counter's associated limit is exceeded. Also
   * spurious failures are possible.
   *
   * (TODO: what about decrements below zero - make fail?)
   */
  public void updateCounter(DatastoreReaderWriter transaction, String counterName, long delta) {
    // TODO read the eventually consistent state of shards here (bypassing transaction)?
    // Or read them in background outside of this?
  }

  /**
   * Returns the current value of the counter referred to by counterName, a weakly consistent
   * estimate (a value that may have not truly been the counter value at any point in time).
   */
  public long getCounter(Datastore datastore, String counterName) {
    // TODO
  }
}
