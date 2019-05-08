/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2018 Spotify AB
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

package com.spotify.styx.storage;

import com.google.cloud.datastore.DatastoreBatchWriter;
import com.google.cloud.datastore.DatastoreWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.spotify.styx.monitoring.Stats;
import java.util.List;
import java.util.Objects;

interface InstrumentedDatastoreBatchWriter extends DatastoreBatchWriter, InstrumentedDatastoreWriter {

  Stats stats();

  DatastoreBatchWriter batchWriter();

  @Override
  default void addWithDeferredIdAllocation(FullEntity<?>... entities) {
    for (FullEntity<?> entity : entities) {
      stats().recordDatastoreEntityWrites(entity.getKey().getKind(), 1);
    }
    batchWriter().addWithDeferredIdAllocation(entities);
  }

  @Override
  default void putWithDeferredIdAllocation(FullEntity<?>... entities) {
    for (FullEntity<?> entity : entities) {
      stats().recordDatastoreEntityWrites(entity.getKey().getKind(), 1);
    }
    batchWriter().putWithDeferredIdAllocation(entities);
  }

  @Override
  default boolean isActive() {
    return batchWriter().isActive();
  }

  @Override
  default Entity add(FullEntity<?> entity) {
    return InstrumentedDatastoreWriter.super.add(entity);
  }

  @Override
  default List<Entity> add(FullEntity<?>... entities) {
    return InstrumentedDatastoreWriter.super.add(entities);
  }

  @Override
  default void update(Entity... entities) {
    InstrumentedDatastoreWriter.super.update(entities);
  }

  @Override
  default void delete(Key... keys) {
    InstrumentedDatastoreWriter.super.delete(keys);
  }

  @Override
  default Entity put(FullEntity<?> entity) {
    return InstrumentedDatastoreWriter.super.put(entity);
  }

  @Override
  default List<Entity> put(FullEntity<?>... entities) {
    return InstrumentedDatastoreWriter.super.put(entities);
  }

  static InstrumentedDatastoreBatchWriter of(Stats stats, DatastoreBatchWriter batchWriter) {
    Objects.requireNonNull(stats, "stats");
    Objects.requireNonNull(batchWriter, "batchWriter");
    return new InstrumentedDatastoreBatchWriter() {
      @Override
      public Stats stats() {
        return stats;
      }

      @Override
      public DatastoreBatchWriter batchWriter() {
        return batchWriter;
      }

      @Override
      public DatastoreWriter writer() {
        return batchWriter;
      }
    };
  }
}
