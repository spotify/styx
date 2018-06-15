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

import com.google.cloud.datastore.Batch;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.spotify.styx.monitoring.Stats;
import java.util.List;

interface InstrumentedDatastoreBatchWriter
    extends Batch, // XXX: DatastoreBatchWriter is not a public interface, Batch is an almost identical subtype
    InstrumentedDatastoreWriter {

  Stats stats();

  Batch batch();

  @Override
  default Response submit() {
    return batch().submit();
  }

  @Override
  default Datastore getDatastore() {
    return batch().getDatastore();
  }

  @Override
  default void addWithDeferredIdAllocation(FullEntity<?>... entities) {
    for (FullEntity<?> entity : entities) {
      stats().recordDatastoreEntityWrites(entity.getKey().getKind(), 1);
    }
    batch().addWithDeferredIdAllocation(entities);
  }

  @Override
  default void putWithDeferredIdAllocation(FullEntity<?>... entities) {
    for (FullEntity<?> entity : entities) {
      stats().recordDatastoreEntityWrites(entity.getKey().getKind(), 1);
    }
    batch().putWithDeferredIdAllocation(entities);
  }

  @Override
  default boolean isActive() {
    return batch().isActive();
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

  static InstrumentedDatastoreBatchWriter of(Stats stats, Batch batch) {
    return new InstrumentedDatastoreBatchWriter() {
      @Override
      public Stats stats() {
        return stats;
      }

      @Override
      public Batch batch() {
        return batch;
      }

      @Override
      public DatastoreWriter writer() {
        return batch;
      }
    };
  }
}
