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
import com.google.cloud.datastore.DatastoreBatchWriter;
import com.google.cloud.datastore.DatastoreWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.spotify.styx.monitoring.Stats;
import java.util.List;
import java.util.Objects;

interface InstrumentedBatch extends Batch, InstrumentedDatastoreBatchWriter {

  Batch batch();

  @Override
  default Entity add(FullEntity<?> entity) {
    return batch().add(entity);
  }

  @Override
  default List<Entity> add(FullEntity<?>... entities) {
    return batch().add(entities);
  }

  @Override
  default Response submit() {
    return batch().submit();
  }

  @Override
  default Datastore getDatastore() {
    return batch().getDatastore();
  }

  @Override
  default DatastoreBatchWriter batchWriter() {
    return batch();
  }

  static InstrumentedBatch of(Stats stats, Batch batch) {
    Objects.requireNonNull(stats, "stats");
    Objects.requireNonNull(batch, "batch");
    return new InstrumentedBatch() {
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
