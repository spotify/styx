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

import com.google.cloud.datastore.DatastoreWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.spotify.styx.monitoring.Stats;
import java.util.List;

interface InstrumentedDatastoreWriter extends DatastoreWriter {

  Stats stats();

  DatastoreWriter writer();

  @Override
  default Entity add(FullEntity<?> entity) {
    stats().recordDatastoreEntityWrites(entity.getKey().getKind(), 1);
    return writer().add(entity);
  }

  @Override
  default List<Entity> add(FullEntity<?>... entities) {
    for (FullEntity<?> entity : entities) {
      stats().recordDatastoreEntityWrites(entity.getKey().getKind(), 1);
    }
    return writer().add(entities);
  }

  @Override
  default void update(Entity... entities) {
    for (Entity entity : entities) {
      stats().recordDatastoreEntityWrites(entity.getKey().getKind(), 1);
    }
    writer().update(entities);
  }

  @Override
  default Entity put(FullEntity<?> entity) {
    stats().recordDatastoreEntityWrites(entity.getKey().getKind(), 1);
    return writer().put(entity);
  }

  @Override
  default List<Entity> put(FullEntity<?>... entities) {
    for (FullEntity<?> entity : entities) {
      stats().recordDatastoreEntityWrites(entity.getKey().getKind(), 1);
    }
    return writer().put(entities);
  }

  @Override
  default void delete(Key... keys) {
    for (Key key : keys) {
      stats().recordDatastoreEntityDeletes(key.getKind(), 1);
    }
    writer().delete(keys);
  }
}
