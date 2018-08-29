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

import static com.google.common.collect.Iterables.toArray;

import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.DatastoreReaderWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * A wrapper for {@link DatastoreReaderWriter} that translates unchecked {@link DatastoreException}s to checked
 * {@link DatastoreIOException}s.
 */
class CheckedDatastoreReaderWriter {

  private final DatastoreReaderWriter rw;

  CheckedDatastoreReaderWriter(DatastoreReaderWriter rw) {
    this.rw = Objects.requireNonNull(rw);
  }

  Entity put(FullEntity<?> entity) throws IOException {
    return call(() -> rw.put(entity));
  }

  Entity add(FullEntity<?> entity) throws IOException {
    return call(() -> rw.add(entity));
  }

  void update(Entity entity) throws IOException {
    run(() -> rw.update(entity));
  }

  Entity get(Key key) throws IOException {
    return call(() -> rw.get(key));
  }

  List<Entity> get(Iterable<Key> keys) throws IOException {
    return call(() -> ImmutableList.copyOf(rw.get(toArray(keys, Key.class))));
  }

  void get(Iterable<Key> keys, IOConsumer<Entity> f) throws IOException {
    run(() -> rw.get(toArray(keys, Key.class)).forEachRemaining(IOConsumer.unchecked(f)));
  }

  List<Entity> query(EntityQuery query) throws IOException {
    return call(() -> ImmutableList.copyOf(rw.run(query)));
  }

  void query(EntityQuery query, IOConsumer<Entity> f) throws IOException {
    run(() -> rw.run(query).forEachRemaining(IOConsumer.unchecked(f)));
  }

  void delete(Key... keys) throws IOException {
    run(() -> rw.delete(keys));
  }

  static <T> T call(Supplier<T> f) throws IOException {
    try {
      return f.get();
    } catch (DatastoreException e) {
      // Wrap unchecked DatastoreException in a checked DatastoreIOException
      throw new DatastoreIOException(e);
    } catch (RuntimeIOException e) {
      // Propagate IOException from lambda
      throw e.getCause();
    }
  }

  static void run(Runnable f) throws IOException {
    try {
      f.run();
    } catch (DatastoreException e) {
      // Wrap unchecked DatastoreException in a checked DatastoreIOException
      throw new DatastoreIOException(e);
    } catch (RuntimeIOException e) {
      // Propagate IOException from lambda
      throw e.getCause();
    }
  }
}
