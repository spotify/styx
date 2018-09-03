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
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
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

  /**
   * Create a new {@link CheckedDatastoreReaderWriter} wrapping a {@link DatastoreReaderWriter}.
   */
  CheckedDatastoreReaderWriter(DatastoreReaderWriter rw) {
    this.rw = Objects.requireNonNull(rw);
  }

  /**
   * @see DatastoreReaderWriter#put(FullEntity)
   * @throws IOException if the underlying client throws {@link DatastoreException}
   */
  Entity put(FullEntity<?> entity) throws IOException {
    return call(() -> rw.put(entity));
  }

  /**
   * @see DatastoreReaderWriter#add(FullEntity)
   * @throws IOException if the underlying client throws {@link DatastoreException}
   */
  Entity add(FullEntity<?> entity) throws IOException {
    return call(() -> rw.add(entity));
  }

  /**
   * @see DatastoreReaderWriter#update(Entity...)
   * @throws IOException if the underlying client throws {@link DatastoreException}
   */
  void update(Entity... entites) throws IOException {
    run(() -> rw.update(entites));
  }

  /**
   * @see DatastoreReaderWriter#get(Key)
   * @throws IOException if the underlying client throws {@link DatastoreException}
   */
  Entity get(Key key) throws IOException {
    return call(() -> rw.get(key));
  }

  /**
   * Only use this method if the results are small enough that gathering them in list is acceptable.
   * Otherwise use {@link #get(Iterable, IOConsumer)}.
   * @see DatastoreReaderWriter#get(Key...)
   * @throws IOException if the underlying client throws {@link DatastoreException}
   */
  List<Entity> get(Iterable<Key> keys) throws IOException {
    return call(() -> ImmutableList.copyOf(rw.get(toArray(keys, Key.class))));
  }

  /**
   * Prefer this method over {@link #get(Iterable)} to avoid gathering large results in a list.
   * @see DatastoreReaderWriter#get(Key...)
   * @throws IOException if the underlying client throws {@link DatastoreException}
   *                     or if {@code f} throws {@link IOException}.
   */
  void get(Iterable<Key> keys, IOConsumer<Entity> f) throws IOException {
    run(() -> rw.get(toArray(keys, Key.class)).forEachRemaining(IOConsumer.unchecked(f)));
  }

  /**
   * Only use this method if the results are small enough that gathering them in list is acceptable.
   * Otherwise use {@link #query(Query, IOConsumer)}.
   * @see DatastoreReaderWriter#run(Query)
   * @throws IOException if the underlying client throws {@link DatastoreException}
   */
  <T> List<T> query(Query<T> query) throws IOException {
    return call(() -> ImmutableList.copyOf(rw.run(query)));
  }

  /**
   * Prefer this method over {@link #query(Query)} to avoid gathering large results in a list.
   * @see DatastoreReaderWriter#run(Query)
   * @throws IOException if the underlying client throws {@link DatastoreException}
   *                     or if {@code f} throws {@link IOException}.
   */
  <T> void query(Query<T> query, IOConsumer<T> f) throws IOException {
    run(() -> rw.run(query).forEachRemaining(IOConsumer.unchecked(f)));
  }

  /**
   * @see DatastoreReaderWriter#delete(Key...)
   * @throws IOException if the underlying client throws {@link DatastoreException}
   */
  void delete(Key... keys) throws IOException {
    run(() -> rw.delete(keys));
  }

  /**
   * Invokes a {@link Supplier} and translates {@link DatastoreException} and {@link RuntimeIOException}
   * to checked exceptions.
   * @throws DatastoreIOException if {@code f} threw {@link DatastoreException}
   * @throws IOException if {@code f} threw {@link RuntimeIOException}
   */
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

  /**
   * Invokes a {@link Runnable} and translates {@link DatastoreException} and {@link RuntimeIOException}
   * to checked exceptions.
   * @throws DatastoreIOException if {@code f} threw {@link DatastoreException}
   * @throws IOException if {@code f} threw {@link RuntimeIOException}
   */
  static void run(Runnable f) throws IOException {
    call(() -> {
      f.run();
      return null;
    });
  }
}
