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

import com.google.cloud.datastore.DatastoreReader;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.spotify.styx.monitoring.Stats;
import java.util.Iterator;
import java.util.List;

interface InstrumentedDatastoreReader extends DatastoreReader {

  Stats stats();

  DatastoreReader reader();

  @Override
  default Entity get(Key key) {
    stats().recordDatastoreEntityReads(key.getKind(), 1);
    return reader().get(key);
  }

  @Override
  default Iterator<Entity> get(Key... keys) {
    for (Key key : keys) {
      stats().recordDatastoreEntityReads(key.getKind(), 1);
    }
    return reader().get(keys);
  }

  @Override
  default List<Entity> fetch(Key... keys) {
    for (Key key : keys) {
      stats().recordDatastoreEntityReads(key.getKind(), 1);
    }
    return reader().fetch(keys);
  }

  @Override
  default <T> QueryResults<T> run(Query<T> query) {
    final QueryResults<T> results = reader().run(query);
    return InstrumentedQueryResults.of(stats(), query, results);
  }
}
