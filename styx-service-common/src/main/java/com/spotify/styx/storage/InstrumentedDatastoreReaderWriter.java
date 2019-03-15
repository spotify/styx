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
import com.google.cloud.datastore.DatastoreReaderWriter;
import com.google.cloud.datastore.DatastoreWriter;
import com.spotify.styx.monitoring.Stats;
import java.util.Objects;

interface InstrumentedDatastoreReaderWriter extends DatastoreReaderWriter, InstrumentedDatastoreWriter,
    InstrumentedDatastoreReader {

  DatastoreReaderWriter readerWriter();

  @Override
  default DatastoreReader reader() {
    return readerWriter();
  }

  @Override
  default DatastoreWriter writer() {
    return readerWriter();
  }

  static InstrumentedDatastoreReaderWriter of(Stats stats, DatastoreReaderWriter readerWriter) {
    Objects.requireNonNull(stats, "stats");
    Objects.requireNonNull(readerWriter, "readerWriter");
    return new InstrumentedDatastoreReaderWriter() {
      @Override
      public DatastoreReaderWriter readerWriter() {
        return readerWriter;
      }

      @Override
      public Stats stats() {
        return stats;
      }
    };
  }
}
