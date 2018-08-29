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

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import java.io.IOException;
import java.util.Objects;

/**
 * A wrapper for {@link Datastore} that translates unchecked {@link DatastoreException}s to checked
 * {@link DatastoreIOException}s.
 */
class CheckedDatastore extends CheckedDatastoreReaderWriter {

  private final Datastore datastore;

  CheckedDatastore(Datastore datastore) {
    super(datastore);
    this.datastore = Objects.requireNonNull(datastore);
  }

  KeyFactory newKeyFactory() {
    return datastore.newKeyFactory();
  }

  CheckedDatastoreTransaction newTransaction() throws DatastoreIOException {
    try {
      return new CheckedDatastoreTransaction(this, datastore.newTransaction());
    } catch (DatastoreException e) {
      throw new DatastoreIOException(e);
    }
  }

  Key allocateId(IncompleteKey newKey) throws IOException {
    return call(() -> datastore.allocateId(newKey));
  }
}
