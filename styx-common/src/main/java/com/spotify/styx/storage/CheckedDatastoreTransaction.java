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

import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Transaction;
import com.google.cloud.datastore.Transaction.Response;
import java.util.Objects;

/**
 * A wrapper for {@link Transaction} that translates unchecked {@link DatastoreException}s to checked
 * {@link DatastoreIOException}s.
 */
class CheckedDatastoreTransaction extends CheckedDatastoreReaderWriter {

  private final CheckedDatastore datastore;
  final Transaction tx;

  CheckedDatastoreTransaction(CheckedDatastore datastore, Transaction tx) {
    super(tx);
    this.datastore = Objects.requireNonNull(datastore);
    this.tx = Objects.requireNonNull(tx);
  }

  Response commit() throws DatastoreIOException {
    try {
      return tx.commit();
    } catch (DatastoreException e) {
      throw new DatastoreIOException(e);
    }
  }

  void rollback() throws DatastoreIOException {
    try {
      tx.rollback();
    } catch (DatastoreException e) {
      throw new DatastoreIOException(e);
    }
  }

  boolean isActive() {
    return tx.isActive();
  }

  CheckedDatastore getDatastore() {
    return datastore;
  }
}
