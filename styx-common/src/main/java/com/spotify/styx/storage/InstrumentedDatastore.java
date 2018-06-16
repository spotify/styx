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
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.DatastoreReaderWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.FullEntity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.ReadOption;
import com.google.cloud.datastore.Transaction;
import com.google.datastore.v1.TransactionOptions;
import com.spotify.styx.monitoring.Stats;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Instrumentation for {@link Datastore} that counts operations according to https://cloud.google.com/datastore/pricing.
 */
public class InstrumentedDatastore implements Datastore, InstrumentedDatastoreReaderWriter {

  private final Stats stats;
  private final Datastore delegate;

  private InstrumentedDatastore(Datastore delegate, Stats stats) {
    this.delegate = delegate;
    this.stats = Objects.requireNonNull(stats, "stats");
  }

  @Override
  public Transaction newTransaction(TransactionOptions transactionOptions) {
    return InstrumentedTransaction.of(stats, delegate.newTransaction(transactionOptions));
  }

  @Override
  public Transaction newTransaction() {
    return InstrumentedTransaction.of(stats, delegate.newTransaction());
  }

  @Override
  public <T> T runInTransaction(TransactionCallable<T> transactionCallable) {
    return delegate.runInTransaction(rw ->
        transactionCallable.run(InstrumentedDatastoreReaderWriter.of(stats, rw)));
  }

  @Override
  public <T> T runInTransaction(TransactionCallable<T> transactionCallable, TransactionOptions transactionOptions) {
    return delegate.runInTransaction(rw ->
        transactionCallable.run(InstrumentedDatastoreReaderWriter.of(stats, rw)), transactionOptions);
  }

  @Override
  public Batch newBatch() {
    return InstrumentedBatch.of(stats, delegate.newBatch());
  }

  @Override
  public Entity get(Key key, ReadOption... readOptions) {
    stats.recordDatastoreEntityReads(key.getKind(), 1);
    return delegate.get(key, readOptions);
  }

  @Override
  public Iterator<Entity> get(Iterable<Key> keys, ReadOption... readOptions) {
    keys.forEach(key -> stats.recordDatastoreEntityReads(key.getKind(), 1));
    return delegate.get(keys, readOptions);
  }

  @Override
  public List<Entity> fetch(Iterable<Key> keys, ReadOption... readOptions) {
    keys.forEach(key -> stats.recordDatastoreEntityReads(key.getKind(), 1));
    return delegate.fetch(keys, readOptions);
  }

  @Override
  public <T> QueryResults<T> run(Query<T> query, ReadOption... readOptions) {
    final QueryResults<T> results = delegate.run(query, readOptions);
    return InstrumentedQueryResults.of(stats, query, results);
  }

  @Override
  public Key allocateId(IncompleteKey key) {
    return delegate.allocateId(key);
  }

  @Override
  public List<Key> allocateId(IncompleteKey... keys) {
    return delegate.allocateId(keys);
  }

  @Override
  public Entity add(FullEntity<?> entity) {
    return InstrumentedDatastoreReaderWriter.super.add(entity);
  }

  @Override
  public List<Entity> add(FullEntity<?>... entities) {
    return InstrumentedDatastoreReaderWriter.super.add(entities);
  }

  @Override
  public void update(Entity... entities) {
    InstrumentedDatastoreReaderWriter.super.update(entities);
  }

  @Override
  public Entity put(FullEntity<?> entity) {
    return InstrumentedDatastoreReaderWriter.super.put(entity);
  }

  @Override
  public List<Entity> put(FullEntity<?>... entities) {
    return InstrumentedDatastoreReaderWriter.super.put(entities);
  }

  @Override
  public void delete(Key... keys) {
    InstrumentedDatastoreReaderWriter.super.delete(keys);
  }

  @Override
  public KeyFactory newKeyFactory() {
    return delegate.newKeyFactory();
  }

  @Override
  public DatastoreOptions getOptions() {
    return delegate.getOptions();
  }

  @Override
  public Stats stats() {
    return stats;
  }

  @Override
  public DatastoreReaderWriter readerWriter() {
    return delegate;
  }

  public Datastore delegate() {
    return delegate;
  }

  public static InstrumentedDatastore of(Datastore delegate, Stats stats) {
    return new InstrumentedDatastore(delegate, stats);
  }
}
