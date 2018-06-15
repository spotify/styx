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
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

class DatastoreAdapter implements Datastore {

  private final Datastore delegate;

  DatastoreAdapter(Datastore delegate) {
    this.delegate = Objects.requireNonNull(delegate, "delegate");
  }

  @Override
  public Transaction newTransaction(TransactionOptions transactionOptions) {
    return delegate.newTransaction(transactionOptions);
  }

  @Override
  public Transaction newTransaction() {
    return delegate.newTransaction();
  }

  @Override
  public <T> T runInTransaction(TransactionCallable<T> transactionCallable) {
    return delegate.runInTransaction(transactionCallable);
  }

  @Override
  public <T> T runInTransaction(TransactionCallable<T> transactionCallable, TransactionOptions transactionOptions) {
    return delegate.runInTransaction(transactionCallable, transactionOptions);
  }

  @Override
  public Batch newBatch() {
    return delegate.newBatch();
  }

  @Override
  public Key allocateId(IncompleteKey incompleteKey) {
    return delegate.allocateId(incompleteKey);
  }

  @Override
  public List<Key> allocateId(IncompleteKey... incompleteKeys) {
    return delegate.allocateId(incompleteKeys);
  }

  @Override
  public Entity add(FullEntity<?> fullEntity) {
    return delegate.add(fullEntity);
  }

  @Override
  public List<Entity> add(FullEntity<?>... fullEntities) {
    return delegate.add(fullEntities);
  }

  @Override
  public void update(Entity... entities) {
    delegate.update(entities);
  }

  @Override
  public Entity put(FullEntity<?> fullEntity) {
    return delegate.put(fullEntity);
  }

  @Override
  public List<Entity> put(FullEntity<?>... fullEntities) {
    return delegate.put(fullEntities);
  }

  @Override
  public void delete(Key... keys) {
    delegate.delete(keys);
  }

  @Override
  public KeyFactory newKeyFactory() {
    return delegate.newKeyFactory();
  }

  @Override
  public Entity get(Key key, ReadOption... readOptions) {
    return delegate.get(key, readOptions);
  }

  @Override
  public Iterator<Entity> get(Iterable<Key> iterable, ReadOption... readOptions) {
    return delegate.get(iterable, readOptions);
  }

  @Override
  public List<Entity> fetch(Iterable<Key> iterable, ReadOption... readOptions) {
    return delegate.fetch(iterable, readOptions);
  }

  @Override
  public <T> QueryResults<T> run(Query<T> query, ReadOption... readOptions) {
    return delegate.run(query, readOptions);
  }

  @Override
  public DatastoreOptions getOptions() {
    return delegate.getOptions();
  }

  @Override
  public Entity get(Key key) {
    return delegate.get(key);
  }

  @Override
  public Iterator<Entity> get(Key... keys) {
    return delegate.get(keys);
  }

  @Override
  public List<Entity> fetch(Key... keys) {
    return delegate.fetch(keys);
  }

  @Override
  public <T> QueryResults<T> run(Query<T> query) {
    return delegate.run(query);
  }
}
