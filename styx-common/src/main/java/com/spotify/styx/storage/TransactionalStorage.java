/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import java.io.IOException;

/**
 * The interface to the persistence layer where the same transaction can be used across storage
 * operations.
 *
 * <p>Use the {@link Storage#runInTransaction(TransactionFunction)} method for automatic
 * commit/rollback handling.
 *
 * <p>For manual transaction handling, create a new {@link TransactionalStorage} using
 * {@link Storage#newTransaction()} and call {@link TransactionalStorage#commit()} after the desired storage
 * operation calls.
 */
public interface TransactionalStorage {

  /**
   * Stores a Workflow definition.
   *
   * @param workflow the workflow to store
   */
  WorkflowId store(Workflow workflow) throws IOException;

  /**
   * Commit all the storage operations previously called.
   *
   * @throws TransactionException if the commit fails.
   */
  void commit() throws TransactionException;

  /**
   * Roll back the transaction.
   *
   * @throws TransactionException if rollback fails.
   */
  void rollback() throws TransactionException;

  /**
   * Check if this transaction is still active (not yet committed or rolled back).
   */
  boolean isActive();
}
