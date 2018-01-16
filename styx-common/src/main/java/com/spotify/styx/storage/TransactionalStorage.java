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

import java.io.IOException;

/**
 * The interface to the persistence layer where the same transaction can be used across storage operations.
 * A new {@link TransactionalStorage} object has to be created for each transaction, and the method
 * {@link TransactionalStorage#commit()} must be called explicitly to commit the storage operations.
 */
public interface TransactionalStorage {

  /**
   * This should commit or rollback all the storage operations previously called.
   *
   * @throws IOException if the commit fails and the rollback is performed
   */
  void commit() throws IOException;
}
