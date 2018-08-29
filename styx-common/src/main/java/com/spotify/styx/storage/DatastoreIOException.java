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
import java.io.IOException;
import java.util.Objects;

/**
 * A checked version of {@link DatastoreException} that is used to force callers to handle errors.
 */
public class DatastoreIOException extends IOException {

  private final DatastoreException cause;

  public DatastoreIOException(DatastoreException cause) {
    super(cause);
    this.cause = Objects.requireNonNull(cause);
  }

  @Override
  public synchronized DatastoreException getCause() {
    return cause;
  }
}
