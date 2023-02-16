/*-
 * -\-\-
 * Spotify Styx Service Common
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.cloud.datastore.DatastoreException;
import org.junit.Test;

public class TransactionExceptionTest {

  @Test
  public void isConflict() {
    assertThat(transactionException(10, "", "").isConflict(), is(true));
    assertThat(transactionException(1, "", "ABORTED").isConflict(), is(true));
    assertThat(transactionException(1, "", "").isConflict(), is(false));
  }

  @Test
  public void isAlreadyExists() {
    assertThat(transactionException(1, "", "ALREADY_EXISTS").isAlreadyExists(), is(true));
    assertThat(transactionException(1, "entity already exists", "").isAlreadyExists(), is(true));
    assertThat(transactionException(1, "", "").isAlreadyExists(), is(false));
  }

  @Test
  public void isNotFound() {
    assertThat(transactionException(1, "", "NOT_FOUND").isNotFound(), is(true));
    assertThat(transactionException(1, "no entity to update", "").isNotFound(), is(true));
    assertThat(transactionException(1, "", "").isNotFound(), is(false));
  }

  private TransactionException transactionException(int code, String message, String reason) {
    return new TransactionException(new DatastoreException(code, message, reason));
  }
}
