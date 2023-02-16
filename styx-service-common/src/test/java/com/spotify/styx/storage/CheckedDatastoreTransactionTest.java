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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Transaction;
import com.google.cloud.datastore.Transaction.Response;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class CheckedDatastoreTransactionTest {

  private static final DatastoreException CAUSE = new DatastoreException(17, "foo", "bar");

  @Rule public ExpectedException exception = ExpectedException.none();

  @Mock private CheckedDatastore datastore;
  @Mock private Transaction transaction;
  @Mock private Response response;

  private CheckedDatastoreTransaction sut;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    sut = new CheckedDatastoreTransaction(datastore, transaction);
  }

  @Test
  public void commit() throws DatastoreIOException {
    when(transaction.commit()).thenReturn(response);
    assertThat(sut.commit(), is(response));
    verify(transaction).commit();
  }

  @Test
  public void rollback() throws DatastoreIOException {
    sut.rollback();
    verify(transaction).rollback();
  }

  @Test
  public void commitShouldThrowCheckedException() throws DatastoreIOException {
    doThrow(CAUSE).when(transaction).commit();
    exception.expect(DatastoreIOException.class);
    exception.expectCause(is(CAUSE));
    sut.commit();
  }

  @Test
  public void rollbackShouldThrowCheckedException() throws DatastoreIOException {
    doThrow(CAUSE).when(transaction).rollback();
    exception.expect(DatastoreIOException.class);
    exception.expectCause(is(CAUSE));
    sut.rollback();
  }

  @Test
  @Parameters({"true", "false"})
  public void isActive(boolean active) {
    when(transaction.isActive()).thenReturn(active);
    assertThat(sut.isActive(), is(active));
    verify(transaction).isActive();
  }

  @Test
  public void getDatastore() {
    assertThat(sut.getDatastore(), is(datastore));
  }
}
