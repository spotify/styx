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
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.Transaction;
import java.io.IOException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CheckedDatastoreTest {

  private static final DatastoreException CAUSE = new DatastoreException(17, "foo", "bar");

  @Rule public ExpectedException exception = ExpectedException.none();

  @Mock private Datastore datastore;
  @Mock private Transaction transaction;
  @Mock private IncompleteKey incompleteKey;
  @Mock private DatastoreOptions options;

  private final KeyFactory keyFactory = new KeyFactory("foo");
  private final Key key = Key.newBuilder("foo", "bar", 17).build();

  private CheckedDatastore sut;

  @Before
  public void setUp() throws Exception {
    sut = new CheckedDatastore(datastore);
  }

  @Test
  public void newKeyFactory() {
    when(datastore.newKeyFactory()).thenReturn(keyFactory);
    assertThat(sut.newKeyFactory(), is(keyFactory));
    verify(datastore).newKeyFactory();
  }

  @Test
  public void newTransaction() throws DatastoreIOException {
    when(datastore.newTransaction()).thenReturn(transaction);
    assertThat(sut.newTransaction().tx, is(transaction));
    verify(datastore).newTransaction();
  }

  @Test
  public void newTransactionShouldThrowCheckedException() throws DatastoreIOException {
    when(datastore.newTransaction()).thenThrow(CAUSE);
    exception.expect(DatastoreIOException.class);
    exception.expectCause(is(CAUSE));
    sut.newTransaction();
  }

  @Test
  public void allocateId() throws IOException {
    when(datastore.allocateId(incompleteKey)).thenReturn(key);
    assertThat(sut.allocateId(incompleteKey), is(key));
    verify(datastore).allocateId(incompleteKey);
  }

  @Test
  public void allocateIdShouldThrowCheckedException() throws IOException {
    when(datastore.allocateId(incompleteKey)).thenThrow(CAUSE);
    exception.expect(DatastoreIOException.class);
    exception.expectCause(is(CAUSE));
    sut.allocateId(incompleteKey);
  }

  @Test
  public void shouldGetOptions() {
    when(datastore.getOptions()).thenReturn(options);
    assertThat(sut.getOptions(), is(options));
    verify(datastore).getOptions();
  }
}
