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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;;
import static org.junit.Assert.fail;
import static org.mockito.Answers.CALLS_REAL_METHODS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.DatastoreReaderWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.QueryResults;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CheckedDatastoreReaderWriterTest {

  private static final DatastoreException CAUSE = new DatastoreException(17, "foo", "bar");

  @Rule public ExpectedException exception = ExpectedException.none();

  @Mock private Supplier<String> supplier;
  @Mock private Runnable runnable;
  @Mock private Iterator<Entity> entityIterator;
  @Mock private IOConsumer<Entity> entityConsumer;
  @Mock private DatastoreReaderWriter rw;
  @Mock(answer = CALLS_REAL_METHODS) private MockQueryResults queryResults;

  private final Key key1 = Key.newBuilder("foo", "bar", 17).build();
  private final Key key2 = Key.newBuilder("foo", "bar", 4711).build();
  private final Entity entity1 = Entity.newBuilder(key1).build();
  private final Entity entity2 = Entity.newBuilder(key2).build();
  private final EntityQuery query = EntityQuery.newEntityQueryBuilder().build();

  private CheckedDatastoreReaderWriter sut;

  @Before
  public void setUp() throws Exception {
    sut = new CheckedDatastoreReaderWriter(rw);
  }

  @Test
  public void putShouldThrowCheckedException() throws IOException {
    when(rw.put(entity1)).thenThrow(CAUSE);
    exception.expect(IOException.class);
    exception.expectCause(is((Throwable) CAUSE));
    sut.put(entity1);
  }

  @Test
  public void put() throws IOException {
    when(rw.put(entity1)).thenReturn(entity1);
    assertThat(sut.put(entity1), is(entity1));
    verify(rw).put(entity1);
  }

  @Test
  public void addShouldThrowCheckedException() throws IOException {
    when(rw.add(entity1)).thenThrow(CAUSE);
    exception.expect(IOException.class);
    exception.expectCause(is((Throwable) CAUSE));
    sut.add(entity1);
  }

  @Test
  public void add() throws IOException {
    when(rw.add(entity1)).thenReturn(entity1);
    assertThat(sut.add(entity1), is(entity1));
    verify(rw).add(entity1);
  }

  @Test
  public void updateShouldThrowCheckedException() throws IOException {
    doThrow(CAUSE).when(rw).update(entity1, entity2);
    exception.expect(IOException.class);
    exception.expectCause(is((Throwable) CAUSE));
    sut.update(entity1, entity2);
  }

  @Test
  public void update() throws IOException {
    sut.update(entity1, entity2);
    verify(rw).update(entity1, entity2);
  }

  @Test
  public void getSingleShouldThrowCheckedException() throws IOException {
    when(rw.get(key1)).thenThrow(CAUSE);
    exception.expect(IOException.class);
    exception.expectCause(is((Throwable) CAUSE));
    sut.get(key1);
  }

  @Test
  public void getSingle() throws IOException {
    when(rw.get(key1)).thenReturn(entity1);
    assertThat(sut.get(key1), is(entity1));
    verify(rw).get(key1);
  }

  @Test
  public void getAndConsumeShouldThrowCheckedException() throws IOException {
    when(rw.get(key1, key2)).thenThrow(CAUSE);
    exception.expect(IOException.class);
    exception.expectCause(is((Throwable) CAUSE));
    sut.get(List.of(key1, key2), entity -> fail());
  }

  @Test
  public void getAndConsume() throws IOException {
    when(rw.get(key1, key2)).thenReturn(Stream.of(entity1, entity2).iterator());
    sut.get(List.of(key1, key2), entityConsumer);
    verify(rw).get(key1, key2);
    verify(entityConsumer).accept(entity1);
    verify(entityConsumer).accept(entity2);
  }

  @Test
  public void getAndConsumeShouldPropagateIOException() throws IOException {
    final IOException cause = new IOException("foo");
    doReturn(Stream.of(entity1, entity2).iterator()).when(rw).get(key1, key2);
    exception.expect(is(cause));
    sut.get(List.of(key1, key2), entity -> { throw cause; });
  }

  @Test
  public void getAndConsumeShouldHandleForEachRemainingThrowing() throws IOException {
    doThrow(CAUSE).when(entityIterator).forEachRemaining(any());
    doReturn(entityIterator).when(rw).get(key1, key2);
    exception.expect(IOException.class);
    exception.expectCause(is((Throwable) CAUSE));
    sut.get(List.of(key1, key2), entity -> fail());
  }

  @Test
  public void getListShouldThrowCheckedException() throws IOException {
    when(rw.get(key1, key2)).thenThrow(CAUSE);
    exception.expect(IOException.class);
    exception.expectCause(is((Throwable) CAUSE));
    sut.get(List.of(key1, key2));
  }

  @Test
  public void getList() throws IOException {
    when(rw.get(key1, key2)).thenReturn(Stream.of(entity1, entity2).iterator());
    assertThat(sut.get(List.of(key1, key2)), is(List.of(entity1, entity2)));
    verify(rw).get(key1, key2);
  }

  @Test
  public void queryShouldThrowCheckedException() throws IOException {
    when(rw.run(query)).thenThrow(CAUSE);
    exception.expect(IOException.class);
    exception.expectCause(is((Throwable) CAUSE));
    sut.query(query);
  }

  @Test
  public void query() throws IOException {
    when(rw.run(query)).thenReturn(queryResults);
    when(queryResults.results()).thenReturn(Stream.of(entity1, entity2).iterator());
    assertThat(sut.query(query), contains(entity1, entity2));
    verify(rw).run(query);
  }

  @Test
  public void queryAndConsumeShouldThrowCheckedException() throws IOException {
    when(rw.run(query)).thenThrow(CAUSE);
    exception.expect(IOException.class);
    exception.expectCause(is((Throwable) CAUSE));
    sut.query(query, entity -> { });
  }

  @Test
  public void queryAndConsume() throws IOException {
    when(rw.run(query)).thenReturn(queryResults);
    when(queryResults.results()).thenReturn(Stream.of(entity1, entity2).iterator());
    sut.query(query, entityConsumer);
    verify(entityConsumer).accept(entity1);
    verify(entityConsumer).accept(entity2);
    verify(rw).run(query);
  }

  @Test
  public void queryAndConsumeShouldPropagateIOException() throws IOException {
    final IOException cause = new IOException("foo");
    when(queryResults.results()).thenReturn(Stream.of(entity1, entity2).iterator());
    doReturn(queryResults).when(rw).run(any(EntityQuery.class));
    exception.expect(is(cause));
    sut.query(query, entity -> { throw cause; });
  }

  @Test
  public void queryAndConsumeShouldHandleForEachRemainingThrowing() throws IOException {
    doThrow(CAUSE).when(queryResults).forEachRemaining(any());
    doReturn(queryResults).when(rw).run(any(EntityQuery.class));
    exception.expect(IOException.class);
    exception.expectCause(is((Throwable) CAUSE));
    sut.query(query, entity -> fail());
  }

  @Test
  public void deleteShouldThrowCheckedException() throws IOException {
    doThrow(CAUSE).when(rw).delete(key1, key2);
    exception.expect(IOException.class);
    exception.expectCause(is((Throwable) CAUSE));
    sut.delete(key1, key2);
  }

  @Test
  public void delete() throws IOException {
    sut.delete(key1, key1);
    verify(rw).delete(key1, key1);
  }

  @Test
  public void callShouldThrowCheckedException() throws IOException {
    exception.expect(IOException.class);
    exception.expectCause(is((Throwable) CAUSE));
    CheckedDatastoreReaderWriter.call(() -> { throw CAUSE; });
  }

  @Test
  public void call() throws IOException {
    when(supplier.get()).thenReturn("foobar");
    assertThat(CheckedDatastoreReaderWriter.call(supplier), is("foobar"));
    verify(supplier).get();
  }

  @Test
  public void runShouldThrowCheckedException() throws IOException {
    exception.expect(IOException.class);
    exception.expectCause(is((Throwable) CAUSE));
    CheckedDatastoreReaderWriter.run(() -> { throw CAUSE; });
  }

  @Test
  public void run() throws IOException {
    CheckedDatastoreReaderWriter.run(runnable);
    verify(runnable).run();
  }

  @Test
  public void callShouldPropagateIOException() throws IOException {
    final IOException cause = new IOException("foobar");
    exception.expect(is(cause));
    CheckedDatastoreReaderWriter.call(() -> { throw new RuntimeIOException(cause); });
  }

  @Test
  public void runShouldPropagateIOException() throws IOException {
    final IOException cause = new IOException("foobar");
    exception.expect(is(cause));
    CheckedDatastoreReaderWriter.run(() -> { throw new RuntimeIOException(cause); });
  }

  private static abstract class MockQueryResults implements QueryResults<Entity> {

    abstract Iterator<Entity> results();

    @Override
    public boolean hasNext() {
      return results().hasNext();
    }

    @Override
    public Entity next() {
      return results().next();
    }
  }
}
