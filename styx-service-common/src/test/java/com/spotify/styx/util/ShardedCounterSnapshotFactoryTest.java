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

package com.spotify.styx.util;

import static com.spotify.styx.util.ShardedCounter.NUM_SHARDS;
import static com.spotify.styx.util.ShardedCounterSnapshotFactory.TRANSACTION_GROUP_SIZE;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.cloud.datastore.Datastore;
import com.spotify.styx.DatastoreEmulatorContainer;
import com.spotify.styx.model.Resource;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.Storage;
import java.io.IOException;
import java.time.Duration;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ShardedCounterSnapshotFactoryTest {

  private static final String RESOURCE_ID = "resourceid-1";

  @Mock private Connection connection;

  private Storage storage;
  private Datastore datastore;
  private ShardedCounterSnapshotFactory counterSnapshotFactory;

  // TODO: the datastore emulator behavior wrt conflicts etc differs from the real datastore
  @ClassRule public static final DatastoreEmulatorContainer datastoreEmulator = new DatastoreEmulatorContainer();

  @Before
  public void setUp() throws IOException {
    datastore = datastoreEmulator.datastoreClient();
    storage = Mockito.spy(new AggregateStorage(connection, datastore, Duration.ZERO));
    counterSnapshotFactory = spy(new ShardedCounterSnapshotFactory(storage));
    storage.storeResource(Resource.create(RESOURCE_ID, 10L));
  }

  @After
  public void tearDown() throws IOException {
    datastoreEmulator.reset();
  }

  @Test
  public void testCreate() throws IOException {
    counterSnapshotFactory.create(RESOURCE_ID);
    assertEquals(128, storage.shardsForCounter(RESOURCE_ID).size());
  }

  @Test
  public void testSpeedOfCreate() throws IOException {
    counterSnapshotFactory.create(RESOURCE_ID);
    verify(storage, times(NUM_SHARDS / TRANSACTION_GROUP_SIZE + 1)).runInTransactionWithRetries(any());
    assertEquals(128, storage.shardsForCounter(RESOURCE_ID).size());
  }
}
