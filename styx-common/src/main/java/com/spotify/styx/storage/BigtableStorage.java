/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 Spotify AB
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
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventSerializer;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowInstanceExecutionData;
import com.spotify.styx.util.ResourceNotFoundException;
import com.spotify.styx.util.RunnableWithException;
import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import okio.ByteString;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A backend for {@link AggregateStorage} backed by Google Bigtable
 */
public class BigtableStorage {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableStorage.class);

  public static final TableName EVENTS_TABLE_NAME = TableName.valueOf("styx_events");

  public static final byte[] EVENT_CF = Bytes.toBytes("event");
  public static final byte[] EVENT_QUALIFIER = Bytes.toBytes("event");

  public static final int MAX_BIGTABLE_RETRIES = 100;

  private final Connection connection;
  private final Duration retryBaseDelay;

  private final EventSerializer eventSerializer = new EventSerializer();

  BigtableStorage(Connection connection, Duration retryBaseDelay) {
    this.connection = Objects.requireNonNull(connection);
    this.retryBaseDelay = Objects.requireNonNull(retryBaseDelay);
  }

  SortedSet<SequenceEvent> readEvents(WorkflowInstance workflowInstance) throws IOException {
    final Table eventsTable = connection.getTable(EVENTS_TABLE_NAME);

    final Scan scan = new Scan()
        .setRowPrefixFilter(Bytes.toBytes(workflowInstance.toKey() + '#'));

    final SortedSet<SequenceEvent> set = newSortedEventSet();

    for (Result result : eventsTable.getScanner(scan)) {
      set.add(parseEventResult(result));
    }
    return set;
  }

  void writeEvent(SequenceEvent sequenceEvent) throws IOException {
    storeWithRetries(() -> {
      final Table eventsTable = connection.getTable(EVENTS_TABLE_NAME);

      final String workflowInstanceKey = sequenceEvent.event().workflowInstance().toKey();
      final String
          keyString =
          String.format("%s#%08d", workflowInstanceKey, sequenceEvent.counter());
      final byte[] key = Bytes.toBytes(keyString);
      final Put put = new Put(key, sequenceEvent.timestamp());

      final byte[] eventBytes = eventSerializer.convert(sequenceEvent.event()).toByteArray();
      put.addColumn(EVENT_CF, EVENT_QUALIFIER, eventBytes);
      eventsTable.put(put);
    });
  }

  Optional<Long> getLatestStoredCounter(WorkflowInstance workflowInstance) throws IOException {
    final Set<SequenceEvent> storedEvents = readEvents(workflowInstance);
    final Optional<SequenceEvent> lastStoredEvent = storedEvents.stream().reduce((a, b) -> b);
    if (lastStoredEvent.isPresent()) {
      return Optional.of(lastStoredEvent.get().counter());
    } else {
      return Optional.empty();
    }
  }

  WorkflowInstanceExecutionData executionData(WorkflowInstance workflowInstance) throws IOException {
    SortedSet<SequenceEvent> events = readEvents(workflowInstance);
    if (events.isEmpty()) {
      throw new IOException("Workflow instance not found");
    }

    return WorkflowInstanceExecutionData.fromEvents(events);
  }

  private SequenceEvent parseEventResult(Result r) throws IOException {
    final String key = new String(r.getRow());
    final long timestamp = r.getColumnLatestCell(EVENT_CF, EVENT_QUALIFIER).getTimestamp();
    final byte[] value = r.getValue(EVENT_CF, EVENT_QUALIFIER);
    final Event event = eventSerializer.convert(ByteString.of(value));
    return SequenceEvent.parseKey(key, event, timestamp);
  }

  private void storeWithRetries(RunnableWithException<IOException> storingOperation) throws IOException {
    int storeRetries = 0;
    boolean succeeded = false;

    while (storeRetries < MAX_BIGTABLE_RETRIES && !succeeded) {
      try {
        storingOperation.run();
        succeeded = true;
      } catch (ResourceNotFoundException e) {
        throw e;
      } catch (DatastoreException | IOException e) {
        storeRetries++;
        if (storeRetries == MAX_BIGTABLE_RETRIES) {
          throw e;
        }
        LOG.warn(String.format("Failed to read/write from/to Bigtable (attempt #%d)", storeRetries), e);
        try {
          Thread.sleep(retryBaseDelay.toMillis());
        } catch (InterruptedException e1) {
          throw Throwables.propagate(e1);
        }
      }
    }
  }

  private static TreeSet<SequenceEvent> newSortedEventSet() {
    return Sets.newTreeSet(SequenceEvent.COUNTER_COMPARATOR);
  }
}
