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

import static com.spotify.styx.serialization.Json.deserializeEvent;
import static com.spotify.styx.serialization.Json.serialize;

import com.google.cloud.datastore.DatastoreException;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.util.ResourceNotFoundException;
import com.spotify.styx.util.RunnableWithException;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import okio.ByteString;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
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

  BigtableStorage(Connection connection, Duration retryBaseDelay) {
    this.connection = Objects.requireNonNull(connection);
    this.retryBaseDelay = Objects.requireNonNull(retryBaseDelay);
  }

  SortedSet<SequenceEvent> readEvents(WorkflowInstance workflowInstance) throws IOException {
    try (final Table eventsTable = connection.getTable(EVENTS_TABLE_NAME)) {
      final Scan scan = new Scan()
          .setRowPrefixFilter(Bytes.toBytes(workflowInstance.toKey() + '#'));

      final SortedSet<SequenceEvent> set = newSortedEventSet();

      for (Result result : eventsTable.getScanner(scan)) {
        set.add(parseEventResult(result));
      }
      return set;
    }
  }

  void writeEvent(SequenceEvent sequenceEvent) throws IOException {
    storeWithRetries(() -> {
      try (final Table eventsTable = connection.getTable(EVENTS_TABLE_NAME)) {
        final String workflowInstanceKey = sequenceEvent.event().workflowInstance().toKey();
        final String keyString = String.format("%s#%08d", workflowInstanceKey, sequenceEvent.counter());
        final byte[] key = Bytes.toBytes(keyString);
        final Put put = new Put(key, sequenceEvent.timestamp());

        final byte[] eventBytes = serialize(sequenceEvent.event()).toByteArray();
        put.addColumn(EVENT_CF, EVENT_QUALIFIER, eventBytes);
        eventsTable.put(put);
      }
    });
  }

  List<WorkflowInstanceExecutionData> executionData(WorkflowId workflowId, String offset, int limit)
      throws IOException {
    try (final Table eventsTable = connection.getTable(EVENTS_TABLE_NAME)) {
      final Scan scan = new Scan()
          .setRowPrefixFilter(Bytes.toBytes(workflowId.toKey() + '#'))
          .setFilter(new FirstKeyOnlyFilter());

      if (!Strings.isNullOrEmpty(offset)) {
        final WorkflowInstance offsetInstance = WorkflowInstance.create(workflowId, offset);
        scan.withStartRow(Bytes.toBytes(offsetInstance.toKey() + '#'), true);
      }

      final Set<WorkflowInstance> workflowInstancesSet = Sets.newHashSet();
      try (ResultScanner scanner = eventsTable.getScanner(scan)) {
        Result result = scanner.next();
        while (result != null) {
          final String key = new String(result.getRow());
          final int lastHash = key.lastIndexOf('#');

          final WorkflowInstance wfi = WorkflowInstance.parseKey(key.substring(0, lastHash));
          workflowInstancesSet.add(wfi);
          if (workflowInstancesSet.size() == limit) {
            break;
          }

          result = scanner.next();
        }
      }

      return executionData(workflowInstancesSet);
    }
  }

  List<WorkflowInstanceExecutionData> executionData(WorkflowId workflowId, String start, String stop)
      throws IOException {
    try (final Table eventsTable = connection.getTable(EVENTS_TABLE_NAME)) {
      final Scan scan = new Scan()
          .setRowPrefixFilter(Bytes.toBytes(workflowId.toKey() + '#'))
          .setFilter(new FirstKeyOnlyFilter());

      final WorkflowInstance startRow = WorkflowInstance.create(workflowId, start);
      scan.withStartRow(Bytes.toBytes(startRow.toKey() + '#'), true);

      if (!Strings.isNullOrEmpty(stop)) {
        final WorkflowInstance stopRow = WorkflowInstance.create(workflowId, stop);
        scan.withStopRow(Bytes.toBytes(stopRow.toKey() + '#'), true);
      }

      final Set<WorkflowInstance> workflowInstancesSet = Sets.newHashSet();
      try (ResultScanner scanner = eventsTable.getScanner(scan)) {
        Result result = scanner.next();
        while (result != null) {
          final String key = new String(result.getRow());
          final int lastHash = key.lastIndexOf('#');

          final WorkflowInstance wfi = WorkflowInstance.parseKey(key.substring(0, lastHash));
          workflowInstancesSet.add(wfi);

          result = scanner.next();
        }
      }

      return executionData(workflowInstancesSet);
    }
  }

  Optional<Long> getLatestStoredCounter(WorkflowInstance workflowInstance)
      throws IOException {
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

  private List<WorkflowInstanceExecutionData> executionData(
      Set<WorkflowInstance> workflowInstancesSet) {
    return workflowInstancesSet.parallelStream()
        .map(workflowInstance -> {
          try {
            return executionData(workflowInstance);
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        })
        .sorted(WorkflowInstanceExecutionData.COMPARATOR)
        .collect(Collectors.toList());
  }

  private SequenceEvent parseEventResult(Result r) throws IOException {
    final String key = new String(r.getRow());
    final long timestamp = r.getColumnLatestCell(EVENT_CF, EVENT_QUALIFIER).getTimestamp();
    final byte[] value = r.getValue(EVENT_CF, EVENT_QUALIFIER);
    final Event event = deserializeEvent(ByteString.of(value));
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
