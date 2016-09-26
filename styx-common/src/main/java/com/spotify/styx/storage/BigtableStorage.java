/*
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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.spotify.styx.model.Event;
import com.spotify.styx.model.EventSerializer;
import com.spotify.styx.model.ExecutionStatus;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowExecutionInfo;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowInstanceExecutionData;
import com.spotify.styx.util.ResourceNotFoundException;
import com.spotify.styx.util.RunnableWithException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import okio.ByteString;

/**
 * A backend for {@link AggregateStorage} backed by Google Bigtable
 */
public class BigtableStorage {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableStorage.class);

  // todo: remove when not used from API
  public static final TableName EXECUTION_INFO_TABLE_NAME = TableName.valueOf("execution_info");

  public static final TableName EVENTS_TABLE_NAME = TableName.valueOf("styx_events");
  public static final TableName ACTIVE_STATES_TABLE_NAME = TableName.valueOf("active_states");

  public static final byte[] INFO_CF = Bytes.toBytes("info");
  public static final byte[] STATUS_QUALIFIER = Bytes.toBytes("status");
  public static final byte[] PODNAME_QUALIFIER = Bytes.toBytes("pod_name"); // for backwards compatibility
  public static final byte[] EXECUTION_ID_QUALIFIER = Bytes.toBytes("execution_id");
  public static final byte[] STATE_CF = Bytes.toBytes("state");
  public static final byte[] STATE_QUALIFIER = Bytes.toBytes("state");
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

  Map<WorkflowInstance, Long> readActiveWorkflowInstances() throws IOException {
    final Table activeStatesTable = connection.getTable(ACTIVE_STATES_TABLE_NAME);
    final ImmutableMap.Builder<WorkflowInstance, Long> map = ImmutableMap.builder();

    for (Result result : activeStatesTable.getScanner(STATE_CF, STATE_QUALIFIER)) {
      final WorkflowInstance workflowInstance =
          WorkflowInstance.parseKey(Bytes.toString(result.getRow()));
      final long counter = Long.parseLong(Bytes.toString(result.value()));

      map.put(workflowInstance, counter);
    }

    return map.build();
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


  void writeActiveState(WorkflowInstance workflowInstance, long counter) throws IOException {
    storeWithRetries(() -> {
      final Table activeStatesTable = connection.getTable(ACTIVE_STATES_TABLE_NAME);

      final byte[] counterValue = Bytes.toBytes(Long.toString(counter));
      final byte[] key = Bytes.toBytes(workflowInstance.toKey());
      final Put put = new Put(key)
          .addColumn(STATE_CF, STATE_QUALIFIER, counterValue);

      activeStatesTable.put(put);
    });
  }

  void deleteActiveState(WorkflowInstance workflowInstance) throws IOException {
    storeWithRetries(() -> {
      final Table activeStatesTable = connection.getTable(ACTIVE_STATES_TABLE_NAME);
      final byte[] key = Bytes.toBytes(workflowInstance.toKey());
      final Delete delete = new Delete(key);

      activeStatesTable.delete(delete);
    });
  }

  List<WorkflowInstanceExecutionData> executionData(WorkflowId workflowId)
      throws IOException {
    final Table eventsTable = connection.getTable(EVENTS_TABLE_NAME);

    final Scan scan = new Scan()
        .setRowPrefixFilter(Bytes.toBytes(workflowId.toKey() + '#'))
        .setFilter(new FirstKeyOnlyFilter());

    final Set<WorkflowInstance> workflowInstancesSet = Sets.newHashSet();
    for (Result result : eventsTable.getScanner(scan)) {
      final String key = new String(result.getRow());
      final int lastHash = key.lastIndexOf('#');
      final WorkflowInstance wfi = WorkflowInstance.parseKey(key.substring(0, lastHash));
      workflowInstancesSet.add(wfi);
    }

    final List<WorkflowInstanceExecutionData> workflowInstanceDataList = Lists.newArrayList();
    for (WorkflowInstance workflowInstance : workflowInstancesSet) {
      workflowInstanceDataList.add(executionData(workflowInstance));
    }
    workflowInstanceDataList.sort(WorkflowInstanceExecutionData.COMPARATOR);

    return workflowInstanceDataList;
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

  void store(WorkflowExecutionInfo workflowExecutionInfo) throws IOException {
    final Table execInfo = connection.getTable(EXECUTION_INFO_TABLE_NAME);

    final byte[] key = Bytes.toBytes(workflowExecutionInfo.toKey());
    final Put put = new Put(key);
    put.addColumn(INFO_CF, STATUS_QUALIFIER, Bytes.toBytes(workflowExecutionInfo.executionStatus().toString()));
    if (workflowExecutionInfo.executionId().isPresent()) {
      put.addColumn(INFO_CF, EXECUTION_ID_QUALIFIER, Bytes.toBytes(workflowExecutionInfo.executionId().get()));
    }
    execInfo.put(put);
  }

  Map<WorkflowInstance, List<WorkflowExecutionInfo>> getExecutionInfo(WorkflowId workflowId) throws
                                                                                          IOException {
    final Table execInfo = connection.getTable(EXECUTION_INFO_TABLE_NAME);

    final Scan scan = new Scan()
        .setRowPrefixFilter(Bytes.toBytes(workflowId.toKey() + '#'));

    final Map<WorkflowInstance, List<WorkflowExecutionInfo>> map = new HashMap<>();
    for (Result r : execInfo.getScanner(scan)) {
      final WorkflowExecutionInfo workflowExecutionInfo = parseExecutionInfoResult(r);
      final WorkflowInstance workflowInstance = workflowExecutionInfo.workflowInstance();

      map.computeIfAbsent(workflowInstance, (ignore) -> Lists.newArrayList())
          .add(workflowExecutionInfo);
    }

    map.forEach((key, list) -> list.sort(WorkflowExecutionInfo.WHEN_COMPARATOR));

    return map;
  }

  List<WorkflowExecutionInfo> getExecutionInfo(WorkflowInstance workflowInstance)
      throws IOException {
    final Table execInfo = connection.getTable(EXECUTION_INFO_TABLE_NAME);

    final Scan scan = new Scan()
        .setRowPrefixFilter(Bytes.toBytes(workflowInstance.toKey() + '#'));

    final List<WorkflowExecutionInfo> executionInfos = Lists.newArrayList();
    for (Result r : execInfo.getScanner(scan)) {
      final WorkflowExecutionInfo workflowExecutionInfo = parseExecutionInfoResult(r);

      executionInfos.add(workflowExecutionInfo);
    }

    executionInfos.sort(WorkflowExecutionInfo.WHEN_COMPARATOR);
    return executionInfos;
  }

  private WorkflowExecutionInfo parseExecutionInfoResult(Result r) throws IOException {
    final String key = new String(r.getRow());
    final byte[] statusValue = r.getValue(INFO_CF, STATUS_QUALIFIER);
    final byte[] executionIdValue = r.getValue(INFO_CF, EXECUTION_ID_QUALIFIER);
    final byte[] podNameValue = r.getValue(INFO_CF, PODNAME_QUALIFIER); // for backwards compatibility
    final String status = statusValue == null ? "" : new String(statusValue);

    final String executionId;
    if (executionIdValue != null) {
      executionId = new String(executionIdValue);
    } else if (podNameValue != null) {
      executionId = new String(podNameValue);
    } else {
      executionId = "";
    }

    final ExecutionStatus executionStatus = ExecutionStatus.valueOf(status);
    final WorkflowExecutionInfo workflowExecutionInfo;
    try {
      workflowExecutionInfo = WorkflowExecutionInfo.parseKey(key, executionStatus, executionId);
    } catch (Throwable t) {
      throw new IOException("Failed to parse execution info key: " + key + ". " + t.getMessage(), t);
    }

    return workflowExecutionInfo;
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
