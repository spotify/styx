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
import static com.spotify.styx.util.CloserUtil.register;
import static java.util.stream.Collectors.toList;

import com.github.rholder.retry.Attempt;
import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.github.rholder.retry.RetryerBuilder;
import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.StopStrategy;
import com.github.rholder.retry.WaitStrategies;
import com.github.rholder.retry.WaitStrategy;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import com.google.common.io.Closer;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.SequenceEvent;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.util.MDCUtil;
import com.spotify.styx.util.ResourceNotFoundException;
import io.grpc.Context;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
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
public class BigtableStorage implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableStorage.class);

  public static final TableName EVENTS_TABLE_NAME = TableName.valueOf("styx_events");

  private static final byte[] EVENT_CF = Bytes.toBytes("event");
  private static final byte[] EVENT_QUALIFIER = Bytes.toBytes("event");

  private static final int REQUEST_CONCURRENCY = 32;

  private static final WaitStrategy DEFAULT_WAIT_STRATEGY = WaitStrategies.fixedWait(1, TimeUnit.SECONDS);
  private static final StopStrategy DEFAULT_RETRY_STOP_STRATEGY = StopStrategies.stopAfterAttempt(100);

  private final Closer closer = Closer.create();

  private final Connection connection;
  private final Retryer<Void> retryer;
  private final Executor executor;

  BigtableStorage(Connection connection) {
    this(connection, new ForkJoinPool(REQUEST_CONCURRENCY), DEFAULT_WAIT_STRATEGY, DEFAULT_RETRY_STOP_STRATEGY);
  }

  @VisibleForTesting
  BigtableStorage(Connection connection,
                  ExecutorService executor,
                  WaitStrategy waitStrategy,
                  StopStrategy retryStopStrategy) {
    this.connection = Objects.requireNonNull(connection, "connection");

    this.executor = MDCUtil.withMDC(Context.currentContextExecutor(
        register(closer, Objects.requireNonNull(executor, "executor"), "bigtable-storage")));

    retryer = RetryerBuilder.<Void>newBuilder()
        .retryIfExceptionOfType(IOException.class)
        .withWaitStrategy(Objects.requireNonNull(waitStrategy, "waitStrategy"))
        .withStopStrategy(Objects.requireNonNull(retryStopStrategy, "retryStopStrategy"))
        .withRetryListener(BigtableStorage::onRequestAttempt)
        .build();
  }

  @Override
  public void close() throws IOException {
    closer.close();
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
    try {
      retryer.call(() -> {
        try (final Table eventsTable = connection.getTable(EVENTS_TABLE_NAME)) {
          final String workflowInstanceKey = sequenceEvent.event().workflowInstance().toKey();
          final String keyString = String.format("%s#%08d", workflowInstanceKey, sequenceEvent.counter());
          final byte[] key = Bytes.toBytes(keyString);
          final Put put = new Put(key, sequenceEvent.timestamp());

          final byte[] eventBytes = serialize(sequenceEvent.event()).toByteArray();
          put.addColumn(EVENT_CF, EVENT_QUALIFIER, eventBytes);
          eventsTable.put(put);
          return null;
        }
      });
    } catch (ExecutionException | RetryException e) {
      var cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      } else {
        throw new RuntimeException(cause);
      }
    }
  }

  List<WorkflowInstanceExecutionData> executionData(WorkflowId workflowId, String offset, int limit)
      throws IOException {
    try (final Table eventsTable = connection.getTable(EVENTS_TABLE_NAME)) {
      final Scan scan = new Scan()
          .setRowPrefixFilter(Bytes.toBytes(workflowId.toKey() + '#'))
          .setFilter(new FirstKeyOnlyFilter());

      if (!Strings.isNullOrEmpty(offset)) {
        final WorkflowInstance offsetInstance = WorkflowInstance.create(workflowId, offset);
        scan.setStartRow(Bytes.toBytes(offsetInstance.toKey() + '#'));
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
      scan.setStartRow(Bytes.toBytes(startRow.toKey() + '#'));

      if (!Strings.isNullOrEmpty(stop)) {
        final WorkflowInstance stopRow = WorkflowInstance.create(workflowId, stop);
        scan.setStopRow(Bytes.toBytes(stopRow.toKey() + '#'));
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
    return lastStoredEvent.map(SequenceEvent::counter);
  }

  WorkflowInstanceExecutionData executionData(WorkflowInstance workflowInstance) throws IOException {
    SortedSet<SequenceEvent> events = readEvents(workflowInstance);
    if (events.isEmpty()) {
      throw new ResourceNotFoundException("Workflow instance not found");
    }

    return new WFIExecutionBuilder().executionInfo(events);
  }

  private List<WorkflowInstanceExecutionData> executionData(
      Set<WorkflowInstance> workflowInstancesSet) {
    return workflowInstancesSet.stream()
        .map(workflowInstance -> asyncIO(() -> {
          try {
            return executionData(workflowInstance);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
        }))
        .collect(toList())
        .stream()
        .map(CompletableFuture::join)
        .sorted(WorkflowInstanceExecutionData.COMPARATOR)
        .collect(toList());
  }

  private SequenceEvent parseEventResult(Result r) throws IOException {
    final String key = new String(r.getRow());
    final long timestamp = r.getColumnLatestCell(EVENT_CF, EVENT_QUALIFIER).getTimestamp();
    final byte[] value = r.getValue(EVENT_CF, EVENT_QUALIFIER);
    final Event event = deserializeEvent(ByteString.of(value));
    return SequenceEvent.parseKey(key, event, timestamp);
  }

  private <T> CompletableFuture<T> asyncIO(IOOperation<T> f) {
    return f.executeAsync(executor);
  }

  private static <T> void onRequestAttempt(Attempt<T> attempt) {
    if (attempt.hasException()) {
      LOG.warn(String.format("Failed to write to Bigtable (attempt #%d)", attempt.getAttemptNumber()),
          attempt.getExceptionCause());
    }
  }

  private static TreeSet<SequenceEvent> newSortedEventSet() {
    return Sets.newTreeSet(SequenceEvent.COUNTER_COMPARATOR);
  }
}
