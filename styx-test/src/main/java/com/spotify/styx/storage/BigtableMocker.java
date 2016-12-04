/*-
 * -\-\-
 * Spotify Styx Testing Utilities
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

import static java.util.stream.Collectors.toList;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigtable.hbase.adapters.read.RowCell;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Mocking utility for a bigtable {@link Connection}
 */
public class BigtableMocker {

  private final Connection bigtable;

  private int numFailures = 0; //Default does not throw exceptions

  private final Map<TableName, List<Cell>> tableCells = Maps.newHashMap();

  public BigtableMocker(Connection bigtable) {
    this.bigtable = bigtable;
  }

  public void addRowsToTable(TableName tableName, List<Cell> moreCells) throws IOException {
    tableCells.merge(
        tableName,
        moreCells,
        (oldValue, value) -> {
          oldValue.addAll(moreCells);
          return oldValue;
        });

    finalizeMocking();
  }

  public void removeRowsFromTable(TableName tableName, List<Cell> removeCells) throws IOException {
    tableCells.computeIfPresent(tableName, (key, value) -> {
      List<Cell> newCells = Lists.newArrayList();
      value.removeAll(removeCells);
      return newCells;
    });

    finalizeMocking();
  }


  public BigtableMocker setupTable(TableName tableName) {
    tableCells.put(tableName, Lists.newArrayList());
    return this;
  }

  public void finalizeMocking() throws IOException {
    for (Map.Entry<TableName, List<Cell>> tableEntry : tableCells.entrySet()) {
      TableName tableName = tableEntry.getKey();
      List<Cell> cells = tableEntry.getValue();

      Table table = mock(Table.class);
      when(bigtable.getTable(tableName)).thenReturn(table);

      when(table.get(any(Get.class)))
          .thenAnswer(invocation -> resultOfGet(cells, invocation.getArgumentAt(0, Get.class)));

      when(table.getScanner(any(byte[].class), any(byte[].class)))
          .thenAnswer(invocation -> resultOfFullScan(
              cells,
              invocation.getArgumentAt(0, byte[].class),
              invocation.getArgumentAt(1, byte[].class)));
      when(table.getScanner(any(Scan.class)))
          .thenAnswer(invocation -> resultOfScan(cells, invocation.getArgumentAt(0, Scan.class)));
      doAnswer(invocation -> {
        if (numFailures > 0) {
          numFailures--;
          throw new IOException("Something went wrong in performing put operation");
        }
        Put put = invocation.getArgumentAt(0, Put.class);
        List<Cell> list = Lists.newArrayList();

        put.getFamilyCellMap()
            .values()
            .forEach((list2) -> list2.forEach((kv) -> {
              Cell cell = getCell(kv);
              list.add(cell);
            }));
        addRowsToTable(tableName, list);
        return null;
      }).when(table).put(any(Put.class));
      doAnswer(invocation -> {
        if (numFailures > 0) {
          numFailures--;
          throw new IOException("Something went wrong in performing delete operation");
        }
        Delete delete = invocation.getArgumentAt(0, Delete.class);
        List<Cell> list = Lists.newArrayList();

        delete.getFamilyCellMap()
            .values()
            .forEach((list2) -> list2.forEach((kv) -> {
              Cell cell = getCell(kv);
              list.add(cell);
            }));
        removeRowsFromTable(tableName, list);
        return null;
      }).when(table).delete(any(Delete.class));
    }
  }

  private Cell getCell(Cell kv) {
    final byte[] rowArray;
    final byte[] familyArray;
    final byte[] qualifierArray;
    final long timestamp;
    final byte[] valueArray;

    rowArray = Arrays.copyOfRange(
        kv.getRowArray(),
        kv.getRowOffset(),
        kv.getRowOffset() + kv.getRowLength());
    familyArray = Arrays.copyOfRange(
        kv.getFamilyArray(),
        kv.getFamilyOffset(),
        kv.getFamilyOffset() + kv.getFamilyLength());
    qualifierArray = Arrays.copyOfRange(
        kv.getQualifierArray(),
        kv.getQualifierOffset(),
        kv.getQualifierOffset() + kv.getQualifierLength());
    timestamp = kv.getTimestamp();
    valueArray = Arrays.copyOfRange(
        kv.getValueArray(),
        kv.getValueOffset(),
        kv.getValueOffset() + kv.getValueLength());
    return new RowCell(rowArray, familyArray, qualifierArray, timestamp, valueArray);
  }

  private Result resultOfGet(List<Cell> cells, Get get) {
    final byte[] row = get.getRow();

    return cells.stream()
        .filter(cell -> Bytes.equals(cell.getRowArray(), row))
        .findFirst()
        .map(cell -> Result.create(new Cell[] {cell}))
        .orElseGet(() -> Result.create(Collections.emptyList()));
  }

  private ResultScanner resultOfScan(List<Cell> cells, Scan scan) throws IOException {
    byte[] startRow = scan.getStartRow();
    byte[] stopRow = scan.getStopRow();

    List<Result> inRangeResults = cells.stream().filter(
        cell -> Bytes.compareTo(startRow, cell.getRowArray()) <= 0
                && Bytes.compareTo(stopRow, cell.getRowArray()) > 0)
        .map(cell -> Result.create(new Cell[] {cell}))
        .collect(toList());

    ResultScanner resultScanner = mock(ResultScanner.class);
    when(resultScanner.iterator()).thenReturn(inRangeResults.iterator());

    if (!inRangeResults.isEmpty()) {
      Result first = inRangeResults.get(0);
      Result[] rest = inRangeResults.subList(1, inRangeResults.size())
          .toArray(new Result[inRangeResults.size()]);
      rest[rest.length - 1] = null; // signal end of scanner
      when(resultScanner.next()).thenReturn(first, rest);
    }

    return resultScanner;
  }

  private ResultScanner resultOfFullScan(List<Cell> cells, byte[] family, byte[] qualifier) {
    List<Result> inRangeResults = cells.stream().filter(
        cell -> Bytes.equals(family, cell.getFamilyArray())
                && Bytes.equals(qualifier, cell.getQualifierArray()))
        .map(cell -> Result.create(new Cell[] {cell}))
        .collect(toList());

    ResultScanner resultScanner = mock(ResultScanner.class);
    when(resultScanner.iterator()).thenReturn(inRangeResults.iterator());

    return resultScanner;
  }

  public BigtableMocker setNumFailures(int numFailures) {
    this.numFailures = numFailures;

    return this;
  }
}
