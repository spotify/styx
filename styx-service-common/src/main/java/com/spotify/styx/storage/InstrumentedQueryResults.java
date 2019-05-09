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

import com.google.cloud.datastore.Cursor;
import com.google.cloud.datastore.GqlQuery;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.ProjectionEntityQuery;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery;
import com.google.datastore.v1.QueryResultBatch;
import com.spotify.styx.monitoring.Stats;
import java.util.Objects;

class InstrumentedQueryResults<T> implements QueryResults<T> {

  private final Stats stats;
  private final String kind;
  private final QueryResults<T> results;

  private InstrumentedQueryResults(Stats stats, String kind, QueryResults<T> results) {
    this.stats = Objects.requireNonNull(stats, "stats");
    this.kind = Objects.requireNonNull(kind, "kind");
    this.results = Objects.requireNonNull(results, "results");
  }

  static <T> QueryResults<T> of(Stats stats, Query<T> query, QueryResults<T> results) {
    final String kind = queryKind(query);
    stats.recordDatastoreQueries(kind, 1);
    if (query instanceof KeyQuery) {
      // Key queries do not count as entity reads
      return results;
    }
    if (query instanceof ProjectionEntityQuery
        && ((ProjectionEntityQuery) query).getDistinctOn().isEmpty()) {
      // Projection queries with no distinct on clause do not count as entity reads
      return results;
    }
    return new InstrumentedQueryResults<>(stats, kind, results);

  }

  @Override
  public Class<?> getResultClass() {
    return results.getResultClass();
  }

  @Override
  public Cursor getCursorAfter() {
    return results.getCursorAfter();
  }

  @Override
  public int getSkippedResults() {
    return results.getSkippedResults();
  }

  @Override
  public QueryResultBatch.MoreResultsType getMoreResults() {
    return results.getMoreResults();
  }

  @Override
  public boolean hasNext() {
    return results.hasNext();
  }

  @Override
  public T next() {
    final T value = results.next();
    stats.recordDatastoreEntityReads(kind, 1);
    return value;
  }

  private static <T> String queryKind(Query<T> query) {
    final String kind;
    if (query instanceof StructuredQuery) {
      kind = ((StructuredQuery<T>) query).getKind();
    } else if (query instanceof GqlQuery) {
      kind = "<gql>";
    } else {
      kind = "<unknown>";
    }
    return kind;
  }

}
