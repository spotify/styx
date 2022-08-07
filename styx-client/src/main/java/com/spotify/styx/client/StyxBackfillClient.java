/*-
 * -\-\-
 * styx-client
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

package com.spotify.styx.client;

import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.BackfillsPayload;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillInput;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Interface for Styx client, backfill resources.
 */
public interface StyxBackfillClient extends AutoCloseable {

  /**
   * Create a {@link Backfill}
   *
   * @param componentId componentId id
   * @param workflowId  workflowId id
   * @param start       beginning of backfill range, inclusive
   * @param end         end of backfill range, exclusive
   * @param concurrency maximum number of concurrent active states for the backfill
   * @return The created {@link Backfill}
   */
  @Deprecated
  CompletionStage<Backfill> backfillCreate(String componentId,
                                           String workflowId,
                                           String start,
                                           String end,
                                           int concurrency);


  /**
   * Create a {@link Backfill}
   *
   * @param componentId componentId id
   * @param workflowId  workflowId id
   * @param start       beginning of backfill range, inclusive
   * @param end         end of backfill range, exclusive
   * @param concurrency maximum number of concurrent active states for the backfill
   * @param description descriptive message of backfill purpose
   * @return The created {@link Backfill}
   */
  @Deprecated
  CompletionStage<Backfill> backfillCreate(String componentId,
                                           String workflowId,
                                           String start,
                                           String end,
                                           int concurrency,
                                           String description);

  /**
   * Create a {@link Backfill}
   *
   * @param backfill The backfill configuration.
   * @return The created {@link Backfill}
   */
  CompletionStage<Backfill> backfillCreate(BackfillInput backfill);

  /**
   * Create a {@link Backfill}
   *
   * @param backfill The backfill configuration.
   * @param allowFuture allow backfilling future partitions 
   * @return The created {@link Backfill}
   */
  CompletionStage<Backfill> backfillCreate(BackfillInput backfill, boolean allowFuture);

  /**
   * Edit concurrency value of existing {@link Backfill}
   *
   * @param backfillId  backfill id
   * @param concurrency the updated concurrency value
   * @return The updated {@link Backfill}
   */
  CompletionStage<Backfill> backfillEditConcurrency(String backfillId,
                                                    int concurrency);

  /**
   * Halt an existing {@link Backfill}
   *
   * @param backfillId backfill id
   */
  @Deprecated
  CompletionStage<Void> backfillHalt(String backfillId);

  /**
   * Halt an existing {@link Backfill}
   *
   * @param backfillId backfill id
   * @param graceful when set to true, already triggered instances of the backfill will not be halted
   */
  CompletionStage<Void> backfillHalt(String backfillId, boolean graceful);

  /**
   * Get an existing {@link Backfill}
   *
   * @param backfillId    backfill id
   * @param includeStatus if to include status info for the {@link Backfill}
   * @return The required {@link Backfill}
   */
  CompletionStage<BackfillPayload> backfill(String backfillId, boolean includeStatus);

  /**
   * List of existing {@link Backfill}s
   *
   * @param componentId   componentId id to filter on
   * @param workflowId    componentId id to filter on
   * @param showAll       if to include also inactive {@link Backfill}s
   * @param includeStatus if to include status info for the {@link Backfill}s
   * @return The required list of {@link Backfill}s, according to the applied filters/options
   */
  CompletionStage<BackfillsPayload> backfillList(Optional<String> componentId,
                                                 Optional<String> workflowId,
                                                 boolean showAll,
                                                 boolean includeStatus,
      Optional<Instant> start);

  @Override
  void close();
}
