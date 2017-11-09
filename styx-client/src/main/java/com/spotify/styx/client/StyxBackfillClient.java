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
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Interface for Styx client, backfill resources.
 */
public interface StyxBackfillClient {

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
  CompletionStage<Backfill> backfillCreate(final String componentId,
                                           final String workflowId,
                                           final String start,
                                           final String end,
                                           final int concurrency);

  /**
   * Edit concurrency value of existing {@link Backfill}
   *
   * @param backfillId  backfill id
   * @param concurrency the updated concurrency value
   * @return The updated {@link Backfill}
   */
  CompletionStage<Backfill> backfillEditConcurrency(final String backfillId,
                                                    final int concurrency);

  /**
   * Halt an existing {@link Backfill}
   *
   * @param backfillId backfill id
   */
  CompletionStage<Void> backfillHalt(final String backfillId);

  /**
   * Get an existing {@link Backfill}
   *
   * @param backfillId    backfill id
   * @param includeStatus if to include status info for the {@link Backfill}
   * @return The required {@link Backfill}
   */
  CompletionStage<BackfillPayload> backfill(final String backfillId, boolean includeStatus);

  /**
   * List of existing {@link Backfill}s
   *
   * @param componentId   componentId id to filter on
   * @param workflowId    componentId id to filter on
   * @param showAll       if to include also inactive {@link Backfill}s
   * @param includeStatus if to include status info for the {@link Backfill}s
   * @return The required list of {@link Backfill}s, according to the applied filters/options
   */
  CompletionStage<BackfillsPayload> backfillList(final Optional<String> componentId,
                                                 final Optional<String> workflowId,
                                                 final boolean showAll,
                                                 final boolean includeStatus);

}
