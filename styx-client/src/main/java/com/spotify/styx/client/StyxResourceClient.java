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

import com.spotify.styx.api.ResourcesPayload;
import com.spotify.styx.model.Resource;
import java.util.concurrent.CompletionStage;

/**
 * Interface for Styx client, Styx resource resources.
 */
interface StyxResourceClient extends AutoCloseable {

  /**
   * Create a {@link Resource}
   *
   * @param resourceId  resource id to use for the {@link Resource}
   * @param concurrency resource value
   * @return The created {@link Resource}
   */
  CompletionStage<Resource> resourceCreate(String resourceId,
                                           int concurrency,
                                           String requestsMemory, Double requestsCpu,
                                           String limitsMemory, Double limitsCpu);

  /**
   * Edit an existing {@link Resource}
   *
   * @param resourceId  resource id
   * @param concurrency the updated resource value
   * @return The updated{@link Resource}
   */
  CompletionStage<Resource> resourceEdit(String resourceId,
                                         int concurrency,
                                         String requestsMemory, Double requestsCpu,
                                         String limitsMemory, Double limitsCpu);

  /**
   * Get an existing {@link Resource}
   *
   * @param resourceId resource id
   * @return The required {@link Resource}
   */
  CompletionStage<Resource> resource(String resourceId);

  /**
   * List of existing {@link Resource}s
   *
   * @return The list of all existing {@link Resource}s
   */
  CompletionStage<ResourcesPayload> resourceList();

  @Override
  void close();
}
