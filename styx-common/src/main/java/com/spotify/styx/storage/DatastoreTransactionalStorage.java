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

package com.spotify.styx.storage;

import static com.spotify.styx.serialization.Json.OBJECT_MAPPER;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_WORKFLOW_JSON;

import com.google.cloud.datastore.DatastoreReaderWriter;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.StringValue;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

class DatastoreTransactionalStorage implements TransactionalStorage {

  private final DatastoreReaderWriter tx;
  private final DatastoreStorage storage;

  DatastoreTransactionalStorage(DatastoreStorage storage,
      DatastoreReaderWriter transaction) {
    this.storage = Objects.requireNonNull(storage);
    this.tx = Objects.requireNonNull(transaction);
  }

  @Override
  public WorkflowId store(Workflow workflow) throws IOException {
    final Key componentKey = storage.getComponentKey((workflow.componentId()));
    if (tx.get(componentKey) == null) {
      tx.put(Entity.newBuilder(componentKey).build());
    }

    final String json = OBJECT_MAPPER.writeValueAsString(workflow);
    final Key workflowKey = storage.workflowKey(workflow.id());
    final Optional<Entity> workflowOpt = storage.getOpt(tx, workflowKey);
    final Entity workflowEntity = storage.asBuilderOrNew(workflowOpt, workflowKey)
        .set(PROPERTY_WORKFLOW_JSON,
            StringValue.newBuilder(json).setExcludeFromIndexes(true).build())
        .build();

    tx.put(workflowEntity);

    return workflow.id();
  }
}
