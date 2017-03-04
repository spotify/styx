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

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.DatastoreReader;
import com.google.cloud.datastore.DateTime;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyFactory;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StructuredQuery.PropertyFilter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.util.FnWithException;
import com.spotify.styx.util.ResourceNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A backend for {@link AggregateStorage} backed by Google Datastore
 */
class DatastoreStorage {

  private static final Logger LOG = LoggerFactory.getLogger(DatastoreStorage.class);

  public static final String KIND_STYX_CONFIG = "StyxConfig";
  public static final String KIND_COMPONENT = "Component";
  public static final String KIND_WORKFLOW = "Workflow";
  public static final String KIND_ACTIVE_WORKFLOW_INSTANCE = "ActiveWorkflowInstance";
  public static final String KIND_RESOURCE = "Resource";
  public static final String KIND_BACKFILL = "Backfill";

  public static final String PROPERTY_CONFIG_ENABLED = "enabled";
  public static final String PROPERTY_CONFIG_DOCKER_RUNNER_ID = "dockerRunnerId";
  public static final String PROPERTY_CONFIG_CONCURRENCY = "concurrency";
  public static final String PROPERTY_CONFIG_CLIENT_BLACKLIST = "clientBlacklist";
  public static final String PROPERTY_WORKFLOW_JSON = "json";
  public static final String PROPERTY_WORKFLOW_ENABLED = "enabled";
  public static final String PROPERTY_NEXT_NATURAL_TRIGGER = "nextNaturalTrigger";
  public static final String PROPERTY_DOCKER_IMAGE = "dockerImage";
  public static final String PROPERTY_COUNTER = "counter";
  public static final String PROPERTY_COMPONENT = "component";
  public static final String PROPERTY_WORKFLOW = "workflow";
  public static final String PROPERTY_PARAMETER = "parameter";
  public static final String PROPERTY_COMMIT_SHA = "commitSha";
  public static final String PROPERTY_CONCURRENCY = "concurrency";
  public static final String PROPERTY_START = "start";
  public static final String PROPERTY_END = "end";
  public static final String PROPERTY_NEXT_TRIGGER = "nextTrigger";
  public static final String PROPERTY_PARTITIONING = "partitioning";
  public static final String PROPERTY_ALL_TRIGGERED = "allTriggered";
  public static final String PROPERTY_HALTED = "halted";
  public static final String PROPERTY_DEBUG_ENABLED = "debug";
  public static final String PROPERTY_SUBMISSION_RATE_LIMIT = "submissionRateLimit";

  public static final String KEY_GLOBAL_CONFIG = "styxGlobal";

  public static final boolean DEFAULT_CONFIG_ENABLED = true;
  public static final String DEFAULT_CONFIG_DOCKER_RUNNER_ID = "default";
  public static final boolean DEFAULT_WORKFLOW_ENABLED = false;
  public static final boolean DEFAULT_DEBUG_ENABLED = false;

  public static final int MAX_RETRIES = 100;

  private final Datastore datastore;
  private final Duration retryBaseDelay;
  private final KeyFactory componentKeyFactory;

  @VisibleForTesting
  final Key globalConfigKey;

  DatastoreStorage(Datastore datastore, Duration retryBaseDelay) {
    this.datastore = Objects.requireNonNull(datastore);
    this.retryBaseDelay = Objects.requireNonNull(retryBaseDelay);

    this.componentKeyFactory = datastore.newKeyFactory().kind(KIND_COMPONENT);
    this.globalConfigKey = datastore.newKeyFactory().kind(KIND_STYX_CONFIG).newKey(KEY_GLOBAL_CONFIG);
  }

  private String readConfigString(String property, String defaultValue) {
    return getOpt(datastore, globalConfigKey)
        .filter(w -> w.contains(property))
        .map(config -> config.getString(property))
        .orElse(defaultValue);
  }

  private boolean readConfigBoolean(String property, boolean defaultValue) {
    return getOpt(datastore, globalConfigKey)
        .filter(w -> w.contains(property))
        .map(config -> config.getBoolean(property))
        .orElse(defaultValue);
  }

  boolean globalEnabled() throws IOException {
    return readConfigBoolean(PROPERTY_CONFIG_ENABLED, DEFAULT_CONFIG_ENABLED);
  }

  boolean debugEnabled() throws IOException {
    return readConfigBoolean(PROPERTY_DEBUG_ENABLED, DEFAULT_DEBUG_ENABLED);
  }

  public String globalDockerRunnerId() {
    return readConfigString(PROPERTY_CONFIG_DOCKER_RUNNER_ID, DEFAULT_CONFIG_DOCKER_RUNNER_ID);
  }

  boolean setGlobalEnabled(boolean globalEnabled) throws IOException {
    return storeWithRetries(() -> datastore.runInTransaction(transaction -> {
      final Optional<Entity> configOpt = getOpt(transaction, globalConfigKey);
      final Entity.Builder config = asBuilderOrNew(configOpt, globalConfigKey);
      final boolean oldValue = configOpt
          .filter(w -> w.contains(PROPERTY_CONFIG_ENABLED))
          .map(c -> c.getBoolean(PROPERTY_CONFIG_ENABLED))
          .orElse(DEFAULT_CONFIG_ENABLED);

      config.set(PROPERTY_CONFIG_ENABLED, globalEnabled);
      transaction.put(config.build());

      return oldValue;
    }));
  }

  boolean enabled(WorkflowId workflowId) throws IOException {
    final Key workflowKey = workflowKey(workflowId);

    return getOpt(datastore, workflowKey)
        .filter(w -> w.contains(PROPERTY_WORKFLOW_ENABLED))
        .map(workflow -> workflow.getBoolean(PROPERTY_WORKFLOW_ENABLED))
        .orElse(DEFAULT_WORKFLOW_ENABLED);
  }

  Set<WorkflowId> enabled() throws IOException {
    final EntityQuery queryWorkflows = EntityQuery.entityQueryBuilder().kind(KIND_WORKFLOW).build();
    final QueryResults<Entity> result = datastore.run(queryWorkflows);

    final Set<WorkflowId> enabledWorkflows = Sets.newHashSet();

    while (result.hasNext()) {
      final Entity workflow = result.next();
      final boolean enabled =
          workflow.contains(PROPERTY_WORKFLOW_ENABLED)
          && workflow.getBoolean(PROPERTY_WORKFLOW_ENABLED);

      if (enabled) {
        enabledWorkflows.add(parseWorkflowId(workflow));
      }
    }

    return enabledWorkflows;
  }

  void store(Workflow workflow) throws IOException {
    storeWithRetries(() -> datastore.runInTransaction(transaction -> {
      final String json = Json.OBJECT_MAPPER.writeValueAsString(workflow);
      final Key componentKey = componentKeyFactory.newKey(workflow.componentId());

      final Entity retrievedComponent = transaction.get(componentKey);
      if (retrievedComponent == null) {
        transaction.put(Entity.builder(componentKey).build());
      }

      final Key workflowKey = workflowKey(workflow.id());
      final Optional<Entity> workflowOpt = getOpt(transaction, workflowKey);
      final Entity workflowEntity = asBuilderOrNew(workflowOpt, workflowKey)
          .set(PROPERTY_WORKFLOW_JSON, json)
          .build();

      return transaction.put(workflowEntity);
    }));
  }

  Optional<Workflow> workflow(WorkflowId workflowId) throws IOException {
    final Key workflowKey = workflowKey(workflowId);
    return getOpt(datastore, workflowKey)
        .filter(e -> e.contains(PROPERTY_WORKFLOW_JSON))
        .map(e -> {
          try {
            return Json.OBJECT_MAPPER
                .readValue(e.getString(PROPERTY_WORKFLOW_JSON), Workflow.class);
          } catch (IOException e1) {
            LOG.info("Failed to read workflow for {}, {}", workflowId.componentId(), workflowId.endpointId());
          }
          return null;
        });
  }

  void delete(WorkflowId workflowId) throws IOException {
    storeWithRetries(() -> {
      datastore.delete(workflowKey(workflowId));
      return null;
    });
  }

  public void updateNextNaturalTrigger(WorkflowId workflowId, Instant nextNaturalTrigger) throws IOException {
    storeWithRetries(() -> datastore.runInTransaction(transaction -> {
      final Key workflowKey = workflowKey(workflowId);
      final Optional<Entity> workflowOpt = getOpt(transaction, workflowKey);
      if (!workflowOpt.isPresent()) {
        throw new ResourceNotFoundException(
            String.format("%s:%s doesn't exist.", workflowId.componentId(), workflowId.endpointId()));
      }

      final Entity.Builder builder = Entity
          .builder(workflowOpt.get())
          .set(PROPERTY_NEXT_NATURAL_TRIGGER, instantToDatetime(nextNaturalTrigger));
      return transaction.put(builder.build());
    }));
  }

  public Map<Workflow, Optional<Instant>> workflowsWithNextNaturalTrigger()
      throws IOException {
    Map<Workflow, Optional<Instant>> map = Maps.newHashMap();
    final EntityQuery query =
        Query.entityQueryBuilder().kind(KIND_WORKFLOW).build();
    final QueryResults<Entity> result = datastore.run(query);

    while (result.hasNext()) {
      final Entity entity = result.next();
      Workflow workflow;
      try {
        workflow =
            Json.OBJECT_MAPPER.readValue(entity.getString(PROPERTY_WORKFLOW_JSON), Workflow.class);
      } catch (IOException e) {
        LOG.warn("Failed to read workflow {}.", entity.key());
        continue;
      }
      map.put(workflow,
          entity.contains(PROPERTY_NEXT_NATURAL_TRIGGER)
          ? Optional.of(datetimeToInstant(entity.getDateTime(PROPERTY_NEXT_NATURAL_TRIGGER)))
          : Optional.empty());
    }
    return map;
  }

  Map<WorkflowInstance, Long> allActiveStates() throws IOException {
    final EntityQuery query =
        Query.entityQueryBuilder().kind(KIND_ACTIVE_WORKFLOW_INSTANCE).build();

    return queryActiveStates(query);
  }

  Map<WorkflowInstance, Long> activeStates(String componentId) throws IOException {
    final EntityQuery query =
        Query.entityQueryBuilder().kind(KIND_ACTIVE_WORKFLOW_INSTANCE)
            .filter(PropertyFilter.eq(PROPERTY_COMPONENT, componentId))
            .build();

    return queryActiveStates(query);
  }

  private Map<WorkflowInstance, Long> queryActiveStates(EntityQuery activeStatesQuery) throws IOException {
    final ImmutableMap.Builder<WorkflowInstance, Long> mapBuilder = ImmutableMap.builder();
    final QueryResults<Entity> results = datastore.run(activeStatesQuery);
    while (results.hasNext()) {
      final Entity entity = results.next();
      final long counter = entity.getLong(PROPERTY_COUNTER);
      final WorkflowInstance instance = parseWorkflowInstance(entity);

      mapBuilder.put(instance, counter);
    }

    return mapBuilder.build();
  }

  void writeActiveState(WorkflowInstance workflowInstance, long counter) throws IOException {
    storeWithRetries(() -> {
      final Key key = activeWorkflowInstanceKey(workflowInstance);
      final Entity entity = Entity.builder(key)
          .set(PROPERTY_COMPONENT, workflowInstance.workflowId().componentId())
          .set(PROPERTY_WORKFLOW, workflowInstance.workflowId().endpointId())
          .set(PROPERTY_PARAMETER, workflowInstance.parameter())
          .set(PROPERTY_COUNTER, counter)
          .build();

      return datastore.put(entity);
    });
  }

  void deleteActiveState(WorkflowInstance workflowInstance) throws IOException {
    storeWithRetries(() -> {
      datastore.delete(activeWorkflowInstanceKey(workflowInstance));
      return null;
    });
  }

  void patchState(WorkflowId workflowId, WorkflowState state) throws IOException {
    storeWithRetries(() -> datastore.runInTransaction(transaction -> {
      final Key workflowKey = workflowKey(workflowId);
      final Optional<Entity> workflowOpt = getOpt(transaction, workflowKey);
      if (!workflowOpt.isPresent()) {
        throw new ResourceNotFoundException(
            String.format("%s:%s doesn't exist.", workflowId.componentId(), workflowId.endpointId()));
      }

      final Entity.Builder builder = Entity.builder(workflowOpt.get());
      state.enabled().ifPresent(x -> builder.set(PROPERTY_WORKFLOW_ENABLED, x));
      state.dockerImage().ifPresent(x -> builder.set(PROPERTY_DOCKER_IMAGE, x));
      state.commitSha().ifPresent(x -> builder.set(PROPERTY_COMMIT_SHA, x));
      state.nextNaturalTrigger()
          .ifPresent(x -> builder.set(PROPERTY_NEXT_NATURAL_TRIGGER, instantToDatetime(x)));
      return transaction.put(builder.build());
    }));
  }

  void patchState(String componentId, WorkflowState state) throws IOException {
    storeWithRetries(() -> datastore.runInTransaction(transaction -> {
      final Key componentKey = componentKeyFactory.newKey(componentId);
      final Optional<Entity> componentOpt = getOpt(transaction, componentKey);
      if (!componentOpt.isPresent()) {
        throw new ResourceNotFoundException(String.format("%s doesn't exist.", componentId));
      }

      final Entity.Builder builder = Entity.builder(componentOpt.get());
      state.dockerImage().ifPresent(x -> builder.set(PROPERTY_DOCKER_IMAGE, x));
      state.commitSha().ifPresent(x -> builder.set(PROPERTY_COMMIT_SHA, x));

      return transaction.put(builder.build());
    }));
  }

  Optional<String> getDockerImage(WorkflowId workflowId) throws IOException {
    final Key workflowKey = workflowKey(workflowId);
    Optional<String> dockerImage = getOptStringProperty(datastore, workflowKey, PROPERTY_DOCKER_IMAGE);
    if (dockerImage.isPresent()) {
      return dockerImage;
    }

    final Key componentKey = componentKeyFactory.newKey(workflowId.componentId());
    dockerImage = getOptStringProperty(datastore, componentKey, PROPERTY_DOCKER_IMAGE);
    if (dockerImage.isPresent()) {
      return dockerImage;
    }

    return workflow(workflowId).flatMap(wf -> wf.schedule().dockerImage());
  }

  public WorkflowState workflowState(WorkflowId workflowId) throws IOException {
    WorkflowState.Builder builder = WorkflowState.builder().enabled(enabled(workflowId));
    getDockerImage(workflowId).ifPresent(builder::dockerImage);
    getCommitSha(workflowId).ifPresent(builder::commitSha);
    return builder.build();
  }

  private Optional<String> getCommitSha(WorkflowId workflowId) {
    final Key workflowKey = workflowKey(workflowId);
    Optional<String> commitSha = getOptStringProperty(datastore, workflowKey, PROPERTY_COMMIT_SHA);
    if (commitSha.isPresent()) {
      return commitSha;
    }

    final Key componentKey = componentKeyFactory.newKey(workflowId.componentId());
    return getOptStringProperty(datastore, componentKey, PROPERTY_COMMIT_SHA);
  }

  private <T> T storeWithRetries(FnWithException<T, IOException> storingOperation) throws IOException {
    int storeRetries = 0;

    while (storeRetries < MAX_RETRIES) {
      try {
        return storingOperation.apply();
      } catch (ResourceNotFoundException e) {
        throw e;
      } catch (DatastoreException | IOException e) {
        if (e.getCause() instanceof ResourceNotFoundException) {
          throw (ResourceNotFoundException) e.getCause();
        }

        storeRetries++;
        if (storeRetries == MAX_RETRIES) {
          throw e;
        }

        LOG.warn(String.format("Failed to read/write from/to Datastore (attempt #%d)", storeRetries), e);
        try {
          Thread.sleep(retryBaseDelay.toMillis());
        } catch (InterruptedException e1) {
          throw Throwables.propagate(e1);
        }
      }
    }

    throw new IOException("This should never happen");
  }

  private Key workflowKey(WorkflowId workflowId) {
    return datastore.newKeyFactory()
        .ancestors(PathElement.of(KIND_COMPONENT, workflowId.componentId()))
        .kind(KIND_WORKFLOW)
        .newKey(workflowId.endpointId());
  }

  private Key activeWorkflowInstanceKey(WorkflowInstance workflowInstance) {
    return datastore.newKeyFactory()
        .kind(KIND_ACTIVE_WORKFLOW_INSTANCE)
        .newKey(workflowInstance.toKey());
  }

  private WorkflowInstance parseWorkflowInstance(Entity activeWorkflowInstance) {
    final String componentId = activeWorkflowInstance.getString(PROPERTY_COMPONENT);
    final String workflowId = activeWorkflowInstance.getString(PROPERTY_WORKFLOW);
    final String parameter = activeWorkflowInstance.getString(PROPERTY_PARAMETER);

    return WorkflowInstance.create(WorkflowId.create(componentId, workflowId), parameter);
  }

  private WorkflowId parseWorkflowId(Entity workflow) {
    final String componentId = workflow.key().ancestors().get(0).name();
    final String endpointId = workflow.key().name();

    return WorkflowId.create(componentId, endpointId);
  }

  /**
   * Optionally get an {@link Entity} from a {@link DatastoreReader}.
   *
   * @param datastoreReader  The reader to get from
   * @param key              The key to get
   * @return an optional containing the entity if it existed, empty otherwise.
   */
  private Optional<Entity> getOpt(DatastoreReader datastoreReader, Key key) {
    return Optional.ofNullable(datastoreReader.get(key));
  }

  /**
   * Optionally get a value for an {@link Entity}'s property from a {@link DatastoreReader}.
   *
   * @return an optional containing the property value if it existed, empty otherwise.
   */
  private Optional<String> getOptStringProperty(DatastoreReader datastoreReader, Key key,
                                                String property) {
    return getOpt(datastoreReader, key)
        .filter(e -> e.contains(property))
        .map(e -> e.getString(property));
  }

  /**
   * Convert an optional {@link Entity} into a builder if it exists, otherwise create a new builder.
   *
   * @param entityOpt  The optional entity
   * @param key        The key for which to create a new builder if the entity is not present
   * @return an entity builder either based of the given entity or a new one using the key.
   */
  private Entity.Builder asBuilderOrNew(Optional<Entity> entityOpt, Key key) {
    return entityOpt
        .map(c -> Entity.builder(c))
        .orElse(Entity.builder(key));
  }

  void setEnabled(WorkflowId workflowId1, boolean enabled) throws IOException {
    patchState(workflowId1, WorkflowState.patchEnabled(enabled));
  }

  private static DateTime instantToDatetime(Instant instant) {
    return DateTime.copyFrom(Date.from(instant));
  }

  private static Instant datetimeToInstant(DateTime dateTime) {
    return dateTime.toDate().toInstant();
  }

  Optional<Resource> getResource(String id) {
    Entity entity = datastore.get(datastore.newKeyFactory().kind(KIND_RESOURCE).newKey(id));
    if (entity == null) {
      return Optional.empty();
    }
    return Optional.of(entityToResource(entity));
  }

  void postResource(Resource resource) throws IOException {
    storeWithRetries(() -> datastore.put(resourceToEntity(resource)));
  }

  List<Resource> getResources() {
    final EntityQuery query = Query.entityQueryBuilder().kind(KIND_RESOURCE).build();
    final QueryResults<Entity> results = datastore.run(query);
    final ImmutableList.Builder<Resource> resources = ImmutableList.builder();
    while (results.hasNext()) {
      resources.add(entityToResource(results.next()));
    }
    return resources.build();
  }

  private Resource entityToResource(Entity entity) {
    return Resource.create(entity.key().name(), entity.getLong(PROPERTY_CONCURRENCY));
  }

  private Entity resourceToEntity(Resource resource) {
    final Key key = datastore.newKeyFactory().kind(KIND_RESOURCE).newKey(resource.id());
    return Entity.builder(key)
        .set(PROPERTY_CONCURRENCY, resource.concurrency())
        .build();
  }

  void deleteResource(String id) throws IOException {
    final Key key = datastore.newKeyFactory().kind(KIND_RESOURCE).newKey(id);
    storeWithRetries(() -> {
      datastore.delete(key);
      return null;
    });
  }

  Optional<Backfill> getBackfill(String id) {
    Entity entity = datastore.get(datastore.newKeyFactory().kind(KIND_BACKFILL).newKey(id));
    if (entity == null) {
      return Optional.empty();
    }
    return Optional.of(entityToBackfill(entity));
  }

  List<Backfill> getBackfills() {
    final EntityQuery query = Query.entityQueryBuilder().kind(KIND_BACKFILL).build();
    final QueryResults<Entity> results = datastore.run(query);
    final ImmutableList.Builder<Backfill> resources = ImmutableList.builder();
    while (results.hasNext()) {
      resources.add(entityToBackfill(results.next()));
    }
    return resources.build();
  }

  private Backfill entityToBackfill(Entity entity) {
    final WorkflowId workflowId = WorkflowId.create(entity.getString(PROPERTY_COMPONENT),
                                                    entity.getString(PROPERTY_WORKFLOW));

    return Backfill.newBuilder()
        .id(entity.key().name())
        .start(datetimeToInstant(entity.getDateTime(PROPERTY_START)))
        .end(datetimeToInstant(entity.getDateTime(PROPERTY_END)))
        .workflowId(workflowId)
        .concurrency((int) entity.getLong(PROPERTY_CONCURRENCY))
        .nextTrigger(datetimeToInstant(entity.getDateTime(PROPERTY_NEXT_TRIGGER)))
        .partitioning(Partitioning.valueOf(entity.getString(PROPERTY_PARTITIONING)))
        .allTriggered(entity.getBoolean(PROPERTY_ALL_TRIGGERED))
        .halted(entity.getBoolean(PROPERTY_HALTED))
        .build();
  }

  void storeBackfill(Backfill backfill) throws IOException {
    storeWithRetries(() -> datastore.put(backfillToEntity(backfill)));
  }

  private Entity backfillToEntity(Backfill backfill) {
    final Key key = datastore.newKeyFactory().kind(KIND_BACKFILL).newKey(backfill.id());

    Entity.Builder builder = Entity.builder(key)
        .set(PROPERTY_CONCURRENCY, backfill.concurrency())
        .set(PROPERTY_START, instantToDatetime(backfill.start()))
        .set(PROPERTY_END, instantToDatetime(backfill.end()))
        .set(PROPERTY_COMPONENT, backfill.workflowId().componentId())
        .set(PROPERTY_WORKFLOW, backfill.workflowId().endpointId())
        .set(PROPERTY_PARTITIONING, backfill.partitioning().name())
        .set(PROPERTY_NEXT_TRIGGER, instantToDatetime(backfill.nextTrigger()))
        .set(PROPERTY_ALL_TRIGGERED, backfill.allTriggered())
        .set(PROPERTY_HALTED, backfill.halted());

    return builder.build();
  }

  Optional<Long> globalConcurrency() {
    return getOpt(datastore, globalConfigKey)
        .filter(e -> e.contains(PROPERTY_CONFIG_CONCURRENCY))
        .map(e -> e.getLong(PROPERTY_CONFIG_CONCURRENCY));
  }

  Optional<Double> submissionRate() {
    return getOpt(datastore, globalConfigKey)
        .filter(e -> e.contains(PROPERTY_SUBMISSION_RATE_LIMIT))
        .map(e -> e.getDouble(PROPERTY_SUBMISSION_RATE_LIMIT));
  }

  Optional<List<String>> clientBlacklist() {
    return getOpt(datastore, globalConfigKey)
        .filter(e -> e.contains(PROPERTY_CONFIG_CLIENT_BLACKLIST))
        .map(e -> e.getList(PROPERTY_CONFIG_CLIENT_BLACKLIST).stream().map(v -> (String) v.get())
            .collect(Collectors.toList()));
  }
}
