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

import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresentAndIs;
import static com.spotify.styx.model.Schedule.DAYS;
import static com.spotify.styx.model.Schedule.HOURS;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_ALL_TRIGGERED;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_COMPONENT;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_CONCURRENCY;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_CREATED;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_END;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_HALTED;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_LAST_MODIFIED;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_NEXT_TRIGGER;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_SCHEDULE;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_START;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_WORKFLOW;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_WORKFLOW_JSON;
import static com.spotify.styx.storage.DatastoreStorage.globalConfigKey;
import static com.spotify.styx.storage.DatastoreStorage.instantToTimestamp;
import static com.spotify.styx.storage.DatastoreStorage.workflowKey;
import static com.spotify.styx.testdata.TestData.EXECUTION_DESCRIPTION;
import static com.spotify.styx.testdata.TestData.FLYTE_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_SOURCE;
import static com.spotify.styx.testdata.TestData.FULL_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.ServiceOptions;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StringValue;
import com.google.common.collect.ImmutableSet;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.WorkflowWithState;
import com.spotify.styx.model.BackfillBuilder;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.Message.MessageLevel;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.testdata.TestData;
import com.spotify.styx.util.Shard;
import com.spotify.styx.util.TriggerInstantSpec;
import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.threeten.bp.Duration;

@RunWith(JUnitParamsRunner.class)
public class DatastoreStorageTest {

  private static final RetrySettings RETRY_SETTINGS = ServiceOptions.getDefaultRetrySettings().toBuilder()
      .setInitialRetryDelay(Duration.ofMillis(1L))
      .setTotalTimeout(Duration.ofSeconds(5))
      .setMaxAttempts(3)
      .build();

  private static final WorkflowId WORKFLOW_ID1 = WorkflowId.create("component", "endpoint1");
  private static final WorkflowId WORKFLOW_ID2 = WorkflowId.create("component", "endpoint2");
  private static final WorkflowId WORKFLOW_ID3 = WorkflowId.create("component2", "pointless");

  static final WorkflowInstance WORKFLOW_INSTANCE1 = WorkflowInstance.create(WORKFLOW_ID1, "2016-09-01");
  private static final WorkflowInstance WORKFLOW_INSTANCE2 = WorkflowInstance.create(WORKFLOW_ID2, "2016-09-01");
  private static final WorkflowInstance WORKFLOW_INSTANCE3 = WorkflowInstance.create(WORKFLOW_ID3, "2016-09-01");

  private static final Resource RESOURCE1 = Resource.create("resource1", 1L);
  private static final Resource RESOURCE2 = Resource.create("resource2", 2L);

  private static final Instant currentTime = Instant.parse("2019-01-01T00:00:00Z");

  static final Instant TIMESTAMP = Instant.parse("2017-01-01T00:00:00Z");

  private static final TriggerParameters TRIGGER_PARAMETERS = TriggerParameters.builder()
      .env("FOO", "foo",
          "BAR", "bar")
      .build();

  private static final StateData STATE_DATA = StateData.newBuilder()
      .tries(17)
      .consecutiveFailures(13)
      .retryCost(472893)
      .retryDelayMillis(4711L)
      .lastExit(13)
      .trigger(Trigger.backfill("backfill-4711"))
      .executionId("deadbeef")
      .executionDescription(EXECUTION_DESCRIPTION)
//      .commitSha("8843d7f92416211de9ebb963ff4ce28125932878") // TODO: remove unused commitSha field?
      .resourceIds(ImmutableSet.of("foo", "bar"))
      .triggerParameters(TRIGGER_PARAMETERS)
      .addMessage(Message.create(MessageLevel.INFO, "foo the bar"))
      .build();

  static final RunState RUN_STATE = RunState.create(WORKFLOW_INSTANCE1, State.NEW,
      STATE_DATA, TIMESTAMP, 42L);

  static final RunState RUN_STATE1 = RunState.create(WORKFLOW_INSTANCE1, State.NEW,
      STATE_DATA, TIMESTAMP, 43L);

  private static final RunState RUN_STATE2 = RunState.create(WORKFLOW_INSTANCE2, State.NEW,
      STATE_DATA, TIMESTAMP, 84L);

  private static final RunState RUN_STATE3 = RunState.create(WORKFLOW_INSTANCE3, State.NEW,
      STATE_DATA, TIMESTAMP, 17L);


  static final RunState FULLY_POPULATED_RUNSTATE = RunState.create(WORKFLOW_INSTANCE, State.QUEUED,
      StateData.newBuilder()
          .tries(17)
          .consecutiveFailures(89)
          .retryCost(2.0)
          .retryDelayMillis(4711L)
          .lastExit(13)
          .trigger(Trigger.adhoc("foobar"))
          .executionId("foo-bar-17")
          .runnerId("test")
          .executionDescription(ExecutionDescription.builder()
              .dockerImage("foo/bar:34234")
              .dockerArgs(List.of("foo", "the", "bar", "baz"))
              .dockerTerminationLogging(true)
              .serviceAccount("foo@bar.baz")
              .commitSha("2d2bfa926b94508de5aab47b5f305659ead2274a")
              .env("foo", "bar")
              .runningTimeout(java.time.Duration.ZERO)
              .retryCondition("#tries<2")
              .build())
          .resourceIds(ImmutableSet.of("GLOBAL_STYX_CLUSTER", "foo-resource", "bar-resource"))
          .addMessage(Message.info("foo"))
          .addMessage(Message.warning("bar"))
          .addMessage(Message.error("baz"))
          .addMessage(Message.create(MessageLevel.UNKNOWN, "quux"))
          .build(),
      TIMESTAMP,
      42L);

  private static final WorkflowId WORKFLOW_ID = WorkflowId.create("dockerComp", "dockerEndpoint");

  private static final String DOCKER_IMAGE = "gcr.io/foo/bar";
  private static final WorkflowConfiguration WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id(WORKFLOW_ID.id())
          .schedule(DAYS)
          .dockerImage(DOCKER_IMAGE)
          .build();
  static final Workflow WORKFLOW = Workflow.create(WORKFLOW_ID.componentId(),
      WORKFLOW_CONFIGURATION);

  @ClassRule public static final DatastoreEmulator datastoreEmulator = new DatastoreEmulator();

  private DatastoreStorage storage;
  private CheckedDatastore datastore;
  private Datastore datastoreClient;

  @Mock TransactionFunction<String, FooException> transactionFunction;
  @Mock Function<CheckedDatastoreTransaction, DatastoreStorageTransaction> storageTransactionFactory;
  @Mock Logger logger;

  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    datastoreClient = datastoreEmulator.options().toBuilder()
        .setRetrySettings(RETRY_SETTINGS)
        .build()
        .getService();
    var innerDatastore = new CheckedDatastore(datastoreClient);
    datastore = spy(innerDatastore);
    storage = new DatastoreStorage(datastore, DatastoreStorageTransaction::new, executor, logger);
  }

  @After
  public void tearDown() throws Exception {
    datastoreEmulator.reset();
    storage.close();
  }

  @Test
  @Parameters(method = "configurations")
  public void shouldPersistWorkflows(WorkflowConfiguration configuration) throws Exception {
    Workflow workflow = Workflow.create("test", configuration);
    storage.store(workflow);
    Optional<Workflow> retrieved = storage.workflow(workflow.id());

    assertThat(retrieved, is(Optional.of(workflow)));
  }

  @SuppressWarnings("unused")
  private static WorkflowConfiguration[] configurations() {
    return new WorkflowConfiguration[] {
        FULL_WORKFLOW_CONFIGURATION,
        FLYTE_WORKFLOW_CONFIGURATION,
        FLYTE_WORKFLOW_CONFIGURATION_WITH_DEPLOYMENT_SOURCE
    };
  }

  @Test
  @Parameters(method = "configurations")
  public void shouldDeleteWorkflows(WorkflowConfiguration configuration) throws Exception {
    var foo = Workflow.create("foo", configuration);
    var bar = Workflow.create("bar", configuration);

    storage.store(foo);
    storage.store(bar);

    var fooKey = workflowKey(datastore::newKeyFactory, foo.id());
    var barKey = workflowKey(datastore::newKeyFactory, bar.id());

    assertThat(datastore.get(fooKey), is(notNullValue()));
    assertThat(datastore.get(barKey), is(notNullValue()));

    storage.delete(foo.id());

    assertThat(storage.workflow(foo.id()), is(Optional.empty()));
    assertThat(datastore.get(fooKey), is(nullValue()));
    assertThat(datastore.get(barKey), is(notNullValue()));
  }

  @Test
  public void shouldPersistNextScheduledRun() throws Exception {
    Instant instant = Instant.parse("2016-03-14T14:00:00Z");
    Instant offset = instant.plus(1, ChronoUnit.DAYS);
    TriggerInstantSpec spec = TriggerInstantSpec.create(instant, offset);

    storage.store(WORKFLOW);
    storage.updateNextNaturalTrigger(WORKFLOW.id(), spec);

    final Map<Workflow, TriggerInstantSpec> result = storage.workflowsWithNextNaturalTrigger();
    assertThat(result.values().size(), is(1));
    assertThat(result, hasEntry(WORKFLOW, spec));
  }

  @Test
  public void shouldReturnEmptyOptionalWhenWorkflowIdDoesNotExist() throws Exception {
    Optional<Workflow> retrieved = storage.workflow(WorkflowId.create("foo", "bar"));

    assertThat(retrieved, is(Optional.empty()));
  }

  @Test
  public void shouldReturnEmptyWorkflowStateExceptEnabledWhenWorkflowStateDoesNotExist() throws Exception {
    final Workflow workflow = Workflow.create("foo",
                                              WORKFLOW_CONFIGURATION);
    storage.store(workflow);
    WorkflowState retrieved = storage.workflowState(workflow.id());
    assertThat(retrieved, is(WorkflowState.patchEnabled(false)));
  }

  @Test
  public void shouldStoreWorkflowEnabledFlag() throws Exception {
    storage.store(workflow(WORKFLOW_ID1));

    storage.setEnabled(WORKFLOW_ID1, true);
    assertTrue(storage.enabled(WORKFLOW_ID1));

    storage.setEnabled(WORKFLOW_ID1, false);
    assertFalse(storage.enabled(WORKFLOW_ID1));

    storage.setEnabled(WORKFLOW_ID1, false);
    assertFalse(storage.enabled(WORKFLOW_ID1));
  }

  @Test
  public void shouldStoreWorkflowEnabledFlagsSeparately() throws Exception {
    storage.store(workflow(WORKFLOW_ID1));
    storage.store(workflow(WORKFLOW_ID2));

    storage.setEnabled(WORKFLOW_ID1, true);
    storage.setEnabled(WORKFLOW_ID2, true);
    assertTrue(storage.enabled(WORKFLOW_ID1));
    assertTrue(storage.enabled(WORKFLOW_ID2));

    storage.setEnabled(WORKFLOW_ID1, false);
    assertFalse(storage.enabled(WORKFLOW_ID1));
    assertTrue(storage.enabled(WORKFLOW_ID2));
  }

  @Test
  public void shouldReturnEnabledWorkflows() throws Exception {
    storage.store(workflow(WORKFLOW_ID1));
    storage.store(workflow(WORKFLOW_ID2));
    storage.store(workflow(WORKFLOW_ID3));

    storage.setEnabled(WORKFLOW_ID1, true);
    storage.setEnabled(WORKFLOW_ID2, false);
    assertThat(storage.enabled(), containsInAnyOrder(WORKFLOW_ID1));

    storage.setEnabled(WORKFLOW_ID2, true);
    assertThat(storage.enabled(), containsInAnyOrder(WORKFLOW_ID1, WORKFLOW_ID2));

    storage.setEnabled(WORKFLOW_ID3, true);
    assertThat(storage.enabled(), containsInAnyOrder(WORKFLOW_ID1, WORKFLOW_ID2, WORKFLOW_ID3));

    storage.setEnabled(WORKFLOW_ID1, false);
    assertThat(storage.enabled(), containsInAnyOrder(WORKFLOW_ID2, WORKFLOW_ID3));

    storage.setEnabled(WORKFLOW_ID2, false);
    storage.setEnabled(WORKFLOW_ID3, false);
    assertThat(storage.enabled(), is(empty()));
  }

  @Test
  public void workflowShouldBeDisabledByDefault() throws Exception {
    boolean enabled = storage.enabled(WORKFLOW_ID1);

    assertFalse(enabled);
  }

  @Test
  public void shouldRetainAllWorkflowSettings() throws Exception {
    WorkflowId id = WORKFLOW.id();

    storage.store(WORKFLOW);
    storage.setEnabled(id, true);
    Optional<Workflow> workflow = storage.workflow(id);
    boolean enabled = storage.enabled(id);
    assertThat(workflow, isPresentAndIs(WORKFLOW));
    assertTrue(enabled);

    storage.setEnabled(id, false);
    workflow = storage.workflow(id);
    enabled = storage.enabled(id);
    assertThat(workflow, isPresentAndIs(WORKFLOW));
    assertFalse(enabled);

    storage.store(WORKFLOW);
    workflow = storage.workflow(id);
    enabled = storage.enabled(id);
    assertThat(workflow, isPresentAndIs(WORKFLOW));
    assertFalse(enabled);
  }

  @Test
  public void shouldWriteActiveWorkflowInstanceWithState() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE, RUN_STATE1);

    List<Entity> activeInstances = entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE);
    assertThat(activeInstances, hasSize(1));

    Entity instance = activeInstances.get(0);
    assertThat(instance.getLong(DatastoreStorage.PROPERTY_COUNTER), is(RUN_STATE1.counter()));
    assertThat(instance.getString(DatastoreStorage.PROPERTY_COMPONENT), is(WORKFLOW_INSTANCE.workflowId().componentId()));
    assertThat(instance.getString(DatastoreStorage.PROPERTY_WORKFLOW), is(WORKFLOW_INSTANCE.workflowId().id()));
    assertThat(instance.getString(DatastoreStorage.PROPERTY_PARAMETER), is(WORKFLOW_INSTANCE.parameter()));
  }

  @Test
  public void testFullPersistentStatePersistence() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE, FULLY_POPULATED_RUNSTATE);
    final RunState read = storage
        .readActiveStates(WORKFLOW_INSTANCE.workflowId().componentId())
        .get(WORKFLOW_INSTANCE);
    assertThat(read, is(FULLY_POPULATED_RUNSTATE));
  }

  @Test
  public void shouldDeleteActiveWorkflowInstance() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE1, RUN_STATE);
    storage.writeActiveState(WORKFLOW_INSTANCE2, RUN_STATE2);

    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(2));

    storage.deleteActiveState(WORKFLOW_INSTANCE1);
    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(1));
  }

  @Test
  public void shouldReturnAllActiveStates() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE1, RUN_STATE);
    storage.writeActiveState(WORKFLOW_INSTANCE2, RUN_STATE2);

    final Map<WorkflowInstance, RunState> activeStates = storage.readActiveStates();
    assertThat(activeStates, is(Map.of(
        WORKFLOW_INSTANCE1, RUN_STATE,
        WORKFLOW_INSTANCE2, RUN_STATE2)));
  }

  @Test
  public void shouldReturnAllActiveStatesForAComponent() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE2, RUN_STATE2);
    storage.writeActiveState(WORKFLOW_INSTANCE3, RUN_STATE3);

    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(2));

    final Map<WorkflowInstance, RunState> activeStates =
        storage.readActiveStates(WORKFLOW_ID1.componentId());

    assertThat(activeStates, is(Map.of(WORKFLOW_INSTANCE2, RUN_STATE2)));
  }

  @Test
  public void shouldReturnActiveStatesForAWorkflow() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE2, RUN_STATE2);
    storage.writeActiveState(WORKFLOW_INSTANCE3, RUN_STATE3);

    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(2));

    final Map<WorkflowInstance, RunState> activeStates =
        storage.readActiveStates(WORKFLOW_ID2.componentId(), WORKFLOW_ID2.id());

    assertThat(activeStates, is(Map.of(WORKFLOW_INSTANCE2, RUN_STATE2)));
  }

  @Test
  public void readActiveStatesShouldPropagateIOException() throws Exception {
    var cause = new IOException("foobar");
    doThrow(cause).when(datastore).query(any());

    var exception = assertThrows(IOException.class, () -> storage.readActiveStates());
    assertThat(exception, is(cause));
  }

  @Test
  public void shouldReturnAllActiveStatesForATriggerId() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE, FULLY_POPULATED_RUNSTATE);

    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(1));

    final Map<WorkflowInstance, RunState> activeStates =
        storage.activeStatesByTriggerId("foobar");

    assertThat(activeStates, is(Map.of(WORKFLOW_INSTANCE, FULLY_POPULATED_RUNSTATE)));
  }

  @Test
  public void shouldReturnActiveStateForWFI() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE2, RUN_STATE2);

    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(1));

    final Optional<RunState> activeStates =
        storage.readActiveState(WORKFLOW_INSTANCE2);

    assertThat(activeStates, is(Optional.of(RUN_STATE2)));
  }

  @Test
  public void shouldWriteActiveStatesWithSamePartitionAsSeparateEntities() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE1, RUN_STATE);
    storage.writeActiveState(WORKFLOW_INSTANCE2, RUN_STATE2);

    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(2));
  }

  private List<Entity> entitiesOfKind(String kind) {
    EntityQuery query = Query.newEntityQueryBuilder().setKind(kind).build();
    QueryResults<Entity> keys = datastoreClient.run(query);
    List<Entity> entities = new ArrayList<>();
    while (keys.hasNext()) {
      entities.add(keys.next());
    }
    return entities;
  }

  @Test
  public void allFieldsAreSetWhenRetrievingWorkflowState() throws Exception {
    storage.store(WORKFLOW);
    Instant instant = Instant.parse("2016-03-14T14:00:00Z");
    Instant offset = instant.plus(1, ChronoUnit.DAYS);
    TriggerInstantSpec spec = TriggerInstantSpec.create(instant, offset);
    storage.updateNextNaturalTrigger(WORKFLOW.id(), spec);
    WorkflowState state = WorkflowState.builder()
        .enabled(true)
        .nextNaturalTrigger(instant)
        .nextNaturalOffsetTrigger(offset)
        .build();
    storage.patchState(WORKFLOW.id(), state);

    WorkflowState retrieved = storage.workflowState(WORKFLOW.id());

    assertThat(retrieved, is(state));
  }

  @Test
  public void shouldReturnWorkflowsWithState() throws Exception {
    assertThat(storage.workflowsWithState().isEmpty(), is(true));

    Workflow workflow1 = workflow(WORKFLOW_ID1);
    Workflow workflow2 = workflow(WORKFLOW_ID2);
    Workflow workflow3 = workflow(WORKFLOW_ID3);

    storage.store(workflow1);
    storage.store(workflow2);
    storage.store(workflow3);

    var instant = Instant.parse("2016-03-14T14:00:00Z");
    var state_workflow_id1 = WorkflowState.builder()
            .enabled(true)
            .nextNaturalTrigger(instant)
            .nextNaturalOffsetTrigger(instant.plus(1, ChronoUnit.DAYS))
            .build();
    var state_workflow_id2 = state_workflow_id1.toBuilder().enabled(false).build();
    var state_workflow_id3 = state_workflow_id1.toBuilder().nextNaturalOffsetTrigger(instant.plus(2, ChronoUnit.DAYS)).build();

    storage.patchState(WORKFLOW_ID1, state_workflow_id1);
    storage.patchState(WORKFLOW_ID2, state_workflow_id2);
    storage.patchState(WORKFLOW_ID3, state_workflow_id3);

    var workflows = storage.workflowsWithState();
    assertThat(workflows.size(), is(3));

    assertThat(workflows, hasEntry(WORKFLOW_ID1, WorkflowWithState.create(workflow1, state_workflow_id1)));
    assertThat(workflows, hasEntry(WORKFLOW_ID2, WorkflowWithState.create(workflow2, state_workflow_id2)));
    assertThat(workflows, hasEntry(WORKFLOW_ID3, WorkflowWithState.create(workflow3, state_workflow_id3)));
  }

  @Test
  public void shouldReturnWorkflowWithState() throws Exception {
    storage.store(WORKFLOW);
    var instant = Instant.parse("2016-03-14T14:00:00Z");
    var offset = instant.plus(1, ChronoUnit.DAYS);
    var spec = TriggerInstantSpec.create(instant, offset);
    storage.updateNextNaturalTrigger(WORKFLOW.id(), spec);
    var state = WorkflowState.builder()
        .enabled(true)
        .nextNaturalTrigger(instant)
        .nextNaturalOffsetTrigger(offset)
        .build();
    storage.patchState(WORKFLOW.id(), state);

    var retrieved = storage.workflowWithState(WORKFLOW.id());

    assertThat(retrieved.orElseThrow().workflow(), is(WORKFLOW));
    assertThat(retrieved.orElseThrow().state(), is(state));
  }

  @Test
  public void shouldReturnEmptyWorkflowWithState() throws Exception {
    var retrieved = storage.workflowWithState(WORKFLOW.id());
    assertThat(retrieved.isEmpty(), is(true));
  }

  @Test
  public void getsGlobalDockerRunnerId() throws Exception {
    Entity config = Entity.newBuilder(DatastoreStorage.globalConfigKey(datastore.newKeyFactory()))
        .set(DatastoreStorage.PROPERTY_CONFIG_DOCKER_RUNNER_ID, "foobar")
        .build();
    datastoreClient.put(config);

    assertThat(storage.config().globalDockerRunnerId(), is("foobar"));
  }

  @Test
  public void getsGlobalFlyteRunnerId() throws Exception {
    Entity config = Entity.newBuilder(DatastoreStorage.globalConfigKey(datastore.newKeyFactory()))
        .set(DatastoreStorage.PROPERTY_CONFIG_FLYTE_RUNNER_ID, "foobar")
        .build();
    datastoreClient.put(config);

    assertThat(storage.config().globalFlyteRunnerId(), is("foobar"));
  }

  @Test
  public void shouldReturnEmptyClientBlacklist() throws IOException {
    Entity config = Entity.newBuilder(DatastoreStorage.globalConfigKey(datastore.newKeyFactory()))
        .set(DatastoreStorage.PROPERTY_CONFIG_CLIENT_BLACKLIST,
            List.of()).build();
    datastoreClient.put(config);
    assertThat(storage.config().clientBlacklist(), is(empty()));
  }

  @Test
  public void shouldReturnClientBlacklist() throws IOException {
    Entity config = Entity.newBuilder(DatastoreStorage.globalConfigKey(datastore.newKeyFactory()))
        .set(DatastoreStorage.PROPERTY_CONFIG_CLIENT_BLACKLIST,
            List.of(StringValue.of("v1"), StringValue.of("v2"), StringValue.of("v3")))
        .build();
    datastoreClient.put(config);
    var blacklist = storage.config().clientBlacklist();
    assertThat(blacklist.size(), is(3));
    assertThat(blacklist.contains("v1"), is(true));
    assertThat(blacklist.contains("v2"), is(true));
    assertThat(blacklist.contains("v3"), is(true));
  }

  @Test
  public void shouldReturnAllWorkflows() throws Exception {
    assertThat(storage.workflows().isEmpty(), is(true));

    Workflow workflow1 = workflow(WORKFLOW_ID1);
    Workflow workflow2 = workflow(WORKFLOW_ID2);
    Workflow workflow3 = workflow(WORKFLOW_ID3);

    storage.store(workflow1);
    storage.store(workflow2);
    storage.store(workflow3);

    var workflows = storage.workflows();
    assertThat(workflows.size(), is(3));
    assertThat(workflows, hasEntry(WORKFLOW_ID1, workflow1));
    assertThat(workflows, hasEntry(WORKFLOW_ID2, workflow2));
    assertThat(workflows, hasEntry(WORKFLOW_ID3, workflow3));
  }

  @Test
  public void shouldReturnAllWorkflowsWithNextNaturalTrigger() throws Exception {
    assertThat(storage.workflows().isEmpty(), is(true));

    var workflow1 = workflow(WORKFLOW_ID1);
    var workflow2 = workflow(WORKFLOW_ID2);
    var workflow3 = workflow(WORKFLOW_ID3);

    var now = Instant.parse("2019-04-03T00:00:00Z");
    var triggerSpec1 = TriggerInstantSpec.create(now.plusSeconds(1), now.plusSeconds(11));
    var triggerSpec2 = TriggerInstantSpec.create(now.plusSeconds(2), now.plusSeconds(22));
    var triggerSpec3 = TriggerInstantSpec.create(now.plusSeconds(3), now.plusSeconds(33));
    storage.runInTransactionWithRetries(tx -> tx.storeWorkflowWithNextNaturalTrigger(workflow1, triggerSpec1));
    storage.runInTransactionWithRetries(tx -> tx.storeWorkflowWithNextNaturalTrigger(workflow2, triggerSpec2));
    storage.runInTransactionWithRetries(tx -> tx.storeWorkflowWithNextNaturalTrigger(workflow3, triggerSpec3));

    var workflows = storage.workflowsWithNextNaturalTrigger();
    assertThat(workflows.size(), is(3));
    assertThat(workflows, hasEntry(workflow1, triggerSpec1));
    assertThat(workflows, hasEntry(workflow2, triggerSpec2));
    assertThat(workflows, hasEntry(workflow3, triggerSpec3));
  }

  @Test
  public void shouldFailToReadCorruptWorkflow() throws Exception {
    assertThat(storage.workflows().isEmpty(), is(true));

    var instant = Instant.parse("2019-05-08T01:00:00Z");
    var workflowState = WorkflowState.builder()
        .enabled(true)
        .nextNaturalTrigger(instant)
        .nextNaturalOffsetTrigger(instant)
        .build();

    var workflow1 = workflow(WORKFLOW_ID1);
    storage.store(workflow1);
    storage.patchState(WORKFLOW_ID1, workflowState);
    var workflowKey1 = workflowKey(datastore::newKeyFactory, workflow1.id());

    var workflowEntity1 = datastore.get(workflowKey1);
    var corruptedWorkflowEntity1 = Entity.newBuilder(workflowEntity1)
        .set("json", "bork")
        .build();
    datastore.put(corruptedWorkflowEntity1);

    var workflow2 = workflow(WORKFLOW_ID2);
    storage.store(workflow2);
    storage.patchState(WORKFLOW_ID2, workflowState);

    assertThat(storage.workflows(), is(Map.of(WORKFLOW_ID2, workflow2)));
    assertThat(storage.workflowsWithNextNaturalTrigger(),
        is(Map.of(workflow2, TriggerInstantSpec.create(instant, instant))));
    assertThat(storage.workflows(Set.of(WORKFLOW_ID1, WORKFLOW_ID2)), is(Map.of(WORKFLOW_ID2, workflow2)));
    assertThat(storage.workflows(WORKFLOW_ID1.componentId()), contains(workflow2));

    assertThrows(IOException.class, () -> storage.workflow(workflow1.id()));
  }

  @Test
  public void shouldGetAllWorkflowsByDoingBatchGet() throws Exception {
    assertThat(storage.workflows().isEmpty(), is(true));

    final Set<WorkflowId> workflowIds = ImmutableSet.of(WORKFLOW_ID1, WORKFLOW_ID2, WORKFLOW_ID3);
    Workflow workflow1 = workflow(WORKFLOW_ID1);
    Workflow workflow2 = workflow(WORKFLOW_ID2);
    Workflow workflow3 = workflow(WORKFLOW_ID3);

    storage.store(workflow1);
    storage.store(workflow2);
    storage.store(workflow3);

    var workflows = storage.workflows(workflowIds);
    assertThat(workflows.size(), is(3));
    assertThat(workflows, hasEntry(WORKFLOW_ID1, workflow1));
    assertThat(workflows, hasEntry(WORKFLOW_ID2, workflow2));
    assertThat(workflows, hasEntry(WORKFLOW_ID3, workflow3));
  }

  @Test
  public void shouldReturnAllWorkflowsInComponent() throws Exception {
    String componentId = "component";

    Workflow workflow1 = workflow(WORKFLOW_ID1);
    Workflow workflow2 = workflow(WORKFLOW_ID2);
    Workflow workflow3 = workflow(WORKFLOW_ID3);

    assertThat(workflow1.componentId(), is(componentId));
    assertThat(workflow2.componentId(), is(componentId));
    assertThat(workflow3.componentId(), not(componentId));

    storage.store(workflow1);
    storage.store(workflow2);
    storage.store(workflow3);


    List<Workflow> l = storage.workflows(componentId);
    assertThat(l, hasSize(2));

    assertThat(l, hasItem(workflow1));
    assertThat(l, hasItem(workflow2));
  }

  @Test
  public void shouldReturnEmptyListIfComponentDoesNotExist() throws Exception {
    String componentId = "component";
    List<Workflow> l = storage.workflows(componentId);
    assertThat(l, hasSize(0));
  }

  @Test
  public void testDefaultConfig() throws IOException {
    final StyxConfig expectedConfig = StyxConfig.newBuilder()
        .globalDockerRunnerId("default")
        .globalFlyteRunnerId("default")
        .globalEnabled(true)
        .debugEnabled(false)
        .build();

    assertThat(storage.config(), is(expectedConfig));
  }

  @Parameters({
      "true, true",
      "false, false",
      "_, true"})
  @Test
  public void shouldStoreAndReadBackfillWithoutCreatedAndModifiedTS(String reverse, boolean withTriggerParameters) throws Exception {
    final BackfillBuilder builder = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-02T00:00:00Z"))
        .workflowId(WorkflowId.create("component", "workflow2"))
        .concurrency(2)
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(DAYS);

    if (!reverse.trim().equals("_")) {
      builder.reverse(Boolean.parseBoolean(reverse));
    }

    if (withTriggerParameters) {
      builder.triggerParameters(TriggerParameters.builder()
          .env("FOO", "foo",
              "BAR", "bar")
          .build());
    }

    final Backfill backfill = builder.build();

    storage.storeBackfill(backfill);
    assertThat(storage.getBackfill(backfill.id()), equalTo(Optional.of(backfill)));
  }

  @Test
  public void shouldReadBackfillWithMissingReverseField() throws Exception {
    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-02T00:00:00Z"))
        .workflowId(WorkflowId.create("component", "workflow2"))
        .concurrency(2)
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(DAYS)
        .created(currentTime)
        .lastModified(currentTime)
        .build();

    final Key key = DatastoreStorage.backfillKey(datastore.newKeyFactory(), backfill.id());
    Entity.Builder builder = Entity.newBuilder(key)
        .set(PROPERTY_CONCURRENCY, backfill.concurrency())
        .set(PROPERTY_START, instantToTimestamp(backfill.start()))
        .set(PROPERTY_END, instantToTimestamp(backfill.end()))
        .set(PROPERTY_COMPONENT, backfill.workflowId().componentId())
        .set(PROPERTY_WORKFLOW, backfill.workflowId().id())
        .set(PROPERTY_SCHEDULE, backfill.schedule().toString())
        .set(PROPERTY_NEXT_TRIGGER, instantToTimestamp(backfill.nextTrigger()))
        .set(PROPERTY_ALL_TRIGGERED, backfill.allTriggered())
        .set(PROPERTY_HALTED, backfill.halted())
        .set(PROPERTY_CREATED, instantToTimestamp(backfill.created().orElseThrow()))
        .set(PROPERTY_LAST_MODIFIED, instantToTimestamp(backfill.lastModified().orElseThrow()));

    datastore.put(builder.build());

    assertThat(storage.getBackfill(backfill.id()), equalTo(Optional.of(backfill)));
  }

  @Test
  public void shouldStoreAndReadBackfillWithDescription() throws Exception {
    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-02T00:00:00Z"))
        .workflowId(WorkflowId.create("component", "workflow2"))
        .concurrency(2)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(DAYS)
        .created(currentTime)
        .lastModified(currentTime)
        .build();

    storage.storeBackfill(backfill);
    assertThat(storage.getBackfill(backfill.id()), equalTo(Optional.of(backfill)));
  }

  private Workflow workflow(WorkflowId workflowId) {
    return Workflow.create(
        workflowId.componentId(),
        WorkflowConfiguration.builder()
            .id(workflowId.id())
            .schedule(HOURS)
            .build());
  }

  @Test
  public void runInTransactionWithRetriesShouldCallFunction() throws Exception {
    final DatastoreStorage storage = new DatastoreStorage(datastore, storageTransactionFactory,
        executor, logger);
    final CheckedDatastoreTransaction transaction = datastore.newTransaction();
    final DatastoreStorageTransaction storageTransaction = spy(new DatastoreStorageTransaction(transaction));
    when(storageTransactionFactory.apply(any())).thenReturn(storageTransaction);

    when(transactionFunction.apply(any())).thenReturn("foo");

    String result = storage.runInTransactionWithRetries(transactionFunction);

    assertThat(result, is("foo"));
    verify(transactionFunction).apply(storageTransaction);
  }

  @Test
  public void runInTransactionWithRetriesShouldPropagateUserException() throws Exception {
    final Exception expectedException = new FooException();
    when(transactionFunction.apply(any())).thenThrow(expectedException);

    try {
      storage.runInTransactionWithRetries(transactionFunction);
      fail("Expected exception!");
    } catch (FooException e) {
      // Verify that we can throw a user defined checked exception type inside the transaction
      // body and catch it
      assertThat(e, is(expectedException));
    }

    verify(transactionFunction).apply(any());
  }

  @Test
  public void runInTransactionWithRetriesShouldRetryOnConflict() throws Exception {
    var workflow = TestData.WORKFLOW_WITH_RESOURCES;

    // Store workflow
    storage.runInTransactionWithRetries(tx -> {
      tx.store(workflow);
      return null;
    });

    // Start a losing transaction that reads, waits for barrier and then stores the workflow
    var runs = new AtomicInteger();
    var barrier = new CountDownLatch(1);
    var future = executor.submit(() -> storage.runInTransactionWithRetries(tx -> {
      runs.incrementAndGet();
      var wf = tx.workflow(workflow.id());
      barrier.await();
      tx.store(wf.orElseThrow());
      return null;
    }));

    // Execute a winning read-store transaction
    storage.runInTransactionWithRetries(tx -> {
      var wf = tx.workflow(workflow.id());
      tx.store(wf.orElseThrow());
      return null;
    });
    barrier.countDown();

    // Wait for first transaction to also complete and verify that it ran twice
    future.get(30, SECONDS);
    assertThat(runs.get(), is(2));
  }

  @Test
  public void runInTransactionWithRetriesShouldHandleRollbackFailure() throws Exception {
    var transaction = spy(datastore.newTransaction());
    doReturn(transaction).when(datastore).newTransaction();
    var rollbackFailure = new DatastoreIOException(new DatastoreException(1, "fail", "error"));
    doThrow(rollbackFailure).when(transaction).rollback();

    var cause = new RuntimeException("foobar");

    // Run a failing transaction and verify that the rollback failure does not suppress our exception
    try {
      storage.runInTransactionWithRetries(tx -> {
        throw cause;
      });
    } catch (RuntimeException e) {
      assertThat(e, is(cause));
    }

    verify(transaction).rollback();
  }

  @Test
  public void runInTransactionWithRetriesShouldGiveUpOnTimeout() throws Exception {
    var workflow = TestData.WORKFLOW_WITH_RESOURCES;

    // Store workflow
    storage.runInTransactionWithRetries(tx -> {
      tx.store(workflow);
      return null;
    });

    // Start a losing transaction that reads, waits for barrier and then stores the workflow
    var runs = new AtomicInteger();
    var barrier = new CountDownLatch(1);
    var future = executor.submit(() -> storage.runInTransactionWithRetries(tx -> {
      runs.incrementAndGet();
      var wf = tx.workflow(workflow.id());
      barrier.await();
      Thread.sleep(RETRY_SETTINGS.getTotalTimeout().toMillis());
      tx.store(wf.orElseThrow());
      return null;
    }));

    // Execute a winning read-store transaction
    storage.runInTransactionWithRetries(tx -> {
      var wf = tx.workflow(workflow.id());
      tx.store(wf.orElseThrow());
      return null;
    });
    barrier.countDown();

    // Wait for first transaction to fail
    try {
      future.get(30, SECONDS);
      fail("Expected transaction to fail");
    } catch (ExecutionException e) {
      var cause = e.getCause();
      assertThat(cause, instanceOf(DatastoreIOException.class));
      var dex = ((DatastoreIOException) cause).getCause();
      assertThat(dex.getCode(), is(10));
    }

    assertThat(runs.get(), is(1));
  }

  @Test
  public void runInTransactionWithRetriesShouldRetryUntilMaxAttempts() throws Exception {
    var workflow = TestData.WORKFLOW_WITH_RESOURCES;

    // Store workflow
    storage.runInTransactionWithRetries(tx -> {
      tx.store(workflow);
      return null;
    });

    // Start a losing transaction that reads, waits for barrier and then stores the workflow
    var runs = new AtomicInteger();
    var waiting = new LinkedBlockingQueue<Boolean>();
    var proceed = new LinkedBlockingQueue<Boolean>();
    var future = executor.submit(() -> storage.runInTransactionWithRetries(tx -> {
      runs.incrementAndGet();
      var wf = tx.workflow(workflow.id());
      waiting.put(true);
      proceed.take();
      tx.store(wf.orElseThrow());
      return null;
    }));

    // Execute winning read-store transactions to make each retry of the above transaction fail
    for (int i = 0; i < RETRY_SETTINGS.getMaxAttempts(); i++) {
      waiting.take();
      storage.runInTransactionWithRetries(tx -> {
        var wf = tx.workflow(workflow.id());
        tx.store(wf.orElseThrow());
        return null;
      });
      proceed.put(true);
    }

    // Wait for first transaction to give up
    try {
      future.get(300, SECONDS);
      fail("Expected transaction to fail");
    } catch (ExecutionException e) {
      var cause = e.getCause();
      assertThat(cause, instanceOf(DatastoreIOException.class));
      var dex = ((DatastoreIOException) cause).getCause();
      assertThat(dex.getCode(), is(10));
    }

    // Verify that it retried as many times as expected before giving up
    assertThat(runs.get(), is(RETRY_SETTINGS.getMaxAttempts()));
  }

  @Test
  public void runInTransactionWithRetriesShouldCallFunctionAndRollbackOnPreCommitConflict() throws Exception {
    var expectedException = new DatastoreIOException(new DatastoreException(1, "", ""));
    when(transactionFunction.apply(any())).thenThrow(expectedException);

    var transaction = spy(datastore.newTransaction());
    when(datastore.newTransaction()).thenReturn(transaction);

    try {
      storage.runInTransactionWithRetries(transactionFunction);
      fail("Expected exception!");
    } catch (DatastoreIOException e) {
      assertThat(e, is(expectedException));
    }

    verify(transactionFunction).apply(any());
    verify(transaction, never()).commit();
    verify(transaction).rollback();
  }

  @Test
  public void runInTransactionWithRetriesShouldCallFunctionAndRollbackOnCommitFailure() throws Exception {
    var commitException = new DatastoreIOException(new DatastoreException(1, "", ""));

    var transaction = spy(datastore.newTransaction());
    doReturn(transaction).when(datastore).newTransaction();

    when(transactionFunction.apply(any())).thenReturn("");
    doThrow(commitException).when(transaction).commit();

    try {
      storage.runInTransactionWithRetries(transactionFunction);
      fail("Expected exception!");
    } catch (DatastoreIOException e) {
      assertThat(e, is(commitException));
    }

    verify(transactionFunction).apply(any());
    verify(transaction).rollback();
  }

  @Test
  public void runInTransactionWithRetriesShouldPropagateCommitExceptionIfRollbackFails() throws Exception {

    var transaction = spy(datastore.newTransaction());
    doReturn(transaction).when(datastore).newTransaction();

    when(transactionFunction.apply(any())).thenReturn("");

    var commitException = new DatastoreIOException(new DatastoreException(1, "", ""));
    doThrow(commitException).when(transaction).commit();

    var rollbackException = new DatastoreIOException(new DatastoreException(2, "", ""));
    doThrow(rollbackException).when(transaction).rollback();

    try {
      storage.runInTransactionWithRetries(transactionFunction);
      fail("Expected exception!");
    } catch (DatastoreIOException e) {
      assertThat(e, is(commitException));
    }

    verify(transactionFunction).apply(any());
    verify(transaction).rollback();
    verify(logger).debug("Exception on rollback", rollbackException);
  }

  @Test
  public void runInTransactionWithRetriesShouldPropagateDatastoreIOException() throws Exception {
    var datastoreException = new DatastoreException(1, "", "");
    var datastoreIOException = new DatastoreIOException(datastoreException);
    when(transactionFunction.apply(any())).thenThrow(datastoreIOException);
  }

  @Test
  public void runInTransactionWithRetriesShouldThrowIfDatastoreNewTransactionFails() throws Exception {
    var cause = new DatastoreIOException(new DatastoreException(1, "", ""));
    doThrow(cause).when(datastore).newTransaction();

    var exception = assertThrows(
        DatastoreIOException.class,
        () -> storage.runInTransactionWithRetries(transactionFunction)
    );

    assertThat(exception, is(cause));
  }

  @Test
  public void shouldReturnResource() throws Exception {
    storage.runInTransactionWithRetries(tx -> {
      tx.store(RESOURCE1);
      tx.store(RESOURCE2);
      return null;
    });
    assertThat(storage.getResource(RESOURCE1.id()), is(Optional.of(RESOURCE1)));
  }

  @Test
  public void shouldReturnResources() throws IOException {
    storage.runInTransactionWithRetries(tx -> {
      tx.store(RESOURCE1);
      tx.store(RESOURCE2);
      return null;
    });
    assertThat(storage.getResources(), is(List.of(RESOURCE1, RESOURCE2)));
  }

  @Test
  public void shouldDeleteResource() throws IOException {
    storage.runInTransactionWithRetries(tx -> {
      tx.store(RESOURCE1);
      return null;
    });
    storage.deleteResource(RESOURCE1.id());
    assertThat(storage.getResources(), is(List.of()));
    assertThat(storage.shardsForCounter(RESOURCE1.id()), is(Map.of()));
  }

  @Test
  public void shouldReturnShardsForCounter() throws Exception {
    storage.runInTransactionWithRetries(tx -> {
      tx.store(Shard.create(RESOURCE1.id(), 0, 0));
      tx.store(Shard.create(RESOURCE1.id(), 1, 3));
      return null;
    });
    final Map<Integer, Long> map = storage.shardsForCounter(RESOURCE1.id());
    assertEquals(2, map.size());
    assertEquals(0, map.get(0).longValue());
    assertEquals(3, map.get(1).longValue());
  }

  @Test
  public void shouldReturnCounterLimit() throws IOException {
    storage.runInTransactionWithRetries(tx -> {
      tx.store(RESOURCE1);
      return null;
    });

    assertEquals(1L, storage.getLimitForCounter(RESOURCE1.id()));
  }

  @Test
  public void shouldReturnDefaultGlobalCounterLimit() throws IOException {
    assertEquals(Long.MAX_VALUE, storage.getLimitForCounter("GLOBAL_STYX_CLUSTER"));
  }

  @Test
  public void shouldReturnGlobalCounterLimit() throws IOException {
    final Key key = globalConfigKey(datastore.newKeyFactory());
    Entity.Builder builder = Entity.newBuilder(key)
        .set(PROPERTY_CONCURRENCY, 4000L);
    datastore.put(builder.build());

    assertEquals(4000L, storage.getLimitForCounter("GLOBAL_STYX_CLUSTER"));
  }

  @Test
  public void shouldGetExceptionForUnknownCounter() {
    var exception = assertThrows(
        IllegalArgumentException.class,
        () -> storage.getLimitForCounter("bar-resource"));

    assertThat(exception.getMessage(), is("No limit found in Datastore for bar-resource"));
  }

  @Test
  public void shouldStoreWorkflows() throws IOException {
    var workflow = workflow(WORKFLOW_ID1);

    storage.store(workflow);

    var newKey = workflowKey(datastore::newKeyFactory, workflow.id());
    var newEntity = datastore.get(newKey);
    var newProperties = newEntity.getNames()
        .stream()
        .collect(toMap(key -> key, name -> newEntity.getValue(name).get()));
    assertThat(
        newProperties,
        allOf(
            hasEntry(PROPERTY_COMPONENT, workflow.id().componentId()),
            hasEntry(PROPERTY_WORKFLOW_JSON, Json.OBJECT_MAPPER.writeValueAsString(workflow))));
  }

  @Test
  public void sleepShouldBeInterruptible() throws Exception {
    var running = new CompletableFuture<String>();
    var future = executor.submit(() -> {
      running.complete(null);
      DatastoreStorage.sleepMillis(Long.MAX_VALUE);
      return "foobar";
    });
    running.join();
    executor.shutdownNow();
    var result = future.get(30, SECONDS);
    assertThat(result, is("foobar"));
  }

  private static class FooException extends Exception {
  }
}
