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

import static com.github.npathai.hamcrestopt.OptionalMatchers.hasValue;
import static com.spotify.styx.model.Schedule.DAYS;
import static com.spotify.styx.model.Schedule.HOURS;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_CONFIG_BOOTSTRAP_ACTIVE_WFI_ENABLED;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_CONFIG_RESOURCES_SYNC_ENABLED;
import static com.spotify.styx.storage.DatastoreStorage.activeWorkflowInstanceIndexShardEntryKey;
import static com.spotify.styx.testdata.TestData.FULL_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.http.protobuf.ProtoHttpContent;
import com.google.bigtable.repackaged.com.google.common.collect.ImmutableList;
import com.google.bigtable.repackaged.com.google.common.collect.ImmutableMap;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.PathElement;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.Transaction;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteStreams;
import com.google.datastore.v1.LookupRequest;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfiguration.Secret;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.Message.MessageLevel;
import com.spotify.styx.state.RunState;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.util.Shard;
import com.spotify.styx.util.TriggerInstantSpec;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DatastoreStorageTest {

  static final WorkflowId WORKFLOW_ID1 = WorkflowId.create("component", "endpoint1");
  static final WorkflowId WORKFLOW_ID2 = WorkflowId.create("component", "endpoint2");
  static final WorkflowId WORKFLOW_ID3 = WorkflowId.create("component2", "pointless");

  static final WorkflowInstance WORKFLOW_INSTANCE1 = WorkflowInstance.create(WORKFLOW_ID1, "2016-09-01");
  static final WorkflowInstance WORKFLOW_INSTANCE2 = WorkflowInstance.create(WORKFLOW_ID2, "2016-09-01");
  static final WorkflowInstance WORKFLOW_INSTANCE3 = WorkflowInstance.create(WORKFLOW_ID3, "2016-09-01");

  static final Instant TIMESTAMP = Instant.parse("2017-01-01T00:00:00Z");


  static final RunState RUN_STATE = RunState.create(WORKFLOW_INSTANCE1, State.NEW,
      StateData.zero(), TIMESTAMP, 42L);

  static final RunState RUN_STATE1 = RunState.create(WORKFLOW_INSTANCE1, State.NEW,
      StateData.zero(), TIMESTAMP, 43L);

  static final RunState RUN_STATE2 = RunState.create(WORKFLOW_INSTANCE2, State.NEW,
      StateData.zero(), TIMESTAMP, 84L);

  static final RunState RUN_STATE3 = RunState.create(WORKFLOW_INSTANCE3, State.NEW,
      StateData.zero(), TIMESTAMP, 17L);


  static final RunState FULLY_POPULATED_RUNSTATE = RunState.create(WORKFLOW_INSTANCE, State.QUEUED,
      StateData.newBuilder()
          .tries(17)
          .consecutiveFailures(89)
          .retryCost(2.0)
          .retryDelayMillis(4711L)
          .lastExit(13)
          .trigger(Trigger.adhoc("foobar"))
          .executionId("foo-bar-17")
          .executionDescription(ExecutionDescription.builder()
              .dockerImage("foo/bar:34234")
              .dockerArgs("foo", "the", "bar", "baz")
              .dockerTerminationLogging(true)
              .secret(Secret.create("foobar", "/var/quux/baz"))
              .serviceAccount("foo@bar.baz")
              .commitSha("2d2bfa926b94508de5aab47b5f305659ead2274a")
              .build())
          .resourceIds(ImmutableSet.of("GLOBAL_STYX_CLUSTER", "foo-resource", "bar-resource"))
          .addMessage(Message.info("foo"))
          .addMessage(Message.warning("bar"))
          .addMessage(Message.error("baz"))
          .addMessage(Message.create(MessageLevel.UNKNOWN, "quux"))
          .build(),
      TIMESTAMP,
      42L);

  static final WorkflowId WORKFLOW_ID = WorkflowId.create("dockerComp", "dockerEndpoint");

  static final WorkflowConfiguration WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id(WORKFLOW_ID.id())
          .schedule(DAYS)
          .build();
  static final Workflow WORKFLOW = Workflow.create(WORKFLOW_ID.componentId(),
                                                           WORKFLOW_CONFIGURATION);
  private static final String COUNTER_ID1 = "counter-id1";

  private static LocalDatastoreHelper helper;
  private DatastoreStorage storage;
  private Datastore datastore;

  @Mock TransactionFunction<String, FooException> transactionFunction;
  @Mock Function<Transaction, DatastoreStorageTransaction> storageTransactionFactory;

  @BeforeClass
  public static void setUpClass() throws Exception {
    // TODO: the datastore emulator behavior wrt conflicts etc differs from the real datastore
    helper = LocalDatastoreHelper.create(1.0); // 100% global consistency
    helper.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (helper != null) {
      try {
        helper.stop(org.threeten.bp.Duration.ofSeconds(30));
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    datastore = helper.getOptions().getService();
    storage = new DatastoreStorage(datastore, Duration.ZERO);
  }

  @After
  public void tearDown() throws Exception {
    // clear datastore after each test
    KeyQuery query = Query.newKeyQueryBuilder().build();
    final QueryResults<Key> keys = datastore.run(query);
    while (keys.hasNext()) {
      datastore.delete(keys.next());
    }
  }

  @Test
  public void shouldPersistWorkflows() throws Exception {
    Workflow workflow = Workflow.create("test",
                                        FULL_WORKFLOW_CONFIGURATION);
    storage.store(workflow);
    Optional<Workflow> retrieved = storage.workflow(workflow.id());

    assertThat(retrieved, is(Optional.of(workflow)));
  }

  @Test
  public void shouldDeleteWorkflows() throws Exception {
    final Workflow workflow = Workflow.create("foo", WORKFLOW_CONFIGURATION);
    storage.store(workflow);
    storage.store(Workflow.create("bar", WORKFLOW_CONFIGURATION));

    assertThat(entitiesOfKind(DatastoreStorage.KIND_WORKFLOW), hasSize(2));

    storage.delete(workflow.id());
    assertThat(entitiesOfKind(DatastoreStorage.KIND_WORKFLOW), hasSize(1));
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
    assertThat(workflow, hasValue(WORKFLOW));
    assertTrue(enabled);

    storage.setEnabled(id, false);
    workflow = storage.workflow(id);
    enabled = storage.enabled(id);
    assertThat(workflow, hasValue(WORKFLOW));
    assertFalse(enabled);

    storage.store(WORKFLOW);
    workflow = storage.workflow(id);
    enabled = storage.enabled(id);
    assertThat(workflow, hasValue(WORKFLOW));
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
    assertThat(activeStates, is(ImmutableMap.of(
        WORKFLOW_INSTANCE1, RUN_STATE,
        WORKFLOW_INSTANCE2, RUN_STATE2)));
  }

  @Test
  public void shouldBootstrapActiveStatesIndexes() throws Exception {
    shouldReindexActiveStates(storage::indexActiveWorkflowInstances);
  }

  @Test
  public void shouldConcurrentlyIndexActiveStates() throws IOException {
    // XXX: this does not really test that this indexing method works as expected when running concurrently with the
    //      scheduler, it only checks that it creates missing index entries at all.
    shouldReindexActiveStates(storage::concurrentlyIndexActiveWorkflowInstances);
  }

  public void shouldReindexActiveStates(Runnable indexer) throws IOException {
    storage.writeActiveState(WORKFLOW_INSTANCE1, RUN_STATE);
    storage.writeActiveState(WORKFLOW_INSTANCE2, RUN_STATE2);

    final Key indexEntryKey1 = activeWorkflowInstanceIndexShardEntryKey(datastore.newKeyFactory(), WORKFLOW_INSTANCE1);
    final Key indexEntryKey2 = activeWorkflowInstanceIndexShardEntryKey(datastore.newKeyFactory(), WORKFLOW_INSTANCE2);

    assertThat(datastore.get(indexEntryKey1), notNullValue());
    assertThat(datastore.get(indexEntryKey2), notNullValue());

    datastore.delete(indexEntryKey1, indexEntryKey2);

    assertThat(datastore.get(indexEntryKey1), is(nullValue()));
    assertThat(datastore.get(indexEntryKey2), is(nullValue()));

    indexer.run();

    assertThat(datastore.get(indexEntryKey1), notNullValue());
    assertThat(datastore.get(indexEntryKey2), notNullValue());

    final Map<WorkflowInstance, RunState> activeStates = storage.readActiveStates();

    assertThat(activeStates, is(ImmutableMap.of(
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

    assertThat(activeStates, is(ImmutableMap.of(WORKFLOW_INSTANCE2, RUN_STATE2)));
  }

  @Test
  public void shouldReturnAllActiveStatesForATriggerId() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE, FULLY_POPULATED_RUNSTATE);

    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(1));

    final Map<WorkflowInstance, RunState> activeStates =
        storage.activeStatesByTriggerId("foobar");

    assertThat(activeStates, is(ImmutableMap.of(WORKFLOW_INSTANCE, FULLY_POPULATED_RUNSTATE)));
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
    Datastore datastore = helper.getOptions().getService();
    EntityQuery query = Query.newEntityQueryBuilder().setKind(kind).build();
    QueryResults<Entity> keys = datastore.run(query);
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
  public void getsGlobalDockerRunnerId() throws Exception {
    Entity config = Entity.newBuilder(DatastoreStorage.globalConfigKey(datastore.newKeyFactory()))
        .set(DatastoreStorage.PROPERTY_CONFIG_DOCKER_RUNNER_ID, "foobar")
        .build();
    helper.getOptions().getService().put(config);

    assertThat(storage.config().globalDockerRunnerId(), is("foobar"));
  }

  @Test
  public void getsResourcesSyncEnabled() {
    Entity config = Entity.newBuilder(DatastoreStorage.globalConfigKey(datastore.newKeyFactory()))
        .set(PROPERTY_CONFIG_RESOURCES_SYNC_ENABLED, true)
        .build();
    helper.getOptions().getService().put(config);

    assertThat(storage.config().resourcesSyncEnabled(), is(true));
  }

  @Test
  public void getsBootstrapActiveWFIEnabled() {
    Entity config = Entity.newBuilder(DatastoreStorage.globalConfigKey(datastore.newKeyFactory()))
        .set(PROPERTY_CONFIG_BOOTSTRAP_ACTIVE_WFI_ENABLED, true)
        .build();
    helper.getOptions().getService().put(config);

    assertThat(storage.config().bootstrapActiveWFIEnabled(), is(true));
  }

  @Test
  public void shouldReturnEmptyClientBlacklist() {
    Entity config = Entity.newBuilder(DatastoreStorage.globalConfigKey(datastore.newKeyFactory()))
        .set(DatastoreStorage.PROPERTY_CONFIG_CLIENT_BLACKLIST,
            ImmutableList.of()).build();
    helper.getOptions().getService().put(config);
    assertThat(storage.config().clientBlacklist(), is(empty()));
  }

  @Test
  public void shouldReturnClientBlacklist() {
    Entity config = Entity.newBuilder(DatastoreStorage.globalConfigKey(datastore.newKeyFactory()))
        .set(DatastoreStorage.PROPERTY_CONFIG_CLIENT_BLACKLIST,
            ImmutableList.of(StringValue.of("v1"), StringValue.of("v2"), StringValue.of("v3")))
        .build();
    helper.getOptions().getService().put(config);
    List<String> blacklist = storage.config().clientBlacklist();
    assertThat(blacklist.size(), is(3));
    assertThat(blacklist.get(0), is("v1"));
    assertThat(blacklist.get(1), is("v2"));
    assertThat(blacklist.get(2), is("v3"));
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

    storage.setEnabled(WORKFLOW_ID1, true);
    storage.setEnabled(WORKFLOW_ID2, false);
    storage.updateNextNaturalTrigger(WORKFLOW_ID3, TriggerInstantSpec.create(TIMESTAMP, TIMESTAMP.plus(Duration.ofHours(1))));

    assertThat(storage.workflows().size(), is(3));
    assertThat(storage.workflows(), hasEntry(WORKFLOW_ID1, workflow1));
    assertThat(storage.workflows(), hasEntry(WORKFLOW_ID2, workflow2));
    assertThat(storage.workflows(), hasEntry(WORKFLOW_ID3, workflow3));
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

    storage.setEnabled(WORKFLOW_ID1, true);
    storage.setEnabled(WORKFLOW_ID2, false);
    storage.updateNextNaturalTrigger(WORKFLOW_ID3, TriggerInstantSpec.create(TIMESTAMP, TIMESTAMP.plus(Duration.ofHours(1))));

    assertThat(storage.workflows(workflowIds).size(), is(3));
    assertThat(storage.workflows(workflowIds), hasEntry(WORKFLOW_ID1, workflow1));
    assertThat(storage.workflows(workflowIds), hasEntry(WORKFLOW_ID2, workflow2));
    assertThat(storage.workflows(workflowIds), hasEntry(WORKFLOW_ID3, workflow3));
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
  public void testDefaultConfig() {
    final StyxConfig expectedConfig = StyxConfig.newBuilder()
        .globalDockerRunnerId("default")
        .globalEnabled(true)
        .debugEnabled(false)
        .resourcesSyncEnabled(false)
        .executionGatingEnabled(false)
        .build();

    assertThat(storage.config(), is(expectedConfig));
  }

  @Test
  public void shouldStoreAndReadBackfill() throws Exception {
    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-02T00:00:00Z"))
        .workflowId(WorkflowId.create("component", "workflow2"))
        .concurrency(2)
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(DAYS)
        .build();

    storage.storeBackfill(backfill);
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
  public void runInTransactionShouldCallFunctionAndCommit() throws Exception {
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO, storageTransactionFactory);
    final Transaction transaction = datastore.newTransaction();
    final DatastoreStorageTransaction storageTransaction = spy(new DatastoreStorageTransaction(transaction));
    when(storageTransactionFactory.apply(any())).thenReturn(storageTransaction);

    when(transactionFunction.apply(any())).thenReturn("foo");

    String result = storage.runInTransaction(transactionFunction);

    assertThat(result, is("foo"));
    verify(transactionFunction).apply(storageTransaction);
    verify(storageTransaction).commit();
    verify(storageTransaction, never()).rollback();
  }

  @Test
  public void runInTransactionShouldCallFunctionAndRollbackOnFailure() throws Exception {
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO, storageTransactionFactory);
    final Transaction transaction = datastore.newTransaction();
    final DatastoreStorageTransaction storageTransaction = spy(new DatastoreStorageTransaction(transaction));
    when(storageTransactionFactory.apply(any())).thenReturn(storageTransaction);

    final Exception expectedException = new FooException();
    when(transactionFunction.apply(any())).thenThrow(expectedException);

    try {
      storage.runInTransaction(transactionFunction);
      fail("Expected exception!");
    } catch (FooException e) {
      // Verify that we can throw a user defined checked exception type inside the transaction
      // body and catch it
      assertThat(e, is(expectedException));
    }

    verify(transactionFunction).apply(storageTransaction);
    verify(storageTransaction, never()).commit();
    verify(storageTransaction).rollback();
  }

  @Test
  public void runInTransactionShouldCallFunctionAndRollbackOnPreCommitConflict() throws Exception {
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO, storageTransactionFactory);
    final Transaction transaction = datastore.newTransaction();
    final DatastoreStorageTransaction storageTransaction = spy(new DatastoreStorageTransaction(transaction));
    when(storageTransactionFactory.apply(any())).thenReturn(storageTransaction);

    final Exception expectedException = new DatastoreException(10, "", "");
    when(transactionFunction.apply(any())).thenThrow(expectedException);

    try {
      storage.runInTransaction(transactionFunction);
      fail("Expected exception!");
    } catch (TransactionException e) {
      assertTrue(e.isConflict());
    }

    verify(transactionFunction).apply(storageTransaction);
    verify(storageTransaction, never()).commit();
    verify(storageTransaction).rollback();
  }

  @Test
  public void runInTransactionShouldCallFunctionAndRollbackOnCommitConflict() throws Exception {
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO, storageTransactionFactory);
    final Transaction transaction = datastore.newTransaction();
    final DatastoreStorageTransaction storageTransaction = spy(new DatastoreStorageTransaction(transaction));
    when(storageTransactionFactory.apply(any())).thenReturn(storageTransaction);

    final DatastoreException datastoreException = new DatastoreException(1, "", "");
    final TransactionException expectedException = new TransactionException(datastoreException);
    when(transactionFunction.apply(any())).thenReturn("");
    doThrow(expectedException).when(storageTransaction).commit();

    try {
      storage.runInTransaction(transactionFunction);
      fail("Expected exception!");
    } catch (TransactionException e) {
      assertThat(e, is(expectedException));
    }

    verify(transactionFunction).apply(storageTransaction);
    verify(storageTransaction).rollback();
  }

  @Test
  public void runInTransactionShouldThrowIfRollbackFailsAfterConflict() throws Exception {
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO, storageTransactionFactory);
    final Transaction transaction = datastore.newTransaction();
    final DatastoreStorageTransaction storageTransaction = spy(new DatastoreStorageTransaction(transaction));
    when(storageTransactionFactory.apply(any())).thenReturn(storageTransaction);

    when(transactionFunction.apply(any())).thenReturn("");
    final DatastoreException datastoreException = new DatastoreException(1, "", "");
    doThrow(new TransactionException(datastoreException)).when(storageTransaction).commit();
    final TransactionException expectedException = new TransactionException(datastoreException);
    doThrow(expectedException).when(storageTransaction).rollback();

    try {
      storage.runInTransaction(transactionFunction);
      fail("Expected exception!");
    } catch (TransactionException e) {
      assertFalse(e.isConflict());
      assertThat(e, is(expectedException));
    }

    verify(transactionFunction).apply(storageTransaction);
    verify(storageTransaction).rollback();
  }

  @Test
  public void runInTransactionShouldThrowIfDatastoreNewTransactionFails() throws Exception {
    Datastore datastore = mock(Datastore.class);
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO, storageTransactionFactory);
    when(datastore.newTransaction()).thenThrow(new DatastoreException(1, "", ""));

    when(transactionFunction.apply(any())).thenReturn("");

    try {
      storage.runInTransaction(transactionFunction);
      fail("Expected exception!");
    } catch (TransactionException e) {
      assertFalse(e.isConflict());
    }

    verify(transactionFunction, never()).apply(any());
  }

  @Test
  public void shouldReturnShardsForCounter() throws Exception {
    storage.runInTransaction(tx -> {
      tx.store(Shard.create(COUNTER_ID1, 0, 0));
      tx.store(Shard.create(COUNTER_ID1, 1, 3));
      return null;
    });
    final Map<Integer, Long> map = storage.shardsForCounter(COUNTER_ID1);
    assertEquals(2, map.size());
    assertEquals(0, map.get(0).longValue());
    assertEquals(3, map.get(1).longValue());
  }

  private static class FooException extends Exception {
  }

  /**
   * A test to illustrate unbounded memory usage.
   *
   * Run with -Xmx256m or some similar sane heap size. Keep an eye on the memory usage as reported by the system,
   * see it grow unboundedly.
   *
   * This behavior was discovered when running SystemTest.retriesUseLatestWorkflowSpecification()
   */
  @Test
  @Ignore("do not run automatically")
  public void testMemoryUsageUsingGet() {
    final Key key = datastore.newKeyFactory().setKind("foo").newKey("bar");
    while (true) {
      datastore.get(key);
    }
  }

  /**
   * Attempts to produce the above memory usage using the raw HttpURLConnection used by the datastore client by default.
   *
   * Does not currently seem to reproduce the issue.
   */
  @Test
  @Ignore("do not run automatically")
  public void testMemoryUsageUsingHttpUrlConnection() throws IOException, InterruptedException {
    final String host = helper.getOptions().getHost();
    final String projectId = helper.getOptions().getProjectId();
    System.err.println("namespace " + helper.getOptions().getNamespace());
    final Key key = datastore.newKeyFactory().setKind("foo").newKey("bar");
    datastore.get(key);

    final String url = "http://" + host + "/v1/projects/" + projectId + ":lookup";

    long bytesRead = 0;

    final ProtoHttpContent protoHttpContent = new ProtoHttpContent(
        LookupRequest.newBuilder().addKeys(toPb(key)).build());

    for (int i = 1;; i++) {
      try {
        URL connUrl = new URL(url);
        HttpURLConnection connection = (HttpURLConnection) connUrl.openConnection();
        connection.setRequestMethod("POST");
        connection.setDoOutput(true);
        connection.setFixedLengthStreamingMode(protoHttpContent.getLength());

        connection.addRequestProperty("accept-encoding", "gzip");
        connection.addRequestProperty("user-agent", "gcloud-java/1.0.0 Google-HTTP-Java-Client/1.22.0 (gzip)");
        connection.addRequestProperty("x-goog-api-format-version", "2");
        connection.addRequestProperty("x-goog-api-client", "gl-java/1.8.0_121 gccl/1.0.0");
        connection.addRequestProperty("content-type", "application/x-protobuf");
        connection.addRequestProperty("content-length", Long.toString(protoHttpContent.getLength()));

        protoHttpContent.writeTo(connection.getOutputStream());

        byte[] response = ByteStreams.toByteArray(connection.getInputStream());
        bytesRead += response.length;
      } catch (IOException ignore) {
      }

      if (i % 1024 == 0) {
        System.err.println("requests executed = " + i + ", bytes read = " + bytesRead);
      }
    }
  }

  static com.google.datastore.v1.Key toPb(Key key) {
    com.google.datastore.v1.Key.Builder keyPb = com.google.datastore.v1.Key.newBuilder();
    com.google.datastore.v1.PartitionId.Builder partitionIdPb =
        com.google.datastore.v1.PartitionId.newBuilder();
    partitionIdPb.setProjectId(key.getProjectId());
    partitionIdPb.setNamespaceId(key.getNamespace());
    keyPb.setPartitionId(partitionIdPb.build());

    for (PathElement pathEntry : key.getAncestors()) {
      keyPb.addPath(toPb(pathEntry));
    }
    final PathElement leaf;
    if (key.getId() != null) {
      leaf = PathElement.of(key.getKind(), key.getId());
    } else if (key.getName() != null) {
      leaf = PathElement.of(key.getKind(), key.getName());
    } else {
      throw new AssertionError();
    }
    keyPb.addPath(toPb(leaf));
    return keyPb.build();
  }

  private static com.google.datastore.v1.Key.PathElement toPb(PathElement pathEntry) {
    com.google.datastore.v1.Key.PathElement.Builder pathElementPb =
        com.google.datastore.v1.Key.PathElement.newBuilder();
    pathElementPb.setKind(pathEntry.getKind());
    if (pathEntry.getId() != null) {
      pathElementPb.setId(pathEntry.getId());
    } else if (pathElementPb.getName() != null) {
      pathElementPb.setName(pathEntry.getName());
    }
    return pathElementPb.build();
  }
}
