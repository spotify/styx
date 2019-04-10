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
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_ALL_TRIGGERED;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_COMPONENT;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_CONCURRENCY;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_END;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_HALTED;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_NEXT_TRIGGER;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_SCHEDULE;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_START;
import static com.spotify.styx.storage.DatastoreStorage.PROPERTY_WORKFLOW;
import static com.spotify.styx.storage.DatastoreStorage.globalConfigKey;
import static com.spotify.styx.storage.DatastoreStorage.instantToTimestamp;
import static com.spotify.styx.storage.DatastoreStorage.workflowKey;
import static com.spotify.styx.storage.DatastoreStorage.workflowKeyNew;
import static com.spotify.styx.storage.DatastoreStorage.workflowToEntity;
import static com.spotify.styx.testdata.TestData.EXECUTION_DESCRIPTION;
import static com.spotify.styx.testdata.TestData.FULL_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static java.util.stream.Collectors.toMap;
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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.google.common.collect.ImmutableSet;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillBuilder;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.TriggerParameters;
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
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.logging.Level;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class DatastoreStorageTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  private static final WorkflowId WORKFLOW_ID1 = WorkflowId.create("component", "endpoint1");
  private static final WorkflowId WORKFLOW_ID2 = WorkflowId.create("component", "endpoint2");
  private static final WorkflowId WORKFLOW_ID2_OLD = WorkflowId.create("component", "endpoint2_old");
  private static final WorkflowId WORKFLOW_ID3 = WorkflowId.create("component2", "pointless");

  static final WorkflowInstance WORKFLOW_INSTANCE1 = WorkflowInstance.create(WORKFLOW_ID1, "2016-09-01");
  private static final WorkflowInstance WORKFLOW_INSTANCE2 = WorkflowInstance.create(WORKFLOW_ID2, "2016-09-01");
  private static final WorkflowInstance WORKFLOW_INSTANCE3 = WorkflowInstance.create(WORKFLOW_ID3, "2016-09-01");

  private static final Resource RESOURCE1 = Resource.create("resource1", 1L);
  private static final Resource RESOURCE2 = Resource.create("resource2", 2L);

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

  static final RunState RUN_STATE2 = RunState.create(WORKFLOW_INSTANCE2, State.NEW,
      STATE_DATA, TIMESTAMP, 84L);

  static final RunState RUN_STATE3 = RunState.create(WORKFLOW_INSTANCE3, State.NEW,
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

  private static final WorkflowId WORKFLOW_ID = WorkflowId.create("dockerComp", "dockerEndpoint");

  private static final WorkflowConfiguration WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id(WORKFLOW_ID.id())
          .schedule(DAYS)
          .build();
  static final Workflow WORKFLOW = Workflow.create(WORKFLOW_ID.componentId(),
      WORKFLOW_CONFIGURATION);

  private static LocalDatastoreHelper helper;
  private DatastoreStorage storage;
  private CheckedDatastore datastore;

  @Mock TransactionFunction<String, FooException> transactionFunction;
  @Mock Function<CheckedDatastoreTransaction, DatastoreStorageTransaction> storageTransactionFactory;

  private final ExecutorService executor = Executors.newSingleThreadExecutor();

  @BeforeClass
  public static void setUpClass() throws Exception {
    final java.util.logging.Logger datastoreEmulatorLogger =
        java.util.logging.Logger.getLogger(LocalDatastoreHelper.class.getName());
    datastoreEmulatorLogger.setLevel(Level.OFF);

    // TODO: the datastore emulator behavior wrt conflicts etc differs from the real datastore
    helper = LocalDatastoreHelper.create(1.0); // 100% global consistency
    helper.start();
  }

  @AfterClass
  public static void tearDownClass() {
    if (helper != null) {
      try {
        helper.stop(org.threeten.bp.Duration.ofSeconds(30));
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    datastore = spy(new CheckedDatastore(helper.getOptions().getService()));
    storage = new DatastoreStorage(datastore, Duration.ZERO, DatastoreStorageTransaction::new, executor);
  }

  @After
  public void tearDown() throws Exception {
    helper.reset();
    storage.close();
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
    var foo = Workflow.create("foo", WORKFLOW_CONFIGURATION);
    var bar = Workflow.create("bar", WORKFLOW_CONFIGURATION);

    storage.store(foo);
    storage.store(bar);

    var fooKey = workflowKeyNew(datastore::newKeyFactory, foo.id());
    var barKey = workflowKeyNew(datastore::newKeyFactory, bar.id());

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
  public void readActiveStatesShouldPropagateIOException() throws Exception {
    final IOException cause = new IOException("foobar");
    doThrow(cause).when(datastore).query(any());
    exception.expect(is(cause));
    storage.readActiveStates();
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
  public void shouldReturnEmptyClientBlacklist() throws IOException {
    Entity config = Entity.newBuilder(DatastoreStorage.globalConfigKey(datastore.newKeyFactory()))
        .set(DatastoreStorage.PROPERTY_CONFIG_CLIENT_BLACKLIST,
            List.of()).build();
    helper.getOptions().getService().put(config);
    assertThat(storage.config().clientBlacklist(), is(empty()));
  }

  @Test
  public void shouldReturnClientBlacklist() throws IOException {
    Entity config = Entity.newBuilder(DatastoreStorage.globalConfigKey(datastore.newKeyFactory()))
        .set(DatastoreStorage.PROPERTY_CONFIG_CLIENT_BLACKLIST,
            List.of(StringValue.of("v1"), StringValue.of("v2"), StringValue.of("v3")))
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
    storage.runInTransaction(tx -> tx.storeWorkflowWithNextNaturalTrigger(workflow1, triggerSpec1));
    storage.runInTransaction(tx -> tx.storeWorkflowWithNextNaturalTrigger(workflow2, triggerSpec2));
    storage.runInTransaction(tx -> tx.storeWorkflowWithNextNaturalTrigger(workflow3, triggerSpec3));

    var workflows = storage.workflowsWithNextNaturalTrigger();
    assertThat(workflows.size(), is(3));
    assertThat(workflows, hasEntry(workflow1, triggerSpec1));
    assertThat(workflows, hasEntry(workflow2, triggerSpec2));
    assertThat(workflows, hasEntry(workflow3, triggerSpec3));
  }

  @Test
  public void shouldFailToReadCorruptWorkflow() throws Exception {
    assertThat(storage.workflows().isEmpty(), is(true));

    Workflow workflow1 = workflow(WORKFLOW_ID1);
    storage.store(workflow1);
    final Key workflowKey = workflowKeyNew(datastore::newKeyFactory, workflow1.id());

    final Entity entity = datastore.get(workflowKey);
    final Entity corrupted = Entity.newBuilder(entity)
        .set("json", "bork")
        .build();
    datastore.put(corrupted);

    exception.expect(IOException.class);
    storage.workflow(workflow1.id());
  }

  @Test
  public void shouldGetAllWorkflowsByDoingBatchGet() throws Exception {
    assertThat(storage.workflows().isEmpty(), is(true));

    final Set<WorkflowId> workflowIds = ImmutableSet.of(WORKFLOW_ID1, WORKFLOW_ID2, WORKFLOW_ID3);
    Workflow workflow1 = workflow(WORKFLOW_ID1);
    Workflow workflow2 = workflow(WORKFLOW_ID2);
    Workflow workflow2old = workflow(WORKFLOW_ID2_OLD);
    Workflow workflow3 = workflow(WORKFLOW_ID3);

    storage.store(workflow1);
    storage.store(workflow2);
    storage.store(workflow3);

    // Should not be returned as we only read _old_ keys
    // TODO: change when we start reading from _new_ keys
    storeWorkflowInOldWay(workflow2old);

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
        .globalEnabled(true)
        .debugEnabled(false)
        .executionGatingEnabled(false)
        .build();

    assertThat(storage.config(), is(expectedConfig));
  }

  @Parameters({
      "true, true",
      "false, false",
      "_, true"})
  @Test
  public void shouldStoreAndReadBackfill(String reverse, boolean withTriggerParameters) throws Exception {
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
        .schedule(DAYS).build();

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
        .set(PROPERTY_HALTED, backfill.halted());

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
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO, storageTransactionFactory, executor);
    final CheckedDatastoreTransaction transaction = datastore.newTransaction();
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
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO, storageTransactionFactory, executor);
    final CheckedDatastoreTransaction transaction = datastore.newTransaction();
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
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO, storageTransactionFactory, executor);
    final CheckedDatastoreTransaction transaction = datastore.newTransaction();
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
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO, storageTransactionFactory, executor);
    final CheckedDatastoreTransaction transaction = datastore.newTransaction();
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
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO, storageTransactionFactory, executor);
    final CheckedDatastoreTransaction transaction = datastore.newTransaction();
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
  public void runInTransactionShouldThrowTransactionExceptionOnDatastoreIOException() throws Exception {
    var storage = new DatastoreStorage(datastore, Duration.ZERO, storageTransactionFactory, executor);
    var transaction = datastore.newTransaction();
    var storageTransaction = spy(new DatastoreStorageTransaction(transaction));
    when(storageTransactionFactory.apply(any())).thenReturn(storageTransaction);

    var datastoreException = new DatastoreException(1, "", "");
    var datastoreIOException = new DatastoreIOException(datastoreException);
    when(transactionFunction.apply(any())).thenThrow(datastoreIOException);

    try {
      storage.runInTransaction(transactionFunction);
      fail("Expected exception!");
    } catch (TransactionException e) {
      assertThat(e.getCause(), is(datastoreException));
    }

    verify(transactionFunction).apply(storageTransaction);
    verify(storageTransaction).rollback();
  }

  @Test
  public void runInTransactionShouldThrowIfDatastoreNewTransactionFails() throws Exception {
    CheckedDatastore datastore = mock(CheckedDatastore.class);
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO, storageTransactionFactory, executor);
    when(datastore.newTransaction()).thenThrow(new DatastoreIOException(new DatastoreException(1, "", "")));

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
  public void shouldReturnResource() throws IOException {
    storage.runInTransaction(tx -> {
      tx.store(RESOURCE1);
      tx.store(RESOURCE2);
      return null;
    });
    assertThat(storage.getResource(RESOURCE1.id()), is(Optional.of(RESOURCE1)));
  }

  @Test
  public void shouldReturnResources() throws IOException {
    storage.runInTransaction(tx -> {
      tx.store(RESOURCE1);
      tx.store(RESOURCE2);
      return null;
    });
    assertThat(storage.getResources(), is(List.of(RESOURCE1, RESOURCE2)));
  }

  @Test
  public void shouldDeleteResource() throws IOException {
    storage.runInTransaction(tx -> {
      tx.store(RESOURCE1);
      return null;
    });
    storage.deleteResource(RESOURCE1.id());
    assertThat(storage.getResources(), is(List.of()));
    assertThat(storage.shardsForCounter(RESOURCE1.id()), is(Map.of()));
  }

  @Test
  public void shouldReturnShardsForCounter() throws Exception {
    storage.runInTransaction(tx -> {
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
    storage.runInTransaction(tx -> {
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
  public void shouldGetExceptionForUnknownCounter() throws IOException {
    exception.expect(IllegalArgumentException.class);
    exception.expectMessage("No limit found in Datastore for bar-resource");

    storage.getLimitForCounter("bar-resource");
  }

  @Test
  public void shouldStoreWorkflowsInOnlyInNewWay() throws IOException {
    var workflow = workflow(WORKFLOW_ID1);
    storage.store(workflow);
    var legacyKey = workflowKey(datastore::newKeyFactory, workflow.id());
    var newKey = workflowKeyNew(datastore::newKeyFactory, workflow.id());
    var legacyEntity = datastore.get(legacyKey);
    var newEntity = datastore.get(newKey);
    var newProperties = newEntity.getNames()
        .stream()
        .collect(toMap(key -> key, newEntity::getValue));

    assertThat(newProperties, is(notNullValue()));
    assertThat(legacyEntity, is(nullValue()));
  }


  // TODO: remove after migration
  private void storeWorkflowInOldWay(Workflow workflow) throws Exception {
    storeWorkflowInOldWay(workflow, WorkflowState.empty());
  }

  private void storeWorkflowInOldWay(Workflow workflow, WorkflowState state) throws Exception {
    final Key key = workflowKey(datastore::newKeyFactory, workflow.id());
    var entity = workflowToEntity(workflow, state, Optional.empty(), key);
    datastore.put(entity);
  }

  private static class FooException extends Exception {
  }
}
