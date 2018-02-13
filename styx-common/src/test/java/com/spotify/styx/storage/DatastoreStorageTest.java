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

import com.google.bigtable.repackaged.com.google.common.collect.ImmutableList;
import com.google.bigtable.repackaged.com.google.common.collect.ImmutableMap;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreException;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.Transaction;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.ExecutionDescription;
import com.spotify.styx.model.StyxConfig;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfiguration.Secret;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.serialization.PersistentWorkflowInstanceState;
import com.spotify.styx.state.Message;
import com.spotify.styx.state.Message.MessageLevel;
import com.spotify.styx.state.RunState.State;
import com.spotify.styx.state.StateData;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.util.TriggerInstantSpec;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DatastoreStorageTest {

  private static final WorkflowId WORKFLOW_ID1 = WorkflowId.create("component", "endpoint1");
  private static final WorkflowId WORKFLOW_ID2 = WorkflowId.create("component", "endpoint2");
  private static final WorkflowId WORKFLOW_ID3 = WorkflowId.create("component2", "pointless");

  private static final WorkflowInstance WORKFLOW_INSTANCE1 = WorkflowInstance.create(WORKFLOW_ID1, "2016-09-01");
  private static final WorkflowInstance WORKFLOW_INSTANCE2 = WorkflowInstance.create(WORKFLOW_ID2, "2016-09-01");
  private static final WorkflowInstance WORKFLOW_INSTANCE3 = WorkflowInstance.create(WORKFLOW_ID3, "2016-09-01");

  private static final Instant TIMESTAMP = Instant.parse("2017-01-01T00:00:00Z");


  private static final PersistentWorkflowInstanceState PERSISTENT_STATE1 = PersistentWorkflowInstanceState.builder()
      .state(State.NEW)
      .data(StateData.zero())
      .timestamp(TIMESTAMP)
      .counter(42L)
      .build();

  private static final PersistentWorkflowInstanceState PERSISTENT_STATE2 = PersistentWorkflowInstanceState.builder()
      .state(State.NEW)
      .data(StateData.zero())
      .timestamp(TIMESTAMP)
      .counter(84L)
      .build();

  private static final PersistentWorkflowInstanceState PERSISTENT_STATE3 = PersistentWorkflowInstanceState.builder()
      .state(State.NEW)
      .data(StateData.zero())
      .timestamp(TIMESTAMP)
      .counter(17L)
      .build();


  private static final PersistentWorkflowInstanceState PERSISTENT_STATE = PersistentWorkflowInstanceState.builder()
      .state(State.NEW)
      .data(StateData.zero())
      .timestamp(TIMESTAMP)
      .counter(42L)
      .build();

  private static final PersistentWorkflowInstanceState FULL_PERSISTENT_STATE = PersistentWorkflowInstanceState.builder()
      .state(State.QUEUED)
      .timestamp(TIMESTAMP)
      .counter(42L)
      .data(StateData.newBuilder()
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
          .addMessage(Message.info("foo"))
          .addMessage(Message.warning("bar"))
          .addMessage(Message.error("baz"))
          .addMessage(Message.create(MessageLevel.UNKNOWN, "quux"))
          .build())
      .build();


  private static final WorkflowId WORKFLOW_ID = WorkflowId.create("dockerComp", "dockerEndpoint");

  private static final WorkflowConfiguration WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id(WORKFLOW_ID.id())
          .schedule(DAYS)
          .build();
  private static final Workflow WORKFLOW = Workflow.create(WORKFLOW_ID.componentId(),
                                                           WORKFLOW_CONFIGURATION);

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
    storage.writeActiveState(WORKFLOW_INSTANCE, PERSISTENT_STATE);

    List<Entity> activeInstances = entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE);
    assertThat(activeInstances, hasSize(1));

    Entity instance = activeInstances.get(0);
    assertThat(instance.getLong(DatastoreStorage.PROPERTY_COUNTER), is(PERSISTENT_STATE.counter()));
    assertThat(instance.getString(DatastoreStorage.PROPERTY_COMPONENT), is(WORKFLOW_INSTANCE.workflowId().componentId()));
    assertThat(instance.getString(DatastoreStorage.PROPERTY_WORKFLOW), is(WORKFLOW_INSTANCE.workflowId().id()));
    assertThat(instance.getString(DatastoreStorage.PROPERTY_PARAMETER), is(WORKFLOW_INSTANCE.parameter()));
  }

  @Test
  public void testFullPersistentStatePersistence() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE, FULL_PERSISTENT_STATE);
    final PersistentWorkflowInstanceState read = storage
        .activeStates(WORKFLOW_INSTANCE.workflowId().componentId())
        .get(WORKFLOW_INSTANCE);
    assertThat(read, is(FULL_PERSISTENT_STATE));
  }

  @Test
  public void shouldDeleteActiveWorkflowInstance() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE1, PERSISTENT_STATE1);
    storage.writeActiveState(WORKFLOW_INSTANCE2, PERSISTENT_STATE2);

    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(2));

    storage.deleteActiveState(WORKFLOW_INSTANCE1);
    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(1));
  }

  @Test
  public void shouldReturnAllActiveStates() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE1, PERSISTENT_STATE1);
    storage.writeActiveState(WORKFLOW_INSTANCE2, PERSISTENT_STATE2);

    final Map<WorkflowInstance, PersistentWorkflowInstanceState> activeStates = storage.allActiveStates();
    assertThat(activeStates, is(ImmutableMap.of(
        WORKFLOW_INSTANCE1, PERSISTENT_STATE1,
        WORKFLOW_INSTANCE2, PERSISTENT_STATE2)));
  }

  @Test
  public void shouldReturnAllActiveStatesForAComponent() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE2, PERSISTENT_STATE2);
    storage.writeActiveState(WORKFLOW_INSTANCE3, PERSISTENT_STATE3);

    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(2));

    final Map<WorkflowInstance, PersistentWorkflowInstanceState> activeStates =
        storage.activeStates(WORKFLOW_ID1.componentId());

    assertThat(activeStates, is(ImmutableMap.of(WORKFLOW_INSTANCE2, PERSISTENT_STATE2)));
  }

  @Test
  public void shouldWriteActiveStatesWithSamePartitionAsSeparateEntities() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE1, PERSISTENT_STATE1);
    storage.writeActiveState(WORKFLOW_INSTANCE2, PERSISTENT_STATE2);

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
  public void testDefaultConfig() throws Exception {
    final StyxConfig expectedConfig = StyxConfig.newBuilder()
        .globalEnabled(true)
        .globalDockerRunnerId("default")
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

    final TransactionException expectedException = new TransactionException(true, null);
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

    doThrow(new TransactionException(true, null)).when(storageTransaction).commit();
    final TransactionException expectedException = new TransactionException(false, null);
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
  public void insertActiveStateShouldFailIfAlreadyExists() throws Exception {
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO);
    storage.runInTransaction(tx -> tx.insertActiveState(WORKFLOW_INSTANCE1, PERSISTENT_STATE1));
    try {
      storage.runInTransaction(tx -> tx.insertActiveState(WORKFLOW_INSTANCE1, PERSISTENT_STATE1));
      fail("Expected exception!");
    } catch (TransactionException e) {
      assertThat(e.isAlreadyExists(), is(true));
    }
  }

  @Test
  public void updateActiveStateShouldFailIfNotAlreadyExists() throws Exception {
    final DatastoreStorage storage = new DatastoreStorage(datastore, Duration.ZERO);
    try {
      storage.runInTransaction(tx -> tx.updateActiveState(WORKFLOW_INSTANCE1, PERSISTENT_STATE1));
      fail("Expected exception!");
    } catch (TransactionException e) {
      assertThat(e.isNotFound(), is(true));
    }
  }

  private static class FooException extends Exception {
  }
}
