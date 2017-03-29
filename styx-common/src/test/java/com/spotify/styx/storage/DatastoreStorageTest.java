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
import static com.spotify.styx.model.WorkflowState.patchDockerImage;
import static com.spotify.styx.testdata.TestData.FULL_WORKFLOW_CONFIGURATION;
import static com.spotify.styx.testdata.TestData.WORKFLOW_INSTANCE;
import static java.util.Collections.emptyList;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.bigtable.repackaged.com.google.common.collect.ImmutableList;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.EntityQuery;
import com.google.cloud.datastore.Key;
import com.google.cloud.datastore.KeyQuery;
import com.google.cloud.datastore.Query;
import com.google.cloud.datastore.QueryResults;
import com.google.cloud.datastore.StringValue;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.util.ResourceNotFoundException;
import com.spotify.styx.util.TriggerInstantSpec;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatastoreStorageTest {

  private static final WorkflowId WORKFLOW_ID1 = WorkflowId.create("component", "endpoint1");
  private static final WorkflowId WORKFLOW_ID2 = WorkflowId.create("component", "endpoint2");
  private static final WorkflowId WORKFLOW_ID3 = WorkflowId.create("component2", "pointless");

  private static final WorkflowInstance WORKFLOW_INSTANCE1 = WorkflowInstance.create(WORKFLOW_ID1, "2016-09-01");
  private static final WorkflowInstance WORKFLOW_INSTANCE2 = WorkflowInstance.create(WORKFLOW_ID2, "2016-09-01");
  private static final WorkflowInstance WORKFLOW_INSTANCE3 = WorkflowInstance.create(WORKFLOW_ID3, "2016-09-01");
  private static final WorkflowId WORKFLOW_ID_NO_DOCKER_IMG = WorkflowId.create("noDockerComp", "NoDockerEndpoint");
  private static final WorkflowId WORKFLOW_ID_NO_STATE = WorkflowId.create("noStateComp", "NoStateEndpoint");
  private static final WorkflowId WORKFLOW_ID_WITH_DOCKER_IMG = WorkflowId.create("dockerComp", "dockerEndpoint");

  private static final WorkflowConfiguration WORKFLOW_CONFIGURATION_EMPTY_CONF =
      WorkflowConfiguration.create(
          WORKFLOW_ID_NO_DOCKER_IMG.id(), DAYS, empty(), empty(), empty(), empty(), empty(),
          emptyList());
  private static final Optional<String> DOCKER_IMAGE = of("busybox");
  private static final String DOCKER_IMAGE_COMPONENT = "busybox:component";
  private static final String DOCKER_IMAGE_WORKFLOW = "busybox:workflow";
  private static final String COMMIT_SHA = "dcee675978b4d89e291bb695d0ca7deaf05d2a32";
  private static final WorkflowConfiguration WORKFLOW_CONFIGURATION_WITH_DOCKER_IMAGE =
      WorkflowConfiguration.create(
          WORKFLOW_ID_WITH_DOCKER_IMG.id(), DAYS, empty(), DOCKER_IMAGE, empty(), empty(), empty(),
          emptyList());
  private static final Workflow
      WORKFLOW_NO_DOCKER_IMAGE =
      Workflow.create(WORKFLOW_ID_NO_DOCKER_IMG.componentId(), URI.create("http://foo"),
                      WORKFLOW_CONFIGURATION_EMPTY_CONF);
  private static final Workflow
      WORKFLOW_NO_STATE =
      Workflow.create(WORKFLOW_ID_NO_STATE.componentId(), URI.create("http://foo"),
                      WORKFLOW_CONFIGURATION_EMPTY_CONF);
  private static final Workflow
      WORKFLOW_WITH_DOCKER_IMAGE =
      Workflow.create(WORKFLOW_ID_WITH_DOCKER_IMG.componentId(), URI.create("http://foo"),
                      WORKFLOW_CONFIGURATION_WITH_DOCKER_IMAGE);

  private static LocalDatastoreHelper helper;
  private DatastoreStorage storage;

  @BeforeClass
  public static void setUpClass() throws Exception {
    helper = LocalDatastoreHelper.create(1.0); // 100% global consistency
    helper.start();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (helper != null) {
      helper.stop();
    }
  }

  @Before
  public void setUp() throws Exception {
    Datastore datastore = helper.options().service();
    storage = new DatastoreStorage(datastore, Duration.ZERO);
  }

  @After
  public void tearDown() throws Exception {
    // clear datastore after each test
    Datastore datastore = helper.options().service();
    KeyQuery query = Query.keyQueryBuilder().build();
    final QueryResults<Key> keys = datastore.run(query);
    while (keys.hasNext()) {
      datastore.delete(keys.next());
    }
  }

  @Test
  public void shouldPersistWorkflows() throws Exception {
    Workflow workflow = Workflow.create("test", URI.create("http://foo"),
        FULL_WORKFLOW_CONFIGURATION);
    storage.store(workflow);
    Optional<Workflow> retrieved = storage.workflow(workflow.id());

    assertThat(retrieved, is(Optional.of(workflow)));
  }

  @Test
  public void shouldDeleteWorkflows() throws Exception {
    storage.store(WORKFLOW_WITH_DOCKER_IMAGE);
    storage.store(WORKFLOW_NO_DOCKER_IMAGE);

    assertThat(entitiesOfKind(DatastoreStorage.KIND_WORKFLOW), hasSize(2));

    storage.delete(WORKFLOW_WITH_DOCKER_IMAGE.id());
    assertThat(entitiesOfKind(DatastoreStorage.KIND_WORKFLOW), hasSize(1));
  }

  @Test
  public void shouldPersistNextScheduledRun() throws Exception {
    Instant instant = Instant.parse("2016-03-14T14:00:00Z");
    Instant offset = instant.plus(1, ChronoUnit.DAYS);
    TriggerInstantSpec spec = TriggerInstantSpec.create(instant, offset);

    storage.store(WORKFLOW_WITH_DOCKER_IMAGE);
    storage.updateNextNaturalTrigger(WORKFLOW_WITH_DOCKER_IMAGE.id(), spec);

    final Map<Workflow, TriggerInstantSpec> result = storage.workflowsWithNextNaturalTrigger();
    assertThat(result.values().size(), is(1));
    assertThat(result, hasEntry(WORKFLOW_WITH_DOCKER_IMAGE, spec));
  }

  @Test
  public void shouldNotRemoveWorkflowWhenSettingDockerImage() throws Exception {
    Workflow workflow = Workflow.create("test", URI.create("http://foo"),
        FULL_WORKFLOW_CONFIGURATION);
    storage.store(workflow);
    Optional<Workflow> retrieved = storage.workflow(workflow.id());
    assertThat(retrieved, is(Optional.of(workflow)));

    storage.patchState(workflow.id(), patchDockerImage(
        FULL_WORKFLOW_CONFIGURATION.dockerImage().get()));
    Optional<String> dockerImg = storage.getDockerImage(workflow.id());
    retrieved = storage.workflow(workflow.id());
    assertThat(dockerImg, is(FULL_WORKFLOW_CONFIGURATION.dockerImage()));
    assertThat(retrieved, is(Optional.of(workflow)));
  }

  @Test
  public void shouldReturnEmptyOptionalWhenWorkflowIdDoesNotExist() throws Exception {
    Optional<Workflow> retrieved = storage.workflow(WorkflowId.create("foo", "bar"));

    assertThat(retrieved, is(empty()));
  }

  @Test
  public void shouldPersistDockerImagePerWorkflow() throws Exception {
    storage.store(WORKFLOW_WITH_DOCKER_IMAGE);
    Optional<String> retrieved = storage.getDockerImage(WORKFLOW_WITH_DOCKER_IMAGE.id());

    assertThat(retrieved, is(WORKFLOW_WITH_DOCKER_IMAGE.configuration().dockerImage()));
  }

  @Test
  public void shouldPersistDockerImagePerComponent() throws Exception {
    WorkflowState state = patchDockerImage(DOCKER_IMAGE_COMPONENT);

    storage.store(Workflow.create(WORKFLOW_ID1.componentId(), URI.create("http://foo"),
                                  WORKFLOW_CONFIGURATION_EMPTY_CONF));
    storage.patchState(WORKFLOW_ID1.componentId(), state);
    Optional<String> retrieved = storage.getDockerImage(WORKFLOW_ID1);

    assertThat(retrieved, is(Optional.of(DOCKER_IMAGE_COMPONENT)));
  }

  @Test
  public void shouldPersistCommitShaPerComponent() throws Exception {
    WorkflowState state = WorkflowState.builder().commitSha(COMMIT_SHA).build();

    storage.store(Workflow.create(WORKFLOW_ID1.componentId(), URI.create("http://foo"),
                                  WORKFLOW_CONFIGURATION_EMPTY_CONF));
    storage.patchState(WORKFLOW_ID1.componentId(), state);
    WorkflowState retrieved = storage.workflowState(WORKFLOW_ID1);

    assertThat(retrieved.commitSha(), is(Optional.of(COMMIT_SHA)));
  }

  @Test
  public void shouldPersistDockerImagePerWorkflowId() throws Exception {
    storage.store(Workflow.create(
        WORKFLOW_ID1.componentId(),
        URI.create("http://foo"),
        WorkflowConfiguration
            .create(WORKFLOW_ID1.id(), DAYS, empty(), empty(), empty(), empty(), empty(), emptyList())));
    storage.patchState(WORKFLOW_ID1, patchDockerImage(DOCKER_IMAGE_WORKFLOW));
    Optional<String> retrieved = storage.getDockerImage(WORKFLOW_ID1);

    assertThat(retrieved, is(Optional.of(DOCKER_IMAGE_WORKFLOW)));
  }

  @Test
  public void shouldPersistCommitShaPerWorkflowId() throws Exception {
    storage.store(Workflow.create(
        WORKFLOW_ID1.componentId(),
        URI.create("http://foo"),
        WorkflowConfiguration
            .create(WORKFLOW_ID1.id(), DAYS, empty(), empty(), empty(), empty(), empty(), emptyList())));
    storage.patchState(WORKFLOW_ID1, WorkflowState.builder().commitSha(COMMIT_SHA).build());
    WorkflowState retrieved = storage.workflowState(WORKFLOW_ID1);

    assertThat(retrieved.commitSha(), is(Optional.of(COMMIT_SHA)));
  }


  @Test
  public void shouldReturnEmptyOptionalWhenImageDoesNotExist() throws Exception {
    storage.store(WORKFLOW_NO_DOCKER_IMAGE);
    Optional<String> retrieved = storage.getDockerImage(WORKFLOW_ID_NO_DOCKER_IMG);
    assertThat(retrieved, is(empty()));
  }

  @Test
  public void shouldReturnEmptyWorkflowStateExceptEnabledWhenWorkflowStateDoesNotExist() throws Exception {
    storage.store(WORKFLOW_NO_STATE);
    WorkflowState retrieved = storage.workflowState(WORKFLOW_ID_NO_STATE);
    assertThat(retrieved, is(WorkflowState.patchEnabled(false)));
  }

  @Test
  public void shouldNotOverwriteDockerImageFromWorkflowWhenUsingComponent() throws Exception {
    WorkflowState state = patchDockerImage(DOCKER_IMAGE_COMPONENT);

    storage.store(WORKFLOW_WITH_DOCKER_IMAGE);
    storage.patchState(WORKFLOW_WITH_DOCKER_IMAGE.id().componentId(), state);
    Optional<String> retrieved = storage.getDockerImage(WORKFLOW_WITH_DOCKER_IMAGE.id());
    assertThat(retrieved, is(Optional.of(DOCKER_IMAGE_COMPONENT)));

    storage.store(WORKFLOW_WITH_DOCKER_IMAGE);
    retrieved = storage.getDockerImage(WORKFLOW_WITH_DOCKER_IMAGE.id());
    assertThat(retrieved, is(Optional.of(DOCKER_IMAGE_COMPONENT)));
  }

  @Test
  public void shouldNotOverwriteDockerImageFromWorkflowWhenUsingWorkflowId() throws Exception {
    storage.store(WORKFLOW_WITH_DOCKER_IMAGE);
    storage.patchState(WORKFLOW_WITH_DOCKER_IMAGE.id(), patchDockerImage(DOCKER_IMAGE_WORKFLOW));
    Optional<String> retrieved = storage.getDockerImage(WORKFLOW_WITH_DOCKER_IMAGE.id());
    assertThat(retrieved, is(Optional.of(DOCKER_IMAGE_WORKFLOW)));

    storage.store(WORKFLOW_WITH_DOCKER_IMAGE);
    retrieved = storage.getDockerImage(WORKFLOW_WITH_DOCKER_IMAGE.id());
    assertThat(retrieved, is(Optional.of(DOCKER_IMAGE_WORKFLOW)));
  }

  @Test
  public void shouldNotOverwriteDockerImageFromComponentWhenUsingWorkflowId() throws Exception {
    WorkflowState state = patchDockerImage(DOCKER_IMAGE_COMPONENT);

    storage.store(WORKFLOW_WITH_DOCKER_IMAGE);
    storage.patchState(WORKFLOW_WITH_DOCKER_IMAGE.id(), patchDockerImage(DOCKER_IMAGE_WORKFLOW));
    Optional<String> retrieved = storage.getDockerImage(WORKFLOW_WITH_DOCKER_IMAGE.id());
    assertThat(retrieved, is(Optional.of(DOCKER_IMAGE_WORKFLOW)));

    storage.patchState(WORKFLOW_WITH_DOCKER_IMAGE.id().componentId(), state);
    retrieved = storage.getDockerImage(WORKFLOW_WITH_DOCKER_IMAGE.id());
    assertThat(retrieved, is(Optional.of(DOCKER_IMAGE_WORKFLOW)));
  }

  @Test(expected = ResourceNotFoundException.class)
  public void shouldNotSetDockerImageWhenComponentDoesNotExist() throws Exception {
    WorkflowState state = patchDockerImage(DOCKER_IMAGE_COMPONENT);
    storage.patchState(WORKFLOW_WITH_DOCKER_IMAGE.id().componentId(), state);
  }

  @Test(expected = ResourceNotFoundException.class)
  public void shouldNotSetDockerImageWhenWorkflowDoesNotExist() throws Exception {
    storage.patchState(WORKFLOW_WITH_DOCKER_IMAGE.id(), patchDockerImage(DOCKER_IMAGE_WORKFLOW));
  }

  @Test
  public void shouldStoreGlobalEnabledFlag() throws Exception {
    boolean old1 = storage.setGlobalEnabled(false);
    assertTrue(old1);
    assertFalse(storage.globalEnabled());

    boolean old2 = storage.setGlobalEnabled(true);
    assertFalse(old2);
    assertTrue(storage.globalEnabled());

    boolean old3 = storage.setGlobalEnabled(true);
    assertTrue(old3);
    assertTrue(storage.globalEnabled());
  }

  @Test
  public void shouldBeGlobalEnabledByDefault() throws Exception {
    boolean enabled = storage.globalEnabled();

    assertTrue(enabled);
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
    assertThat(storage.enabled(), is(Matchers.empty()));
  }

  @Test
  public void workflowShouldBeDisabledByDefault() throws Exception {
    boolean enabled = storage.enabled(WORKFLOW_ID1);

    assertFalse(enabled);
  }

  @Test
  public void shouldRetainAllWorkflowSettings() throws Exception {
    WorkflowId id = WORKFLOW_WITH_DOCKER_IMAGE.id();

    storage.store(WORKFLOW_WITH_DOCKER_IMAGE);
    storage.setEnabled(id, true);
    Optional<Workflow> workflow = storage.workflow(id);
    boolean enabled = storage.enabled(id);
    assertThat(workflow, hasValue(WORKFLOW_WITH_DOCKER_IMAGE));
    assertTrue(enabled);

    storage.setEnabled(id, false);
    workflow = storage.workflow(id);
    enabled = storage.enabled(id);
    assertThat(workflow, hasValue(WORKFLOW_WITH_DOCKER_IMAGE));
    assertFalse(enabled);

    storage.store(WORKFLOW_WITH_DOCKER_IMAGE);
    workflow = storage.workflow(id);
    enabled = storage.enabled(id);
    assertThat(workflow, hasValue(WORKFLOW_WITH_DOCKER_IMAGE));
    assertFalse(enabled);
  }

  @Test
  public void shouldWriteActiveWorkflowInstance() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE, 42L);

    List<Entity> activeInstances = entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE);
    assertThat(activeInstances, hasSize(1));

    Entity instance = activeInstances.get(0);
    assertThat(instance.getLong(DatastoreStorage.PROPERTY_COUNTER), is(42L));
    assertThat(instance.getString(DatastoreStorage.PROPERTY_COMPONENT), is(WORKFLOW_INSTANCE.workflowId().componentId()));
    assertThat(instance.getString(DatastoreStorage.PROPERTY_WORKFLOW), is(WORKFLOW_INSTANCE.workflowId().id()));
    assertThat(instance.getString(DatastoreStorage.PROPERTY_PARAMETER), is(WORKFLOW_INSTANCE.parameter()));
  }

  @Test
  public void shouldDeleteActiveWorkflowInstance() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE1, 42L);
    storage.writeActiveState(WORKFLOW_INSTANCE2, 84L);

    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(2));

    storage.deleteActiveState(WORKFLOW_INSTANCE1);
    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(1));
  }

  @Test
  public void shouldReturnAllActiveStates() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE1, 42L);
    storage.writeActiveState(WORKFLOW_INSTANCE2, 84L);

    Map<WorkflowInstance, Long> activeStates = storage.allActiveStates();
    assertThat(activeStates.entrySet(), hasSize(2));
    assertThat(activeStates, hasEntry(WORKFLOW_INSTANCE1, 42L));
    assertThat(activeStates, hasEntry(WORKFLOW_INSTANCE2, 84L));
  }

  @Test
  public void shouldReturnAllActiveStatesForAComponent() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE2, 42L);
    storage.writeActiveState(WORKFLOW_INSTANCE3, 84L);

    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(2));

    Map<WorkflowInstance, Long> activeStates = storage.activeStates(WORKFLOW_ID1.componentId());
    assertThat(activeStates.entrySet(), hasSize(1));
    assertThat(activeStates, hasEntry(WORKFLOW_INSTANCE2, 42L));
  }

  @Test
  public void shouldWriteActiveStatesWithSamePartitionAsSeparateEntities() throws Exception {
    storage.writeActiveState(WORKFLOW_INSTANCE1, 42L);
    storage.writeActiveState(WORKFLOW_INSTANCE2, 84L);

    assertThat(entitiesOfKind(DatastoreStorage.KIND_ACTIVE_WORKFLOW_INSTANCE), hasSize(2));
  }

  private List<Entity> entitiesOfKind(String kind) {
    Datastore datastore = helper.options().service();
    EntityQuery query = Query.entityQueryBuilder().kind(kind).build();
    QueryResults<Entity> keys = datastore.run(query);
    List<Entity> entities = new ArrayList<>();
    while (keys.hasNext()) {
      entities.add(keys.next());
    }
    return entities;
  }

  @Test
  public void allFieldsAreSetWhenRetrievingWorkflowState() throws Exception {
    storage.store(Workflow.create(
        WORKFLOW_ID1.componentId(),
        URI.create("http://not/important"),
        WorkflowConfiguration
            .create(WORKFLOW_ID1.id(), DAYS, empty(), empty(), empty(), empty(), empty(), emptyList())));
    Instant instant = Instant.parse("2016-03-14T14:00:00Z");
    Instant offset = instant.plus(1, ChronoUnit.DAYS);
    TriggerInstantSpec spec = TriggerInstantSpec.create(instant, offset);
    storage.updateNextNaturalTrigger(WORKFLOW_ID1, spec);
    WorkflowState state = WorkflowState.builder()
        .enabled(true)
        .dockerImage(DOCKER_IMAGE.get())
        .commitSha(COMMIT_SHA)
        .nextNaturalTrigger(instant)
        .nextNaturalOffsetTrigger(offset)
        .build();
    storage.patchState(WORKFLOW_ID1, state);

    WorkflowState retrieved = storage.workflowState(WORKFLOW_ID1);

    assertThat(retrieved, is(state));
  }

  @Test
  public void defaultGlobalDockerRunnerId() throws Exception {
    assertThat(storage.globalDockerRunnerId(), is("default"));
  }

  @Test
  public void getsGlobalDockerRunnerId() throws Exception {
    Entity config = Entity.builder(storage.globalConfigKey)
        .set(DatastoreStorage.PROPERTY_CONFIG_DOCKER_RUNNER_ID, "foobar")
        .build();
    helper.options().service().put(config);

    assertThat(storage.globalDockerRunnerId(), is("foobar"));
  }

  @Test
  public void shouldNotReturnClientBlacklist() {
    assertFalse(storage.clientBlacklist().isPresent());
  }

  @Test
  public void shouldReturnEmptyClientBlacklist() {
    Entity config = Entity.builder(storage.globalConfigKey)
        .set(DatastoreStorage.PROPERTY_CONFIG_CLIENT_BLACKLIST,
            ImmutableList.of()).build();
    helper.options().service().put(config);
    assertTrue(storage.clientBlacklist().get().isEmpty());
  }

  @Test
  public void shouldReturnClientBlacklist() {
    Entity config = Entity.builder(storage.globalConfigKey)
        .set(DatastoreStorage.PROPERTY_CONFIG_CLIENT_BLACKLIST,
            ImmutableList.of(StringValue.of("v1"), StringValue.of("v2"), StringValue.of("v3")))
        .build();
    helper.options().service().put(config);
    List<String> blacklist = storage.clientBlacklist().get();
    assertThat(blacklist.size(), is(3));
    assertThat(blacklist.get(0), is("v1"));
    assertThat(blacklist.get(1), is("v2"));
    assertThat(blacklist.get(2), is("v3"));
  }

  private Workflow workflow(WorkflowId workflowId) {
    return Workflow.create(
        workflowId.componentId(),
        URI.create("http://foo"),
        WorkflowConfiguration
            .create(workflowId.id(), HOURS, empty(), empty(), empty(), empty(), empty(), emptyList()));
  }
}
