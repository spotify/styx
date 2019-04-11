/*-
 * -\-\-
 * Spotify Styx API Service
 * --
 * Copyright (C) 2017 Spotify AB
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

package com.spotify.styx.api;

import static com.spotify.apollo.Status.FORBIDDEN;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasHeader;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasNoPayload;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withCode;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withReasonPhrase;
import static com.spotify.styx.api.JsonMatchers.assertJson;
import static com.spotify.styx.model.SequenceEvent.create;
import static com.spotify.styx.serialization.Json.deserialize;
import static com.spotify.styx.serialization.Json.serialize;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.google.common.base.Throwables;
import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.styx.api.workflow.WorkflowInitializationException;
import com.spotify.styx.api.workflow.WorkflowInitializer;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowConfigurationBuilder;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.BigtableMocker;
import com.spotify.styx.storage.BigtableStorage;
import com.spotify.styx.storage.TransactionFunction;
import com.spotify.styx.util.ParameterUtil;
import com.spotify.styx.util.TriggerUtil;
import com.spotify.styx.util.WorkflowValidator;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.logging.Level;
import okio.ByteString;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class WorkflowResourceTest extends VersionedApiTest {

  private static final TriggerParameters TRIGGER_PARAMETERS = TriggerParameters.builder()
      .env("FOO", "foo",
          "BAR", "bar")
      .build();

  private static LocalDatastoreHelper localDatastore;

  private Datastore datastore = localDatastore.getOptions().getService();
  private Connection bigtable = setupBigTableMockTable();
  private AggregateStorage storage;
  private AggregateStorage rawStorage;

  @Mock private WorkflowValidator workflowValidator;
  @Mock private WorkflowInitializer workflowInitializer;
  @Mock private BiConsumer<Optional<Workflow>, Optional<Workflow>> workflowConsumer;
  @Mock private WorkflowActionAuthorizer workflowActionAuthorizer;
  @Mock private GoogleIdToken idToken;
  @Mock private RequestAuthenticator requestAuthenticator;

  private static final String SERVICE_ACCOUNT = "foo@bar.iam.gserviceaccount.com";

  private static final WorkflowConfiguration WORKFLOW_CONFIGURATION =
      WorkflowConfiguration.builder()
          .id("bar")
          .schedule(Schedule.DAYS)
          .commitSha("00000ef508c1cb905e360590ce3e7e9193f6b370")
          .dockerImage("bar-dummy:dummy")
          .serviceAccount(SERVICE_ACCOUNT)
          .env("FOO", "foo", "BAR", "bar")
          .runningTimeout(Duration.parse("PT23H"))
          .build();

  private static final Workflow WORKFLOW =
      Workflow.create("foo", WORKFLOW_CONFIGURATION);

  private static final Workflow EXISTING_WORKFLOW =
      Workflow.create("foo", WorkflowConfigurationBuilder
          .from(WORKFLOW_CONFIGURATION)
          .dockerImage("earlier:image")
          .build());

  private static final Trigger NATURAL_TRIGGER = Trigger.natural();
  private static final Trigger BACKFILL_TRIGGER = Trigger.backfill("backfill-1");

  private static final ByteString STATEPAYLOAD_FULL =
      ByteString.encodeUtf8("{\"enabled\":\"true\", "
                            + "\"next_natural_trigger\":\"2016-08-10T07:00:01Z\", "
                            + "\"next_natural_offset_trigger\":\"2016-08-10T08:00:01Z\"}");

  private static final ByteString STATEPAYLOAD_ENABLED =
      ByteString.encodeUtf8("{\"enabled\":\"true\"}");

  private static final ByteString STATEPAYLOAD_OTHER_FIELD =
      ByteString.encodeUtf8("{\"enabled\":\"true\",\"other_field\":\"ignored\"}");

  private static final ByteString BAD_JSON =
      ByteString.encodeUtf8("{\"The BAD\"}");

  public WorkflowResourceTest(Api.Version version) {
    super("/workflows", version, "workflow-test");
    MockitoAnnotations.initMocks(this);
  }

  @Override
  protected void init(Environment environment) {
    rawStorage = new AggregateStorage(bigtable, datastore, Duration.ZERO);
    storage = spy(rawStorage);
    when(workflowValidator.validateWorkflow(any())).thenReturn(Collections.emptyList());
    when(requestAuthenticator.authenticate(any())).thenReturn(() -> Optional.of(idToken));
    WorkflowResource workflowResource =
        new WorkflowResource(storage, workflowValidator, workflowInitializer, workflowConsumer,
            workflowActionAuthorizer);

    environment.routingEngine()
        .registerRoutes(workflowResource.routes(requestAuthenticator).map(r ->
            r.withMiddleware(Middlewares.exceptionAndRequestIdHandler())));
  }

  @BeforeClass
  public static void setUpClass() throws Exception {
    final java.util.logging.Logger datastoreEmulatorLogger =
        java.util.logging.Logger.getLogger(LocalDatastoreHelper.class.getName());
    datastoreEmulatorLogger.setLevel(Level.OFF);

    localDatastore = LocalDatastoreHelper.create(1.0); // 100% global consistency
    localDatastore.start();
  }

  @AfterClass
  public static void tearDownClass() {
    if (localDatastore != null) {
      try {
        localDatastore.stop(org.threeten.bp.Duration.ofSeconds(30));
      } catch (Throwable e) {
        e.printStackTrace();
      }
    }
  }

  @Before
  public void setUp() throws Exception {
    storage.storeWorkflow(WORKFLOW);
  }

  @After
  public void tearDown() throws Exception {
    localDatastore.reset();
    serviceHelper.stubClient().clear();
  }

  @Test
  public void patchShouldFailWithForbiddenIfNotAuthorized() throws Exception {
    sinceVersion(Api.Version.V3);

    doThrow(new ResponseException(Response.forStatus(FORBIDDEN)))
        .when(workflowActionAuthorizer).authorizeWorkflowAction(any(), any(WorkflowId.class));

    final Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"), STATEPAYLOAD_FULL));

    assertThat(response, hasStatus(withCode(FORBIDDEN)));

    verify(storage, never()).patchState(any(), any());
  }

  @Test
  public void shouldSucceedWithFullPatchStatePerWorkflow() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/foo/bar/state")));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertJson(response, "enabled", equalTo(false));

    response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"),
                                            STATEPAYLOAD_FULL));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertThat(response, hasHeader("Content-Type", equalTo("application/json")));
    assertJson(response, "enabled", equalTo(true));
    assertJson(response, "next_natural_trigger", equalTo("2016-08-10T07:00:01Z"));
    assertJson(response, "next_natural_offset_trigger", equalTo("2016-08-10T08:00:01Z"));

    final WorkflowState workflowState = storage.workflowState(WORKFLOW.id());
    assertThat(workflowState.enabled().get(), is(true));
    assertThat(workflowState.nextNaturalTrigger().get().toString(),
               equalTo("2016-08-10T07:00:01Z"));
    assertThat(workflowState.nextNaturalOffsetTrigger().get().toString(),
               equalTo("2016-08-10T08:00:01Z"));
  }

  @Test
  public void shouldSucceedWithEnabledPatchStatePerWorkflow() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"),
                                            STATEPAYLOAD_ENABLED));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertThat(response, hasHeader("Content-Type", equalTo("application/json")));
    assertJson(response, "enabled", equalTo(true));

    assertThat(storage.enabled(WORKFLOW.id()), is(true));
  }

  @Test
  public void shouldSucceedWhenStatePayloadWithOtherFieldsIsSent() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"),
                                            STATEPAYLOAD_OTHER_FIELD));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertJson(response, "enabled", equalTo(true));

    assertThat(storage.enabled(WORKFLOW.id()), is(true));
  }

  @Test
  public void shouldFailOnCommitShaInPatch() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"),
                                            ByteString.encodeUtf8("{\"commit_sha\": \"foobar\"}")));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
  }

  @Test
  public void shouldFailOnDockerImageInPatch() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"),
                                            ByteString.encodeUtf8("{\"docker_image\": \"foobar\"}")));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
  }

  @Test
  public void shouldFailOnCommitShaAndDockerImageInPatch() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"),
                                            ByteString.encodeUtf8("{\"commit_sha\": \"foobar\","
                                                                  + "\"docker_image\": \"foobar\"}")));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
  }

  @Test
  public void shouldReturnCurrentWorkflowState() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/foo/bar/state")));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertJson(response, "enabled", equalTo(false));

    storage.patchState(WORKFLOW.id(),
                       WorkflowState.patchEnabled(true));

    response =
        awaitResponse(serviceHelper.request("GET", path("/foo/bar/state")));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertJson(response, "enabled", equalTo(true));
  }

  @Test
  public void shouldReturnBadRequestWhenMalformedStatePayloadIsSent() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"),
                                            BAD_JSON));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
    assertThat(response, hasNoPayload());
    assertThat(response, hasStatus(withReasonPhrase(equalTo("Invalid payload."))));
  }

  @Test
  public void shouldReturnBadRequestWhenNoPayloadIsSent() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state")));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
    assertThat(response, hasNoPayload());
    assertThat(response, hasStatus(withReasonPhrase(equalTo("Missing payload."))));
  }

  @Test
  public void shouldReturnWorkflowInstancesData() throws Exception {
    sinceVersion(Api.Version.V3);

    WorkflowInstance wfi = WorkflowInstance.create(WORKFLOW.id(), "2016-08-10");
    storage.writeEvent(create(Event.triggerExecution(wfi, NATURAL_TRIGGER, TRIGGER_PARAMETERS), 0L, ms("07:00:00")));
    storage.writeEvent(create(Event.created(wfi, "exec", "img"), 1L, ms("07:00:01")));
    storage.writeEvent(create(Event.started(wfi), 2L, ms("07:00:02")));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/foo/bar/instances")));

    assertThat(response, hasStatus(withCode(Status.OK)));

    assertJson(response, "[*]", hasSize(1));
    assertJson(response, "[0].workflow_instance.parameter", is("2016-08-10"));
    assertJson(response, "[0].workflow_instance.workflow_id.component_id", is("foo"));
    assertJson(response, "[0].workflow_instance.workflow_id.id", is("bar"));
    assertJson(response, "[0].triggers", hasSize(1));
    assertJson(response, "[0].triggers.[0].trigger_id", is(TriggerUtil.NATURAL_TRIGGER_ID));
    assertJson(response, "[0].triggers.[0].complete", is(false));
    assertJson(response, "[0].triggers.[0].executions", hasSize(1));
    assertJson(response, "[0].triggers.[0].executions.[0].execution_id", is("exec"));
    assertJson(response, "[0].triggers.[0].executions.[0].docker_image", is("img"));
    assertJson(response, "[0].triggers.[0].executions.[0].statuses", hasSize(2));
    assertJson(response, "[0].triggers.[0].executions.[0].statuses.[0].status", is("SUBMITTED"));
    assertJson(response, "[0].triggers.[0].executions.[0].statuses.[1].status", is("STARTED"));
  }

  @Test
  public void shouldReturnWorkflowRangeOfInstancesData() throws Exception {
    sinceVersion(Api.Version.V3);

    WorkflowInstance wfi = WorkflowInstance.create(WORKFLOW.id(), "2016-08-10");
    storage.writeEvent(create(Event.triggerExecution(wfi, NATURAL_TRIGGER, TRIGGER_PARAMETERS), 0L, ms("07:00:00")));
    storage.writeEvent(create(Event.created(wfi, "exec", "img"), 1L, ms("07:00:01")));
    storage.writeEvent(create(Event.started(wfi), 2L, ms("07:00:02")));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/foo/bar/instances?start=2016-08-10")));

    assertThat(response, hasStatus(withCode(Status.OK)));

    assertJson(response, "[*]", hasSize(1));
    assertJson(response, "[0].workflow_instance.parameter", is("2016-08-10"));
    assertJson(response, "[0].workflow_instance.workflow_id.component_id", is("foo"));
    assertJson(response, "[0].workflow_instance.workflow_id.id", is("bar"));
    assertJson(response, "[0].triggers", hasSize(1));
    assertJson(response, "[0].triggers.[0].trigger_id", is(TriggerUtil.NATURAL_TRIGGER_ID));
    assertJson(response, "[0].triggers.[0].complete", is(false));
    assertJson(response, "[0].triggers.[0].executions", hasSize(1));
    assertJson(response, "[0].triggers.[0].executions.[0].execution_id", is("exec"));
    assertJson(response, "[0].triggers.[0].executions.[0].docker_image", is("img"));
    assertJson(response, "[0].triggers.[0].executions.[0].statuses", hasSize(2));
    assertJson(response, "[0].triggers.[0].executions.[0].statuses.[0].status", is("SUBMITTED"));
    assertJson(response, "[0].triggers.[0].executions.[0].statuses.[1].status", is("STARTED"));
  }

  @Test
  public void shouldReturnWorkflowInstanceData() throws Exception {
    sinceVersion(Api.Version.V3);

    WorkflowInstance wfi = WorkflowInstance.create(WORKFLOW.id(), "2016-08-10");
    storage.writeEvent(create(Event.triggerExecution(wfi, NATURAL_TRIGGER, TRIGGER_PARAMETERS), 0L, ms("07:00:00")));
    storage.writeEvent(create(Event.created(wfi, "exec", "img"), 1L, ms("07:00:01")));
    storage.writeEvent(create(Event.started(wfi), 2L, ms("07:00:02")));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/foo/bar/instances/2016-08-10")));

    assertThat(response, hasStatus(withCode(Status.OK)));

    assertJson(response, "workflow_instance.parameter", is("2016-08-10"));
    assertJson(response, "workflow_instance.workflow_id.component_id", is("foo"));
    assertJson(response, "workflow_instance.workflow_id.id", is("bar"));
    assertJson(response, "triggers", hasSize(1));
    assertJson(response, "triggers.[0].trigger_id", is(TriggerUtil.NATURAL_TRIGGER_ID));
    assertJson(response, "triggers.[0].timestamp", is("2016-08-10T07:00:00Z"));
    assertJson(response, "triggers.[0].complete", is(false));
    assertJson(response, "triggers.[0].executions", hasSize(1));
    assertJson(response, "triggers.[0].executions.[0].execution_id", is("exec"));
    assertJson(response, "triggers.[0].executions.[0].docker_image", is("img"));
    assertJson(response, "triggers.[0].executions.[0].statuses", hasSize(2));
    assertJson(response, "triggers.[0].executions.[0].statuses.[0].status", is("SUBMITTED"));
    assertJson(response, "triggers.[0].executions.[0].statuses.[1].status", is("STARTED"));
    assertJson(response, "triggers.[0].executions.[0].statuses.[0].timestamp",
               is("2016-08-10T07:00:01Z"));
    assertJson(response, "triggers.[0].executions.[0].statuses.[1].timestamp",
               is("2016-08-10T07:00:02Z"));
  }

  @Test
  public void shouldReturn500WhenFailedToGetWorkflowInstanceData() throws Exception {
    sinceVersion(Api.Version.V3);

    WorkflowInstance wfi = WorkflowInstance.create(WORKFLOW.id(), "2016-08-10");
    doThrow(new IOException()).when(storage).executionData(wfi);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/foo/bar/instances/2016-08-10")));

    assertThat(response, hasStatus(withCode(Status.INTERNAL_SERVER_ERROR)));
  }

  @Test
  public void shouldReturn404WhenWorkflowNotFound() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/foo/bar/instances/2016-08-10")));

    assertThat(response, hasStatus(withCode(Status.NOT_FOUND)));
  }

  @Test
  public void shouldReturnWorkflowInstanceDataBackfill() throws Exception {
    sinceVersion(Api.Version.V3);

    WorkflowInstance wfi = WorkflowInstance.create(WORKFLOW.id(), "2016-08-10");
    storage.writeEvent(create(Event.triggerExecution(wfi, BACKFILL_TRIGGER, TRIGGER_PARAMETERS), 0L, ms("07:00:00")));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/foo/bar/instances/2016-08-10")));

    assertThat(response, hasStatus(withCode(Status.OK)));

    assertJson(response, "workflow_instance.parameter", is("2016-08-10"));
    assertJson(response, "workflow_instance.workflow_id.component_id", is("foo"));
    assertJson(response, "workflow_instance.workflow_id.id", is("bar"));
    assertJson(response, "triggers", hasSize(1));
    assertJson(response, "triggers.[0].trigger_id", is("backfill-1"));
    assertJson(response, "triggers.[0].timestamp", is("2016-08-10T07:00:00Z"));
    assertJson(response, "triggers.[0].complete", is(false));
  }

  @Test
  public void shouldPaginateWorkflowInstancesData() throws Exception {
    sinceVersion(Api.Version.V3);

    WorkflowInstance wfi1 = WorkflowInstance.create(WORKFLOW.id(), "2016-08-11");
    WorkflowInstance wfi2 = WorkflowInstance.create(WORKFLOW.id(), "2016-08-12");
    WorkflowInstance wfi3 = WorkflowInstance.create(WORKFLOW.id(), "2016-08-13");
    storage.writeEvent(create(Event.triggerExecution(wfi1, NATURAL_TRIGGER, TRIGGER_PARAMETERS), 0L, ms("07:00:00")));
    storage.writeEvent(create(Event.triggerExecution(wfi2, NATURAL_TRIGGER, TRIGGER_PARAMETERS), 0L, ms("07:00:00")));
    storage.writeEvent(create(Event.triggerExecution(wfi3, NATURAL_TRIGGER, TRIGGER_PARAMETERS), 0L, ms("07:00:00")));

    Response<ByteString> response = awaitResponse(
        serviceHelper.request("GET", path("/foo/bar/instances?offset=2016-08-12&limit=1")));

    assertThat(response, hasStatus(withCode(Status.OK)));

    assertJson(response, "[*]", hasSize(1));
    assertJson(response, "[0].workflow_instance.parameter", is("2016-08-12"));
  }

  @Test
  public void shouldTailPaginateWorkflowInstancesData() throws Exception {
    sinceVersion(Api.Version.V3);

    // Set the next natural trigger
    final WorkflowState workflowState = WorkflowState.builder()
        .nextNaturalTrigger(ParameterUtil.parseDate("2016-08-14"))
        .build();
    storage.patchState(WORKFLOW.id(), workflowState);

    WorkflowInstance wfi1 = WorkflowInstance.create(WORKFLOW.id(), "2016-08-11");
    WorkflowInstance wfi2 = WorkflowInstance.create(WORKFLOW.id(), "2016-08-12");
    WorkflowInstance wfi3 = WorkflowInstance.create(WORKFLOW.id(), "2016-08-13");
    storage.writeEvent(create(Event.triggerExecution(wfi1, NATURAL_TRIGGER, TRIGGER_PARAMETERS), 0L, ms("07:00:00")));
    storage.writeEvent(create(Event.triggerExecution(wfi2, NATURAL_TRIGGER, TRIGGER_PARAMETERS), 0L, ms("07:00:00")));
    storage.writeEvent(create(Event.triggerExecution(wfi3, NATURAL_TRIGGER, TRIGGER_PARAMETERS), 0L, ms("07:00:00")));

    Response<ByteString> response = awaitResponse(
        serviceHelper.request("GET", path("/foo/bar/instances?limit=2&tail=true")));

    assertThat(response, hasStatus(withCode(Status.OK)));

    assertJson(response, "[*]", hasSize(2));
    assertJson(response, "[0].workflow_instance.parameter", is("2016-08-12"));
    assertJson(response, "[1].workflow_instance.parameter", is("2016-08-13"));
  }

  @Test
  public void shouldReturnEmptyNotFoundForTailWithNoNextNaturalTrigger() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response = awaitResponse(
        serviceHelper.request("GET", path("/foo/bar/instances?limit=2&tail=true")));

    assertThat(response, hasStatus(withCode(Status.NOT_FOUND)));
  }

  @Test
  public void shouldReturnNotFoundWhenTailUnknownWorkflowInstancesData() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response = awaitResponse(
        serviceHelper.request("GET", path("/bar/foo/instances?limit=2&tail=true")));

    assertThat(response, hasStatus(withCode(Status.NOT_FOUND)));
    assertThat(response, hasNoPayload());
    assertThat(response, hasStatus(withReasonPhrase(equalTo("Could not find workflow."))));
  }

  @Test
  public void shouldReturnBadRequestWhenNoPayloadIsSentWorkflow() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path("/foo")));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
    assertThat(response, hasNoPayload());
    assertThat(response, hasStatus(withReasonPhrase(equalTo("Missing payload."))));
  }

  @Test
  public void shouldReturnBadRequestWhenMalformedStatePayloadIsSentWorkflow() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path("/foo"),
                                            BAD_JSON));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
    assertThat(response, hasNoPayload());
    assertThat(response, hasStatus(withReasonPhrase(
        startsWith("Invalid payload. Unexpected character ('}' (code 125)): "
                   + "was expecting a colon to separate field name and value"))));
  }

  @Test
  public void createWorkflowShouldFailWithForbiddenIfNotAuthorized() throws Exception {
    sinceVersion(Api.Version.V3);

    doThrow(new ResponseException(Response.forStatus(FORBIDDEN)))
        .when(workflowActionAuthorizer).authorizeWorkflowAction(any(), any(Workflow.class));

    final Response<ByteString> response = awaitResponse(
        serviceHelper.request("POST", path("/foo"), serialize(WORKFLOW_CONFIGURATION)));

    assertThat(response, hasStatus(withCode(FORBIDDEN)));

    verify(storage, never()).patchState(any(), any());

    verifyZeroInteractions(workflowValidator);
    verifyZeroInteractions(workflowInitializer);
    verifyZeroInteractions(workflowConsumer);
  }

  @Test
  public void deleteWorkflowShouldFailWithForbiddenIfNotAuthorized() throws Exception {
    sinceVersion(Api.Version.V3);

    doThrow(new ResponseException(Response.forStatus(FORBIDDEN)))
        .when(workflowActionAuthorizer).authorizeWorkflowAction(any(), any(Workflow.class));

    final Response<ByteString> response = awaitResponse(serviceHelper.request("DELETE", path("/foo/bar")));

    assertThat(response, hasStatus(withCode(FORBIDDEN)));

    verify(storage, never()).delete(WORKFLOW.id());
  }

  @Test
  public void shouldCreateWorkflow() throws Exception {
    sinceVersion(Api.Version.V3);

    when(workflowInitializer.store(eq(WORKFLOW), any())).thenReturn(Optional.empty());

    Response<ByteString> response =
        awaitResponse(
            serviceHelper
                .request("POST", path("/foo"), serialize(WORKFLOW_CONFIGURATION)));

    verify(workflowValidator).validateWorkflow(WORKFLOW);
    verify(workflowInitializer).store(eq(WORKFLOW), any());
    verify(workflowConsumer).accept(Optional.empty(), Optional.of(WORKFLOW));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertThat(deserialize(response.payload().get(), Workflow.class), equalTo(WORKFLOW));
  }

  @Test
  public void shouldUpdateWorkflow() throws Exception {
    sinceVersion(Api.Version.V3);

    when(workflowInitializer.store(eq(WORKFLOW), any())).thenReturn(Optional.of(EXISTING_WORKFLOW));

    Response<ByteString> response =
        awaitResponse(
            serviceHelper
                .request("POST", path("/foo"), serialize(WORKFLOW_CONFIGURATION)));

    verify(workflowValidator).validateWorkflow(WORKFLOW);
    verify(workflowInitializer).store(eq(WORKFLOW), any());
    verify(workflowConsumer).accept(Optional.of(EXISTING_WORKFLOW), Optional.of(WORKFLOW));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertThat(deserialize(response.payload().get(), Workflow.class), equalTo(WORKFLOW));
  }

  @Test
  public void shouldFailToUpdateWorkflowIfNotAuthorized() throws Exception {
    sinceVersion(Api.Version.V3);

    when(workflowInitializer.store(eq(WORKFLOW), any())).then(a -> {
      final Consumer<Optional<Workflow>> guard = a.getArgument(1);
      guard.accept(Optional.of(EXISTING_WORKFLOW));
      throw new AssertionError("Should not reach here");
    });

    doThrow(new ResponseException(Response.forStatus(FORBIDDEN)))
        .when(workflowActionAuthorizer).authorizeWorkflowAction(any(), eq(EXISTING_WORKFLOW));

    final Response<ByteString> response = awaitResponse(
        serviceHelper.request("POST", path("/foo"), serialize(WORKFLOW_CONFIGURATION)));

    verify(workflowInitializer).store(eq(WORKFLOW), any());

    assertThat(response.status().code(), is(FORBIDDEN.code()));
  }

  @Test
  public void shouldReturnErrorMessageWhenFailedToStore() throws Exception {
    sinceVersion(Api.Version.V3);

    when(workflowInitializer.store(eq(WORKFLOW), any()))
        .thenThrow(new WorkflowInitializationException(new Exception()));

    Response<ByteString> response =
        awaitResponse(
            serviceHelper
                .request("POST", path("/foo"), serialize(WORKFLOW_CONFIGURATION)));

    verify(workflowValidator).validateWorkflow(WORKFLOW);

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
  }

  @Test
  public void shouldDeleteWorkflow() throws Exception {
    sinceVersion(Api.Version.V3);
    var response = awaitResponse(serviceHelper.request("DELETE", path("/foo/bar")));
    assertThat(response, hasStatus(withCode(Status.NO_CONTENT)));
    assertThat(response, hasNoPayload());
    assertThat(storage.workflow(WORKFLOW.id()), is(Optional.empty()));
    verify(workflowConsumer).accept(Optional.of(WORKFLOW), Optional.empty());
  }

  @Test
  public void shouldReturnErrorWhenDeleteNonexistWorkflow() throws Exception {
    sinceVersion(Api.Version.V3);
    var response = awaitResponse(serviceHelper.request("DELETE", path("/non/existent")));
    assertThat(response, hasStatus(withCode(Status.NOT_FOUND)));
  }

  @Test
  public void shouldReturnErrorWhenFailedToGetWorkflow() throws Exception {
    sinceVersion(Api.Version.V3);
    doThrow(new IOException()).when(storage).workflow(WORKFLOW.id());
    var response = awaitResponse(serviceHelper.request("GET", path("/foo/bar")));
    assertThat(response, hasStatus(withCode(Status.INTERNAL_SERVER_ERROR)));
    verify(storage, never()).delete(WORKFLOW.id());
  }

  @Test
  public void shouldReturnErrorWhenFailedToDeleteWorkflow() throws Exception {
    sinceVersion(Api.Version.V3);

    doThrow(new IOException()).when(storage).delete(WORKFLOW.id());

    when(storage.runInTransactionWithRetries(any())).thenAnswer(a -> {
      TransactionFunction<Object, Exception> tf = a.getArgument(0);
      return rawStorage.runInTransactionWithRetries(tx -> {
        when(tx.deleteWorkflow(any())).thenThrow(new IOException());
        return tf.apply(tx);
      });
    });

    var response = awaitResponse(serviceHelper.request("DELETE", path("/foo/bar")));

    assertThat(response, hasStatus(withCode(Status.INTERNAL_SERVER_ERROR)));
  }

  @Test
  public void shouldFailInvalidWorkflowImage() throws Exception {
    sinceVersion(Api.Version.V3);

    when(workflowValidator.validateWorkflow(any())).thenReturn(List.of("bad", "image"));

    Response<ByteString> response = awaitResponse(serviceHelper
        .request("POST", path("/foo"), serialize(WORKFLOW_CONFIGURATION)));

    verify(workflowValidator).validateWorkflow(Workflow.create("foo", WORKFLOW_CONFIGURATION));

    assertThat(serviceHelper.stubClient().sentRequests(), is(empty()));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
  }

  @Test
  public void shouldReturnWorkflows() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeWorkflow(Workflow.create("other_component", WORKFLOW_CONFIGURATION));

    Response<ByteString> response = awaitResponse(
        serviceHelper.request("GET", path("")));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertJson(response, "[*]", hasSize(2));
  }

  @Test
  public void shouldReturnWorkflowsInComponent() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeWorkflow(Workflow.create("other_component", WORKFLOW_CONFIGURATION));

    Response<ByteString> response = awaitResponse(
        serviceHelper.request("GET", path("/foo")));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertJson(response, "[*]", hasSize(1));
    assertJson(response, "[0].component_id", is("foo"));
  }

  private long ms(String time) {
    return Instant.parse("2016-08-10T" + time + "Z").toEpochMilli();
  }

  private Connection setupBigTableMockTable() {
    Connection bigtable = mock(Connection.class);
    try {
      new BigtableMocker(bigtable)
          .setNumFailures(0)
          .setupTable(BigtableStorage.EVENTS_TABLE_NAME)
          .finalizeMocking();
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
    return bigtable;
  }
}
