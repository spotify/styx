/*-
 * -\-\-
 * Spotify Styx API Service
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

package com.spotify.styx.api;

import static com.spotify.apollo.test.unit.ResponseMatchers.hasHeader;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasNoPayload;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withCode;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withReasonPhrase;
import static com.spotify.styx.api.ApiVersionTestUtils.ALL_VERSIONS;
import static com.spotify.styx.api.ApiVersionTestUtils.isAtLeast;
import static com.spotify.styx.api.JsonMatchers.assertJson;
import static com.spotify.styx.api.JsonMatchers.assertNoJson;
import static com.spotify.styx.model.SequenceEvent.create;
import static com.spotify.styx.model.WorkflowState.patchDockerImage;
import static java.util.Optional.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeThat;

import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.test.ServiceHelper;
import com.spotify.styx.model.DataEndpoint;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Partitioning;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.storage.InMemStorage;
import java.net.URI;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import okio.ByteString;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class WorkflowResourceTest {

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> versions() {
    return Stream.of(ALL_VERSIONS)
        .map(v -> new Object[]{v})
        .collect(Collectors.toList());
  }

  @Rule
  public ServiceHelper serviceHelper = ServiceHelper.create(this::init, "workflow-test");

  private final Api.Version version;

  private InMemStorage storage = new InMemStorage();

  public WorkflowResourceTest(Api.Version version) {
    this.version = version;
  }

  private static final DataEndpoint DATA_ENDPOINT =
      DataEndpoint.create("bar", Partitioning.DAYS, empty(), empty(), empty());

  private static final Workflow WORKFLOW =
      Workflow.create("foo", URI.create("/hejhej"), DATA_ENDPOINT);

  private static final String VALID_SHA = "470a229b49a14e7682af2abfdac3b881a8aacdf9";
  private static final String INVALID_SHA = "XXXXXX9b49a14e7682af2abfdac3b881a8aacdf9";

  private static final ByteString STATEPAYLOAD_FULL =
      ByteString.encodeUtf8("{\"enabled\":\"true\", \"docker_image\":\"cherry:image\", "
                            + "\"commit_sha\":\"" + VALID_SHA + "\"}");

  private static final ByteString STATEPAYLOAD_ENABLED =
      ByteString.encodeUtf8("{\"enabled\":\"true\"}");

  private static final ByteString STATEPAYLOAD_VALID_SHA =
      ByteString.encodeUtf8("{\"commit_sha\":\"" + VALID_SHA + "\"}");

  private static final ByteString STATEPAYLOAD_INVALID_SHA =
      ByteString.encodeUtf8("{\"commit_sha\":\"" + INVALID_SHA + "\"}");

  private static final ByteString STATEPAYLOAD_IMAGE =
      ByteString.encodeUtf8("{\"docker_image\":\"berry:image\"}");

  private static final ByteString STATEPAYLOAD_BAD =
      ByteString.encodeUtf8("{\"The BAD\"}");

  private static final ByteString STATEPAYLOAD_OTHER_FIELD =
      ByteString.encodeUtf8("{\"enabled\":\"true\",\"other_field\":\"ignored\"}");

  private void init(Environment environment) {
    WorkflowResource workflowResource = new WorkflowResource(storage);

    environment.routingEngine().registerRoutes(workflowResource.routes());
  }

  @Before
  public void setUp() throws Exception {
    storage.store(WORKFLOW);
  }

  @Test
  public void shouldSucceedWithFullPatchStatePerWorkflow() throws Exception {
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/foo/bar/state")));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertJson(response, "enabled", equalTo(false));
    assertNoJson(response, "docker_image");
    assertNoJson(response, "commit_sha");

    response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"),
                                            STATEPAYLOAD_FULL));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertThat(response, hasHeader("Content-Type", equalTo("application/json")));
    assertJson(response, "enabled", equalTo(true));
    assertJson(response, "docker_image", equalTo("cherry:image"));
    assertJson(response, "commit_sha", equalTo(VALID_SHA));

    assertThat(storage.enabled(WORKFLOW.id()), is(true));
    assertThat(storage.workflowState(WORKFLOW.id()).dockerImage().get(), is("cherry:image"));
    assertThat(storage.workflowState(WORKFLOW.id()).commitSha().get(), is(VALID_SHA));
  }

  @Test
  public void shouldSucceedWithEnabledPatchStatePerWorkflow() throws Exception {
    storage.patchState(WORKFLOW.id(), patchDockerImage("preset:image"));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"),
                                            STATEPAYLOAD_ENABLED));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertThat(response, hasHeader("Content-Type", equalTo("application/json")));
    assertJson(response, "enabled", equalTo(true));
    assertJson(response, "docker_image", equalTo("preset:image"));

    assertThat(storage.enabled(WORKFLOW.id()), is(true));
    assertThat(storage.getDockerImage(WORKFLOW.id()), is(Optional.of("preset:image")));
  }

  @Test
  public void shouldSucceedWhenStatePayloadWithOtherFieldsIsSent() throws Exception {
    storage.patchState(WORKFLOW.id(), patchDockerImage("preset:image"));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"),
                                            STATEPAYLOAD_OTHER_FIELD));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertThat(response, hasHeader("Content-Type", equalTo("application/json")));
    assertJson(response, "enabled", equalTo(true));

    assertThat(storage.enabled(WORKFLOW.id()), is(true));
  }

  @Test
  public void shouldSucceedWithImagePatchStatePerWorkflow() throws Exception {
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"),
                                            STATEPAYLOAD_IMAGE));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertThat(response, hasHeader("Content-Type", equalTo("application/json")));
    assertJson(response, "enabled", equalTo(false));
    assertJson(response, "docker_image", equalTo("berry:image"));

    assertThat(storage.enabled(WORKFLOW.id()), is(false));
    assertThat(storage.getDockerImage(WORKFLOW.id()), is(Optional.of("berry:image")));
  }

  @Test
  public void shouldSucceedWithPatchStatePerComponent() throws Exception {
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/state"),
                                            STATEPAYLOAD_IMAGE));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertThat(response, hasHeader("Content-Type", equalTo("application/json")));
    assertNoJson(response, "enabled");
    assertJson(response, "docker_image", equalTo("berry:image"));

    assertThat(storage.enabled(WORKFLOW.id()), is(false));
    assertThat(storage.getDockerImage(WORKFLOW.id()), is(Optional.of("berry:image")));
  }

  @Test
  public void shouldReturnCurrentWorkflowState() throws Exception {
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/foo/bar/state")));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertJson(response, "enabled", equalTo(false));
    assertNoJson(response, "docker_image");
    assertNoJson(response, "commit_sha");

    storage.patchState(WORKFLOW.id(), WorkflowState.all(true, "tina:ranic", "470a229b49a14e7682af2abfdac3b881a8aacdf9"));

    response =
        awaitResponse(serviceHelper.request("GET", path("/foo/bar/state")));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertJson(response, "enabled", equalTo(true));
    assertJson(response, "docker_image", equalTo("tina:ranic"));
    assertJson(response, "commit_sha", equalTo("470a229b49a14e7682af2abfdac3b881a8aacdf9"));
  }

  @Test
  public void shouldAcceptPatchStateWithValidSHA1() throws Exception {
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"),
                                            STATEPAYLOAD_VALID_SHA));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertJson(response, "commit_sha", equalTo(VALID_SHA));

    assertThat(storage.workflowState(WORKFLOW.id()).commitSha().get(),
               is(VALID_SHA));
  }

  @Test
  public void shouldFailPatchStateWithInvalidSHA1() throws Exception {
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"),
                                            STATEPAYLOAD_INVALID_SHA));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
    assertThat(response, hasStatus(withReasonPhrase(equalTo("Invalid SHA-1."))));

    assertThat(storage.workflowState(WORKFLOW.id()).commitSha().isPresent(),
               is(false));
  }

  @Test @Ignore
  public void shouldReturnBadRequestOnEnableWhenWorkflowNotFound() throws Exception {
    // can't implement
    // this can't ever happen in the current bigtable storage implementation
  }

  @Test @Ignore
  public void shouldReturnBadRequestOnImageWhenWorkflowNotFound() throws Exception {
  }

  @Test @Ignore
  public void shouldReturnBadRequestOnImageWhenComponentNotFound() throws Exception {
  }

  @Test
  public void shouldReturnBadRequestWhenMalformedStatePayloadIsSent() throws Exception {
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state"),
                                            STATEPAYLOAD_BAD));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
    assertThat(response, hasNoPayload());
    assertThat(response, hasStatus(withReasonPhrase(equalTo("Invalid payload."))));
  }

  @Test
  public void shouldReturnBadRequestWhenMalformedStatePayloadIsSentComponent() throws Exception {
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/state"),
                                            STATEPAYLOAD_BAD));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
    assertThat(response, hasNoPayload());
    assertThat(response, hasStatus(withReasonPhrase(equalTo("Invalid payload."))));
  }

  @Test
  public void shouldReturnBadRequestWhenNoPayloadIsSent() throws Exception {
    Response<ByteString> response =
    awaitResponse(serviceHelper.request("PATCH", path("/foo/bar/state")));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
    assertThat(response, hasNoPayload());
    assertThat(response, hasStatus(withReasonPhrase(equalTo("Missing payload."))));
  }

  /**
   * This test should be removed when we actually support this
   */
  @Test
  public void shouldReturnBadRequestSettingEnabledOnComponent() throws Exception {
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PATCH", path("/foo/state"),
                                            STATEPAYLOAD_FULL));

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
    assertThat(response, hasNoPayload());
    assertThat(response, hasStatus(withReasonPhrase(equalTo("Enabled flag not supported for components."))));
  }

  @Test
  public void shouldReturnWorkflowInstancesData() throws Exception {
    assumeThat(version, isAtLeast(Api.Version.V1));

    WorkflowInstance wfi = WorkflowInstance.create(WORKFLOW.id(), "2016-08-10");
    storage.writeEvent(create(Event.triggerExecution(wfi, "trig"), 0L, ms("07:00:00")));
    storage.writeEvent(create(Event.created(wfi, "exec", "img"), 1L, ms("07:00:01")));
    storage.writeEvent(create(Event.started(wfi), 2L, ms("07:00:02")));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/foo/bar/instances")));

    assertThat(response, hasStatus(withCode(Status.OK)));

    assertJson(response, "[*]", hasSize(1));
    assertJson(response, "[0].workflow_instance.parameter", is("2016-08-10"));
    assertJson(response, "[0].workflow_instance.workflow_id.component_id", is("foo"));
    assertJson(response, "[0].workflow_instance.workflow_id.endpoint_id", is("bar"));
    assertJson(response, "[0].triggers", hasSize(1));
    assertJson(response, "[0].triggers.[0].trigger_id", is("trig"));
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
    assumeThat(version, isAtLeast(Api.Version.V1));

    WorkflowInstance wfi = WorkflowInstance.create(WORKFLOW.id(), "2016-08-10");
    storage.writeEvent(create(Event.triggerExecution(wfi, "trig"), 0L, ms("07:00:00")));
    storage.writeEvent(create(Event.created(wfi, "exec", "img"), 1L, ms("07:00:01")));
    storage.writeEvent(create(Event.started(wfi), 2L, ms("07:00:02")));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/foo/bar/instances/2016-08-10")));

    assertThat(response, hasStatus(withCode(Status.OK)));

    assertJson(response, "workflow_instance.parameter", is("2016-08-10"));
    assertJson(response, "workflow_instance.workflow_id.component_id", is("foo"));
    assertJson(response, "workflow_instance.workflow_id.endpoint_id", is("bar"));
    assertJson(response, "triggers", hasSize(1));
    assertJson(response, "triggers.[0].trigger_id", is("trig"));
    assertJson(response, "triggers.[0].timestamp", is("2016-08-10T07:00:00Z"));
    assertJson(response, "triggers.[0].complete", is(false));
    assertJson(response, "triggers.[0].executions", hasSize(1));
    assertJson(response, "triggers.[0].executions.[0].execution_id", is("exec"));
    assertJson(response, "triggers.[0].executions.[0].docker_image", is("img"));
    assertJson(response, "triggers.[0].executions.[0].statuses", hasSize(2));
    assertJson(response, "triggers.[0].executions.[0].statuses.[0].status", is("SUBMITTED"));
    assertJson(response, "triggers.[0].executions.[0].statuses.[1].status", is("STARTED"));
    assertJson(response, "triggers.[0].executions.[0].statuses.[0].timestamp", is("2016-08-10T07:00:01Z"));
    assertJson(response, "triggers.[0].executions.[0].statuses.[1].timestamp", is("2016-08-10T07:00:02Z"));
  }

  private Response<ByteString> awaitResponse(CompletionStage<Response<ByteString>> completionStage)
      throws InterruptedException, java.util.concurrent.ExecutionException,
             java.util.concurrent.TimeoutException {
    return completionStage.toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  private String path(String path) {
    return version.prefix() + WorkflowResource.BASE + path;
  }

  private long ms(String time) {
    return Instant.parse("2016-08-10T" + time + "Z").toEpochMilli();
  }
}
