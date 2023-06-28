/*
 * -\-\-
 * Spotify Styx Scheduler Service
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

package com.spotify.styx.client;

import static com.spotify.styx.client.OkHttpTestUtil.APPLICATION_JSON;
import static com.spotify.styx.client.OkHttpTestUtil.bytesOfRequestBody;
import static com.spotify.styx.client.OkHttpTestUtil.response;
import static com.spotify.styx.client.OkHttpTestUtil.responseBuilder;
import static com.spotify.styx.client.StyxOkHttpClient.STYX_API_VERSION;
import static com.spotify.styx.util.StringIsValidUuid.isValidUuid;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.BackfillsPayload;
import com.spotify.styx.api.EventsPayload;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.api.TestServiceAccountUsageAuthorizationRequest;
import com.spotify.styx.api.TestServiceAccountUsageAuthorizationResponse;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.EditableBackfillInput;
import com.spotify.styx.model.Event;
import com.spotify.styx.model.Resource;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.TriggerRequest;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.WorkflowWithState;
import com.spotify.styx.model.data.EventInfo;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.serialization.Json;
import io.grpc.Context;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import okhttp3.HttpUrl;
import okhttp3.Request;
import okhttp3.ResponseBody;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class StyxOkHttpClientTest {

  private static final WorkflowConfiguration WORKFLOW_CONFIGURATION_1 = WorkflowConfiguration.builder()
      .id("bar-wf_1")
      .dockerImage("busybox")
      .dockerArgs(Arrays.asList("echo", "hello world"))
      .schedule(Schedule.DAYS)
      .build();

  private static final WorkflowConfiguration WORKFLOW_CONFIGURATION_2 = WorkflowConfiguration.builder()
      .id("bar-wf_2")
      .dockerImage("busybox")
      .dockerArgs(Arrays.asList("echo", "hello world"))
      .schedule(Schedule.DAYS)
      .build();

  private static final Workflow WORKFLOW_1 = Workflow.create("f[ ]o-cmp", WORKFLOW_CONFIGURATION_1);

  private static final Workflow WORKFLOW_2 = Workflow.create("f[ ]o-cmp", WORKFLOW_CONFIGURATION_2);

  private static final Instant START = Instant.parse("2017-01-01T00:00:00Z");
  private static final Instant END = Instant.parse("2017-01-30T00:00:00Z");

  private static final BackfillInput BACKFILL_INPUT =
      BackfillInput.newBuilder()
      .start(START)
      .end(END)
      .component("f[ ]o-cmp")
      .workflow("bar-w[f]")
      .concurrency(1)
      .build();

  private static final Backfill BACKFILL = Backfill.newBuilder()
      .id("backfill-2")
      .start(START)
      .end(END)
      .workflowId(WorkflowId.create("f[ ]o-cmp", "bar-w[f]"))
      .concurrency(1)
      .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
      .schedule(Schedule.DAYS)
      .build();

  private static final HttpUrl API_URL = new HttpUrl.Builder()
      .scheme("https").host("foo.bar")
      .addPathSegment("api").addPathSegment(STYX_API_VERSION).build();

  private static final String CLIENT_HOST = API_URL.scheme() + "://" + API_URL.host() ;

  @Mock FutureOkHttpClient client;
  @Mock GoogleIdTokenAuth auth;

  private StyxClient styx;

  @Captor ArgumentCaptor<Request> requestCaptor;

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(auth.getToken(any())).thenReturn(Optional.of("foobar"));
    styx = StyxOkHttpClient.create(CLIENT_HOST, client, auth);
  }

  @Test
  @Parameters({
      "foo.bar, https://foo.bar",
      "foo.bar:80, https://foo.bar",
      "foo.bar:17, https://foo.bar:17",
      "http://foo.bar, http://foo.bar",
      "http://foo.bar:80, http://foo.bar",
      "http://foo.bar:17, http://foo.bar:17",
      "https://foo.bar, https://foo.bar",
      "https://foo.bar:443, https://foo.bar",
      "https://foo.bar:17, https://foo.bar:17",
  })
  public void testHosts(String host, String expectedUriPrefix) {
    when(client.send(any(Request.class))).thenReturn(
        CompletableFuture.completedFuture(response(200)));
    styx = StyxOkHttpClient.create(host, client, auth);

    styx.resourceList();
    verify(client, timeout(30_000)).send(requestCaptor.capture());

    final Request request = requestCaptor.getValue();
    assertThat(request.url().toString(), startsWith(expectedUriPrefix));
  }

  @Test
  public void shouldGetAllWorkflows() throws Exception {
    final List<Workflow> workflows = Arrays.asList(WORKFLOW_1, WORKFLOW_2);
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, workflows)));
    final CompletableFuture<List<Workflow>> r = styx.workflows().toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/workflows");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("GET"));
    assertThat(r.join(), is(workflows));
  }

  @Test
  public void shouldGetAllWorkflowsOfAComponent() throws Exception {
    final List<Workflow> workflows = Arrays.asList(WORKFLOW_1, WORKFLOW_2);
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, workflows)));
    final String componentId = WORKFLOW_1.componentId();
    final CompletableFuture<List<Workflow>> r = styx.workflows(componentId).toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final HttpUrl url = API_URL.newBuilder().addPathSegment("workflows")
        .addPathSegment(componentId).build();
    assertThat(request.url().toString(), is(url.uri().toString()));
    assertThat(request.method(), is("GET"));
    assertThat(r.join(), is(workflows));
  }

  @Test
  public void shouldGetBackfill() throws Exception {
    final BackfillPayload payload = BackfillPayload.create(BACKFILL, Optional.empty());
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, payload)));
    final CompletableFuture<BackfillPayload> r =
        styx.backfill("backfill", false).toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/backfills/backfill?status=false");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("GET"));
    assertThat(r.join(), is(payload));
  }

  @Test
  public void shouldGetBackfills() throws Exception {
    final BackfillsPayload payload = BackfillsPayload.create(
        List.of(BackfillPayload.create(BACKFILL, Optional.empty())));
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, payload)));
    final CompletableFuture<BackfillsPayload> r =
        styx.backfillList(Optional.of("component"), Optional.of("workflow"), false, false).toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/backfills?component=component&workflow=workflow&showAll=false&status=false");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("GET"));
    assertThat(r.join(), is(payload));
  }

  @Test
  public void shouldGetBackfillsWithStartFilter() throws Exception {
    final BackfillsPayload payload = BackfillsPayload.create(
        List.of(BackfillPayload.create(BACKFILL, Optional.empty())));
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, payload)));

    LocalDate date = LocalDate.parse("2015-01-01");
    Instant instant = date.atStartOfDay(ZoneId.of("UTC")).toInstant();

    System.out.println(instant);
    final CompletableFuture<BackfillsPayload> r =
        styx.backfillList(Optional.of("component"), Optional.of("workflow"), false, false, Optional.of(instant)).toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/backfills?component=component&workflow=workflow&showAll=false&status=false&start="+ date);
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("GET"));
    assertThat(r.join(), is(payload));
  }

  @Test
  public void shouldHaltBackfill() {
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK)));
    final CompletableFuture<Void> r =
        styx.backfillHalt("backfill").toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/backfills/backfill?graceful=false");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("DELETE"));
  }

  @Test
  public void shouldHaltBackfillGracefullly() {
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK)));
    final CompletableFuture<Void> r =
        styx.backfillHalt("backfill", true).toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/backfills/backfill?graceful=true");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("DELETE"));
  }

  @Test
  public void shouldGetResource() throws Exception {
    final Resource resource = Resource.create("resource", 3);
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, resource)));
    final CompletableFuture<Resource> r =
        styx.resource("resource").toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/resources/resource");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("GET"));
    assertThat(r.join(), is(resource));
  }

  @Test
  public void shouldEditResource() throws Exception {
    final Resource resource = Resource.create("resource", 3);
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, resource)));
    final CompletableFuture<Resource> r =
        styx.resourceEdit("resource", 3).toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/resources/resource");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("PUT"));
    assertThat(Json.deserialize(bytesOfRequestBody(request), Resource.class), is(resource));
    assertThat(r.join(), is(resource));
  }

  @Test
  public void shouldCreateResource() throws Exception {
    final Resource resource = Resource.create("resource", 3);
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK,
                                                               resource)));
    final CompletableFuture<Resource> r =
        styx.resourceCreate("resource", 3).toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/resources");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("POST"));
    assertThat(Json.deserialize(bytesOfRequestBody(request), Resource.class), is(resource));
    assertThat(r.join(), is(resource));
  }

  @Test
  public void shouldRetryWorkflow() throws Exception {
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, WorkflowState.empty())));
    final CompletableFuture<Void> r =
        styx.retryWorkflowInstance("component", "workflow", "2017-01-01T00").toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/scheduler/retry");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("POST"));
    assertThat(Json.deserialize(bytesOfRequestBody(request), WorkflowInstance.class),
               is(WorkflowInstance.create(WorkflowId.create("component", "workflow"), "2017-01-01T00")));
  }

  @Test
  public void shouldHaltWorkflow() throws Exception {
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, WorkflowState.empty())));
    final CompletableFuture<Void> r =
        styx.haltWorkflowInstance("component", "workflow", "2017-01-01T00").toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/scheduler/halt");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("POST"));
    assertThat(Json.deserialize(bytesOfRequestBody(request), WorkflowInstance.class),
               is(WorkflowInstance.create(WorkflowId.create("component", "workflow"), "2017-01-01T00")));
  }

  @Test
  public void shouldGetWorkflowState() throws Exception {
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, WorkflowState.empty())));
    final CompletableFuture<WorkflowState> r =
        styx.workflowState("component", "workflow").toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/workflows/component/workflow/state");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("GET"));
    assertThat(r.join(), is(WorkflowState.empty()));
  }

  @Test
  public void shouldGetWorkflowWithState() throws Exception {
    var workflowWithState = WorkflowWithState.create(WORKFLOW_1, WorkflowState.empty());
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, workflowWithState)));
    var r = styx.workflowWithState("component", "workflow").toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    var request = requestCaptor.getValue();
    var uri = URI.create(API_URL + "/workflows/component/workflow/full");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("GET"));
    assertThat(r.join(), is(workflowWithState));
  }

  @Test
  public void shouldGetEventsForWorkflowInstance() throws Exception {
    final EventsPayload events = EventsPayload.create(List.of(
        EventsPayload.TimestampedEvent
            .create(Event.stop(WorkflowInstance.create(WORKFLOW_1.id(), "foo")), 123)));
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, events)));
    final CompletableFuture<List<EventInfo>> r =
        styx.eventsForWorkflowInstance("component", "workflow", "2017-01-01T00").toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/status/events/component/workflow/2017-01-01T00");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("GET"));
    assertThat(r.join(), is(List.of(EventInfo.create(123, "stop", ""))));
  }

  @Test
  public void shouldFailWithBadEventJson() {
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(responseBuilder(HTTP_OK)
            .body(ResponseBody.create(APPLICATION_JSON, "{invalid json is invalid".getBytes(UTF_8))).build()));
    final CompletableFuture<List<EventInfo>> r =
        styx.eventsForWorkflowInstance("component", "workflow", "2017-01-01T00").toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    assertThat(r.isCompletedExceptionally(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/status/events/component/workflow/2017-01-01T00");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("GET"));
    thrown.expectCause(hasMessage(is("Invalid json returned from API")));
    r.join();
  }

  @Test
  public void shouldWorkWithUnknownEvents() throws Exception {
    final EventsPayload events = EventsPayload.create(List.of(
        EventsPayload.TimestampedEvent
            .create(Event.stop(WorkflowInstance.create(WORKFLOW_1.id(), "foo")), 123)));
    final String badJson = Json.serialize(events).utf8().replace("stop", "stahp");
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(
            responseBuilder(HTTP_OK).body(
                ResponseBody.create(APPLICATION_JSON, badJson.getBytes(UTF_8)))
                .build()));
    final CompletableFuture<List<EventInfo>> r =
        styx.eventsForWorkflowInstance("component", "workflow", "2017-01-01T00").toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/status/events/component/workflow/2017-01-01T00");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("GET"));
    assertThat(r.join(), is(List.of(EventInfo.create(123, "stahp", ""))));
  }

  @Test
  public void shouldGetActiveStatesForComponent() throws Exception {
    final RunStateDataPayload runStateDataPayload = RunStateDataPayload.create(List.of());
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, runStateDataPayload)));
    final CompletableFuture<RunStateDataPayload> r =
        styx.activeStates(Optional.of("component")).toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/status/activeStates?component=component");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("GET"));
    assertThat(r.join(), is(runStateDataPayload));
  }

  @Test
  public void shouldGetWorkflowInstance() throws Exception {
    final WorkflowInstanceExecutionData workflowInstanceExecutionData =
        WorkflowInstanceExecutionData.create(
            WorkflowInstance.create(
                WorkflowId.create("component", "workflow"),
                "2017-01-01T00"),
            Collections.emptyList());
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, workflowInstanceExecutionData)));
    final CompletableFuture<WorkflowInstanceExecutionData> r =
        styx.workflowInstanceExecutions("component", "workflow", "2017-01-01T00")
            .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/workflows/component/workflow/instances/2017-01-01T00");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("GET"));
    assertThat(r.join(), is(workflowInstanceExecutionData));
  }

  @Test
  public void deleteWorkflow() {
    when(client.send(any(Request.class))).thenReturn(
        CompletableFuture.completedFuture(response(HTTP_NO_CONTENT)));
    final CompletableFuture<Void> r = styx.deleteWorkflow("f[ ]o-cmp", "bar-w[f]")
        .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    assertThat(r.isCompletedExceptionally(), is(false));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/workflows/f%5B%20%5Do-cmp/bar-w%5Bf%5D");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(request.method(), is("DELETE"));
  }

  @Test
  public void componentAndWorkflowAreEncoded() {
    when(client.send(any(Request.class))).thenReturn(
        CompletableFuture.completedFuture(response(HTTP_OK)));
    final CompletableFuture<Workflow> r = styx.workflow("f[ ]o-cmp", "bar-w[f]")
        .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(
        API_URL + "/workflows/f%5B%20%5Do-cmp/bar-w%5Bf%5D");
    assertThat(request.url().toString(), is(uri.toString()));
  }

  @Test
  public void createOrUpdateWorkflow() throws Exception {
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, WORKFLOW_1)));
    final CompletableFuture<Workflow> r = styx.createOrUpdateWorkflow("f[ ]o-cmp",
        WORKFLOW_CONFIGURATION_1).toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/workflows/f%5B%20%5Do-cmp");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(Json.deserialize(bytesOfRequestBody(request), WorkflowConfiguration.class),
        is(WORKFLOW_CONFIGURATION_1));
    assertThat(request.method(), is("POST"));
  }

  @Test
  public void shouldCreateBackfill() throws Exception {
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, BACKFILL)));
    final CompletableFuture<Backfill> r = styx.backfillCreate("f[ ]o-cmp", "bar-w[f]",
        "2017-01-01T00:00:00Z", "2017-01-30T00:00:00Z", 1)
        .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/backfills?allowFuture=false");
    assertThat(request.url().toString(), is(uri.toString()));
    assertThat(Json.deserialize(bytesOfRequestBody(request), BackfillInput.class),
        equalTo(BACKFILL_INPUT));
    assertThat(request.method(), is("POST"));
  }

  @Test
  public void shouldCreateBackfillFromInput() throws Exception {
    final BackfillInput backfillInput = BACKFILL_INPUT.builder()
        .reverse(true)
        .build();

    when(client.send(any(Request.class))).thenReturn(CompletableFuture.completedFuture(
        response(HTTP_OK, BACKFILL)));
    final CompletableFuture<Backfill> r = styx.backfillCreate(backfillInput)
        .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.url().toString(), is(API_URL + "/backfills?allowFuture=false"));
    assertThat(Json.deserialize(bytesOfRequestBody(request), BackfillInput.class),
        equalTo(backfillInput));
    assertThat(r.isCompletedExceptionally(), is(false));
    assertThat(request.method(), is("POST"));
  }

  @Test
  public void shouldCreateBackfillWithDescription() throws Exception {
    final BackfillInput backfillInput = BACKFILL_INPUT.builder()
        .description("Description")
        .build();
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, BACKFILL)));
    final CompletableFuture<Backfill> r = styx.backfillCreate("f[ ]o-cmp", "bar-w[f]",
                                                              "2017-01-01T00:00:00Z",
                                                              "2017-01-30T00:00:00Z", 1,
                                                              "Description")
        .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.url().toString(), is(API_URL + "/backfills?allowFuture=false"));
    assertThat(Json.deserialize(bytesOfRequestBody(request), BackfillInput.class),
               equalTo(backfillInput));
    assertThat(request.method(), is("POST"));
  }

  @Test
  public void shouldCreateBackfillAllowFuture() throws Exception {
    final BackfillInput backfillInput = BACKFILL_INPUT.builder()
        .reverse(true)
        .build();

    when(client.send(any(Request.class))).thenReturn(CompletableFuture.completedFuture(
        response(HTTP_OK, BACKFILL)));
    final CompletableFuture<Backfill> r = styx.backfillCreate(backfillInput, true)
        .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.url().toString(), is(API_URL + "/backfills?allowFuture=true"));
    assertThat(Json.deserialize(bytesOfRequestBody(request), BackfillInput.class),
        equalTo(backfillInput));
    assertThat(r.isCompletedExceptionally(), is(false));
    assertThat(request.method(), is("POST"));
  }

  @Test
  public void shouldUpdateBackfillConcurrency() throws Exception {
    final EditableBackfillInput backfillInput = EditableBackfillInput.newBuilder()
        .id(BACKFILL.id())
        .concurrency(4)
        .build();
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(
            response(HTTP_OK, BACKFILL.builder().concurrency(4).build())));
    final CompletableFuture<Backfill> r = styx.backfillEditConcurrency(BACKFILL.id(), 4)
        .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.url().toString(), is(API_URL + "/backfills/" + BACKFILL.id()));
    assertThat(Json.deserialize(bytesOfRequestBody(request), EditableBackfillInput.class),
        equalTo(backfillInput));
    assertThat(request.method(), is("PUT"));
  }

  @Test
  public void shouldUpdateWorkflowState() throws Exception {
    final WorkflowState workflowState = WorkflowState.builder()
        .enabled(true)
        .nextNaturalTrigger(Instant.parse("2017-01-03T21:00:00Z"))
        .nextNaturalTrigger(Instant.parse("2017-01-03T22:00:00Z"))
        .build();
    when(client.send(any(Request.class))).thenReturn(CompletableFuture.completedFuture(
        response(HTTP_OK, workflowState)));
    final CompletableFuture<WorkflowState> r =
        styx.updateWorkflowState("f[ ]o-cmp", "bar-w[f]", workflowState).toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.url().toString(), is(API_URL + "/workflows/f%5B%20%5Do-cmp/bar-w%5Bf%5D/state"));
    assertThat(Json.deserialize(bytesOfRequestBody(request), WorkflowState.class), is(workflowState));
    assertThat(request.method(), is("PATCH"));
  }

  @Test
  public void testTokenFromContext() throws Exception {
    when(auth.getToken(any())).thenReturn(Optional.empty());
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK)));
    var r = Context.current().withValue(GrpcContextKey.AUTHORIZATION_KEY, "foobar")
        .call(() ->
            styx.triggerWorkflowInstance("foo", "bar", "baz").toCompletableFuture());
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    var request = requestCaptor.getValue();
    assertThat(request.header("Authorization"), is("Bearer foobar"));
  }

  @Test
  public void testTokenSuccess() {
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK)));
    final CompletableFuture<Void> r =
        styx.triggerWorkflowInstance("foo", "bar", "baz").toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.header("Authorization"), is("Bearer foobar"));
  }

  @Test
  public void testTokenFailure() throws Exception {
    final IOException rootCause = new IOException("netsplit!");
    when(auth.getToken(any())).thenThrow(rootCause);
    thrown.expect(ClientErrorException.class);
    thrown.expectMessage("Authentication failure: " + rootCause.getMessage());
    thrown.expectCause(is(rootCause));
    styx.triggerWorkflowInstance("foo", "bar", "baz");
  }

  @Test
  public void testUnathorizedMissingCredentialsApiError() throws Exception {
    when(auth.getToken(any())).thenReturn(Optional.empty());
    when(client.send(any()))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_FORBIDDEN)));

    try {
      styx.triggerWorkflowInstance("foo", "bar", "baz").toCompletableFuture().get();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(ApiErrorException.class));
      ApiErrorException apiErrorException = (ApiErrorException) e.getCause();
      assertThat(apiErrorException.isAuthenticated(), is(false));
    }
  }

  @Test
  public void testUnauthorizedWithCredentialsApiError() throws Exception {
    when(client.send(any()))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_FORBIDDEN)));

    try {
      styx.triggerWorkflowInstance("foo", "bar", "baz").toCompletableFuture().get();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(ApiErrorException.class));
      ApiErrorException apiErrorException = (ApiErrorException) e.getCause();
      assertThat(apiErrorException.isAuthenticated(), is(true));
    }
  }

  @Test
  public void testSendsRequestId() throws Exception {
    when(client.send(requestCaptor.capture())).thenReturn(CompletableFuture.completedFuture(
        response(HTTP_OK, Collections.emptyList())));
    styx.workflows();
    final Request request = requestCaptor.getValue();
    assertThat(request.header("X-Styx-Request-Id"), isValidUuid());
  }

  @Test
  public void testUsesServerRequestIdOnMismatch() throws Exception {
    final String responseRequestId = "foobar";
    when(client.send(any())).thenReturn(CompletableFuture.completedFuture(
        responseBuilder(HTTP_INTERNAL_ERROR, Collections.emptyList())
            .addHeader("X-Styx-Request-Id", responseRequestId).build()));

    try {
      styx.workflows().toCompletableFuture().get();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(ApiErrorException.class));
      final ApiErrorException apiError = (ApiErrorException) e.getCause();
      assertThat(apiError.getRequestId(), is(responseRequestId));
    }
  }

  @Test
  public void testUsesClientRequestIdOnNoResponseRequestId() throws Exception {
    when(client.send(requestCaptor.capture()))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_INTERNAL_ERROR, Collections.emptyList())));

    try {
      styx.workflows().toCompletableFuture().get();
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause(), instanceOf(ApiErrorException.class));
      final ApiErrorException apiError = (ApiErrorException) e.getCause();
      final Request request = requestCaptor.getValue();
      assertThat(apiError.getRequestId(), is(request.header("X-Styx-Request-Id")));
    }
  }

  @Test
  public void testTriggerWorkflowInstance() throws IOException {
    final TriggerRequest triggerRequest = TriggerRequest.of(WORKFLOW_1.id(), "2017-01-01",
        TriggerParameters.zero());
    when(client.send(any(Request.class))).thenReturn(CompletableFuture.completedFuture(
        response(HTTP_OK)));
    final CompletableFuture<Void> r =
        styx.triggerWorkflowInstance(WORKFLOW_1.componentId(), WORKFLOW_1.workflowId(),
            "2017-01-01").toCompletableFuture();

    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.url().toString(), is(API_URL + "/scheduler/trigger?allowFuture=false"));
    assertThat(request.method(), is("POST"));
    assertThat(Json.deserialize(bytesOfRequestBody(request), TriggerRequest.class),
               equalTo(triggerRequest));
  }

  @Test
  public void testTriggerWorkflowInstanceAllowFuture() throws IOException {
    final TriggerRequest triggerRequest = TriggerRequest.of(WORKFLOW_1.id(), "2017-01-01",
        TriggerParameters.zero());
    when(client.send(any(Request.class))).thenReturn(CompletableFuture.completedFuture(
        response(HTTP_OK)));
    final CompletableFuture<Void> r =
        styx.triggerWorkflowInstance(WORKFLOW_1.componentId(), WORKFLOW_1.workflowId(),
            "2017-01-01").toCompletableFuture();

    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.url().toString(), is(API_URL + "/scheduler/trigger?allowFuture=false"));
    assertThat(request.method(), is("POST"));
    assertThat(Json.deserialize(bytesOfRequestBody(request), TriggerRequest.class),
        equalTo(triggerRequest));
  }

  @Test
  public void testTriggerWorkflowInstanceWithTriggerParametersAllowFuture() throws IOException {
    final TriggerParameters triggerParameters = TriggerParameters.builder()
        .env("FOO", "BAR", "BAR", "FOO")
        .build();
    final TriggerRequest triggerRequest =
        TriggerRequest.of(WORKFLOW_1.id(), "2017-01-01", triggerParameters);
    when(client.send(any(Request.class)))
        .thenReturn(CompletableFuture.completedFuture(response(HTTP_OK)));
    final CompletableFuture<Void> r =
        styx.triggerWorkflowInstance(WORKFLOW_1.componentId(), WORKFLOW_1.workflowId(),
            "2017-01-01", triggerParameters, true).toCompletableFuture();

    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.url().toString(), is(API_URL + "/scheduler/trigger?allowFuture=true"));
    assertThat(request.method(), is("POST"));
    assertThat(Json.deserialize(bytesOfRequestBody(request), TriggerRequest.class),
        equalTo(triggerRequest));
  }

  @Test
  public void shouldTestServiceAccountUsageAuthorization() throws Exception {
    final String serviceAccount = "foo@bar.iam.gserviceaccount.com";
    final String principal = "baz@example.com";

    final TestServiceAccountUsageAuthorizationResponse expectedResponse = TestServiceAccountUsageAuthorizationResponse
        .builder()
        .message("foo")
        .serviceAccount(serviceAccount)
        .principal(principal)
        .build();

    when(client.send(any())).thenReturn(CompletableFuture.completedFuture(response(HTTP_OK, expectedResponse)));
    final TestServiceAccountUsageAuthorizationResponse response =
        styx.testServiceAccountUsageAuthorization(serviceAccount, principal).toCompletableFuture().get(30, SECONDS);

    verify(client).send(requestCaptor.capture());
    final Request request = requestCaptor.getValue();
    assertThat(request.url().toString(), is(API_URL + "/status/testServiceAccountUsageAuthorization"));
    assertThat(request.method(), is("POST"));
    final TestServiceAccountUsageAuthorizationRequest requestPayload =
        Json.deserialize(bytesOfRequestBody(request), TestServiceAccountUsageAuthorizationRequest.class);
    assertThat(requestPayload.serviceAccount(), is(serviceAccount));
    assertThat(requestPayload.principal(), is(principal));
    assertThat(response, is(expectedResponse));
  }
}
