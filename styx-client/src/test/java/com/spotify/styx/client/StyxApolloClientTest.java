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

import static com.google.common.collect.Iterables.getLast;
import static com.spotify.styx.util.StringIsValidUuid.isValidUuid;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.spotify.apollo.Client;
import com.spotify.apollo.Request;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.styx.api.Api;
import com.spotify.styx.client.auth.GoogleIdTokenAuth;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.EditableBackfillInput;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowInstance;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.data.WorkflowInstanceExecutionData;
import com.spotify.styx.serialization.Json;
import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import okhttp3.HttpUrl;
import okio.ByteString;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class StyxApolloClientTest {

  private static final WorkflowConfiguration WORKFLOW_CONFIGURATION_1 = WorkflowConfiguration.builder()
      .id("bar-wf_1")
      .dockerImage("busybox")
      .dockerArgs(ImmutableList.of("echo", "hello world"))
      .schedule(Schedule.DAYS)
      .build();

  private static final WorkflowConfiguration WORKFLOW_CONFIGURATION_2 = WorkflowConfiguration.builder()
      .id("bar-wf_2")
      .dockerImage("busybox")
      .dockerArgs(ImmutableList.of("echo", "hello world"))
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

  @Mock Client client;
  @Mock GoogleIdTokenAuth auth;

  @Captor ArgumentCaptor<Request> requestCaptor;

  private static final String API_VERSION = getLast(asList(Api.Version.values())).name().toLowerCase();

  private static final HttpUrl API_URL = new HttpUrl.Builder()
      .scheme("https").host("foo.bar")
      .addPathSegment("api").addPathSegment(API_VERSION).build();

  private static final String CLIENT_HOST = API_URL.scheme() + "://" + API_URL.host() ;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    when(auth.getToken(any())).thenReturn(Optional.of("foobar"));
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
    final CompletableFuture<Response<ByteString>> responseFuture = new CompletableFuture<>();
    when(client.send(any(Request.class))).thenReturn(responseFuture);

    final StyxApolloClient styx = new StyxApolloClient(client, host, auth);
    styx.resourceList();
    verify(client, timeout(30_000)).send(requestCaptor.capture());

    final Request request = requestCaptor.getValue();
    assertThat(request.uri(), startsWith(expectedUriPrefix));
  }

  @Test
  public void shouldGetAllWorkflows() throws Exception {
    final List<Workflow> workflows = ImmutableList.of(WORKFLOW_1, WORKFLOW_2);
    when(client.send(any(Request.class))).thenReturn(CompletableFuture.completedFuture(
        Response.forStatus(Status.OK).withPayload(Json.serialize(workflows))));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    final CompletableFuture<List<Workflow>> r = styx.workflows().toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/workflows");
    assertThat(request.uri(), is(uri.toString()));
    assertThat(request.method(), is("GET"));
    assertThat(r.join(), is(workflows));
  }

  @Test
  public void shouldGetWorkflowInstance() throws Exception {
    final WorkflowInstanceExecutionData workflowInstanceExecutionData =
        WorkflowInstanceExecutionData.create(
            WorkflowInstance.create(
                WorkflowId.create("component", "workflow"),
                "2017-01-01T00"),
            Collections.emptyList());
    when(client.send(any(Request.class))).thenReturn(CompletableFuture.completedFuture(
        Response.forStatus(Status.OK).withPayload(Json.serialize(workflowInstanceExecutionData))));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    final CompletableFuture<WorkflowInstanceExecutionData> r =
        styx.workflowInstanceExecutions("component", "workflow", "2017-01-01T00")
            .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/workflows/component/workflow/instances/2017-01-01T00");
    assertThat(request.uri(), is(uri.toString()));
    assertThat(request.method(), is("GET"));
    assertThat(r.join(), is(workflowInstanceExecutionData));
  }

  @Test
  public void deleteWorkflow() {
    when(client.send(any(Request.class))).thenReturn(
        CompletableFuture.completedFuture(Response.forStatus(Status.NO_CONTENT)));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    final CompletableFuture<Void> r = styx.deleteWorkflow("f[ ]o-cmp", "bar-w[f]")
        .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    assertThat(r.isCompletedExceptionally(), is(false));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/workflows/f%5B%20%5Do-cmp/bar-w%5Bf%5D");
    assertThat(request.uri(), is(uri.toString()));
    assertThat(request.method(), is("DELETE"));
  }

  @Test
  public void componentAndWorkflowAreEncoded() {
    when(client.send(any(Request.class))).thenReturn(
        CompletableFuture.completedFuture(Response.forStatus(Status.OK)));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    styx.workflow("f[ ]o-cmp", "bar-w[f]")
        .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(
        API_URL + "/workflows/f%5B%20%5Do-cmp/bar-w%5Bf%5D");
    assertThat(request.uri(), is(uri.toString()));
  }

  @Test
  public void createOrUpdateWorkflow() throws Exception {
    when(client.send(any(Request.class))).thenReturn(CompletableFuture.completedFuture(
        Response.forStatus(Status.OK).withPayload(Json.serialize(WORKFLOW_1))));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    final CompletableFuture<Workflow> r = styx.createOrUpdateWorkflow("f[ ]o-cmp",
        WORKFLOW_CONFIGURATION_1).toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/workflows/f%5B%20%5Do-cmp");
    assertThat(request.uri(), is(uri.toString()));
    assertThat(Json.deserialize(request.payload().get(), WorkflowConfiguration.class),
        is(WORKFLOW_CONFIGURATION_1));
    assertThat(request.method(), is("POST"));
  }

  @Test
  public void shouldCreateBackfill() throws Exception {
    when(client.send(any(Request.class))).thenReturn(CompletableFuture.completedFuture(
        Response.forStatus(Status.OK).withPayload(Json.serialize(BACKFILL))));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    final CompletableFuture<Backfill> r = styx.backfillCreate("f[ ]o-cmp", "bar-w[f]",
        "2017-01-01T00:00:00Z", "2017-01-30T00:00:00Z", 1)
        .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    final URI uri = URI.create(API_URL + "/backfills");
    assertThat(request.uri(), is(uri.toString()));
    assertThat(Json.deserialize(request.payload().get(), BackfillInput.class),
        equalTo(BACKFILL_INPUT));
    assertThat(request.method(), is("POST"));
  }

  @Test
  public void shouldCreateBackfillFromInput() throws Exception {
    final BackfillInput backfillInput = BACKFILL_INPUT.builder()
        .reverse(true)
        .build();

    when(client.send(any(Request.class))).thenReturn(CompletableFuture.completedFuture(
        Response.forStatus(Status.OK).withPayload(Json.serialize(BACKFILL))));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    final CompletableFuture<Backfill> r = styx.backfillCreate(backfillInput)
        .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.uri(), is(API_URL + "/backfills"));
    assertThat(Json.deserialize(request.payload().get(), BackfillInput.class),
        equalTo(backfillInput));
    assertThat(request.method(), is("POST"));
  }

  @Test
  public void shouldCreateBackfillWithDescription() throws Exception {
    final BackfillInput backfillInput = BACKFILL_INPUT.builder()
        .description("Description")
        .build();
    when(client.send(any(Request.class))).thenReturn(CompletableFuture.completedFuture(
        Response.forStatus(Status.OK).withPayload(Json.serialize(BACKFILL))));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    final CompletableFuture<Backfill> r = styx.backfillCreate("f[ ]o-cmp", "bar-w[f]",
                                                              "2017-01-01T00:00:00Z",
                                                              "2017-01-30T00:00:00Z", 1,
                                                              "Description")
        .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.uri(), is(API_URL + "/backfills"));
    assertThat(Json.deserialize(request.payload().get(), BackfillInput.class),
               equalTo(backfillInput));
    assertThat(request.method(), is("POST"));
  }

  @Test
  public void shouldUpdateBackfillConcurrency() throws Exception {
    final EditableBackfillInput backfillInput = EditableBackfillInput.newBuilder()
        .id(BACKFILL.id())
        .concurrency(4)
        .build();
    when(client.send(any(Request.class))).thenReturn(CompletableFuture.completedFuture(
        Response.forStatus(Status.OK).withPayload(Json.serialize(
            BACKFILL.builder().concurrency(4).build()))));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    final CompletableFuture<Backfill> r = styx.backfillEditConcurrency(BACKFILL.id(), 4)
        .toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.uri(), is(API_URL + "/backfills/" + BACKFILL.id()));
    assertThat(Json.deserialize(request.payload().get(), EditableBackfillInput.class),
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
        Response.forStatus(Status.OK).withPayload(Json.serialize(workflowState))));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    final CompletableFuture<WorkflowState> r =
        styx.updateWorkflowState("f[ ]o-cmp", "bar-w[f]", workflowState).toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.uri(), is(API_URL + "/workflows/f%5B%20%5Do-cmp/bar-w%5Bf%5D/state"));
    assertThat(Json.deserialize(request.payload().get(), WorkflowState.class), is(workflowState));
    assertThat(request.method(), is("PATCH"));
  }

  @Test
  public void testTokenSuccess() {
    when(client.send(any(Request.class))).thenReturn(CompletableFuture.completedFuture(
        Response.forStatus(Status.OK)));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    final CompletableFuture<Void> r =
        styx.triggerWorkflowInstance("foo", "bar", "baz").toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.header("Authorization").get(), is("Bearer foobar"));
  }

  @Test
  public void testTokenFailure() throws Exception {
    final IOException rootCause = new IOException("netsplit!");
    when(auth.getToken(any())).thenThrow(rootCause);
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    final CompletableFuture<Void> f = styx.triggerWorkflowInstance("foo", "bar", "baz")
        .toCompletableFuture();
    try {
      f.join();
      fail();
    } catch (CompletionException e) {
      assertThat(e.getCause(), instanceOf(ClientErrorException.class));
      assertThat(e.getCause().getMessage(), is("Authentication failure: " + rootCause.getMessage()));
      assertThat(e.getCause().getCause(), is(rootCause));
    }
  }

  @Test
  public void testUnathorizedMissingCredentialsApiError() throws Exception {
    when(auth.getToken(any())).thenReturn(Optional.empty());
    when(client.send(any()))
        .thenReturn(CompletableFuture.completedFuture(Response.forStatus(Status.UNAUTHORIZED)));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);

    try {
      styx.triggerWorkflowInstance("foo", "bar", "baz").toCompletableFuture().join();
      fail();
    } catch (CompletionException e) {
      assertThat(e.getCause(), instanceOf(ApiErrorException.class));
      ApiErrorException apiErrorException = (ApiErrorException) e.getCause();
      assertThat(apiErrorException.isAuthenticated(), is(false));
    }
  }

  @Test
  public void testUnauthorizedWithCredentialsApiError() {
    when(client.send(any()))
        .thenReturn(CompletableFuture.completedFuture(Response.forStatus(Status.UNAUTHORIZED)));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);

    try {
      styx.triggerWorkflowInstance("foo", "bar", "baz").toCompletableFuture().join();
      fail();
    } catch (CompletionException e) {
      assertThat(e.getCause(), instanceOf(ApiErrorException.class));
      ApiErrorException apiErrorException = (ApiErrorException) e.getCause();
      assertThat(apiErrorException.isAuthenticated(), is(true));
    }
  }

  @Test
  public void testSendsRequestId() throws Exception {
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    when(client.send(requestCaptor.capture())).thenReturn(CompletableFuture.completedFuture(
        Response.forStatus(Status.OK).withPayload(Json.serialize(Collections.emptyList()))));
    styx.workflows();
    final Request request = requestCaptor.getValue();
    assertThat(request.header("X-Request-Id").get(), isValidUuid());
  }

  @Test
  public void testUsesServerRequestIdOnMismatch() throws Exception {
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    final String responseRequestId = "foobar";
    when(client.send(any())).thenReturn(CompletableFuture.completedFuture(
        Response.forStatus(Status.INTERNAL_SERVER_ERROR)
            .withHeader("X-Request-Id", responseRequestId)
            .withPayload(Json.serialize(Collections.emptyList()))));

    try {
      styx.workflows().toCompletableFuture().join();
      fail();
    } catch (CompletionException e) {
      assertThat(e.getCause(), instanceOf(ApiErrorException.class));
      final ApiErrorException apiError = (ApiErrorException) e.getCause();
      assertThat(apiError.getRequestId(), is(responseRequestId));
    }
  }

  @Test
  public void testUsesClientRequestIdOnNoResponseRequestId() throws Exception {
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST, auth);
    when(client.send(requestCaptor.capture())).thenReturn(CompletableFuture.completedFuture(
        Response.forStatus(Status.INTERNAL_SERVER_ERROR)
            .withPayload(Json.serialize(Collections.emptyList()))));

    try {
      styx.workflows().toCompletableFuture().join();
      fail();
    } catch (CompletionException e) {
      assertThat(e.getCause(), instanceOf(ApiErrorException.class));
      final ApiErrorException apiError = (ApiErrorException) e.getCause();
      final Request request = requestCaptor.getValue();
      assertThat(apiError.getRequestId(), is(request.header("X-Request-Id").get()));
    }
  }
}
