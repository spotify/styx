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
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
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
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.serialization.Json;
import java.util.concurrent.CompletableFuture;
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

  @Mock Client client;

  @Captor ArgumentCaptor<Request> requestCaptor;

  private static final String API_VERSION = getLast(asList(Api.Version.values())).name().toLowerCase();

  private static final HttpUrl API_URL = new HttpUrl.Builder()
      .scheme("https").host("foo.bar")
      .addPathSegment("api").addPathSegment(API_VERSION).build();

  private static final String CLIENT_HOST = API_URL.scheme() + "://" + API_URL.host() ;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
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
  public void testHosts(String host, String expectedUriPrefix) throws Exception {
    final CompletableFuture<Response<ByteString>> responseFuture = new CompletableFuture<>();
    when(client.send(any(Request.class))).thenReturn(responseFuture);

    final StyxApolloClient styx = new StyxApolloClient(client, host);
    styx.resourceList();
    verify(client, timeout(30_000)).send(requestCaptor.capture());

    final Request request = requestCaptor.getValue();
    assertThat(request.uri(), startsWith(expectedUriPrefix));
  }

  @Test
  public void deleteWorkflow() throws Exception {
    when(client.send(any(Request.class))).thenReturn(
        CompletableFuture.completedFuture(Response.forStatus(Status.NO_CONTENT)));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST);
    final CompletableFuture<Void> r = styx.deleteWorkflow("foo-comp", "bar-wf").toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    assertThat(r.isCompletedExceptionally(), is(false));
    final Request request = requestCaptor.getValue();
    assertThat(request.uri(), is(API_URL + "/workflows/foo-comp/bar-wf"));
    assertThat(request.method(), is("DELETE"));
  }

  @Test
  public void createOrUpdateWorkflow() throws Exception {
    final WorkflowConfiguration config = WorkflowConfiguration.builder()
        .id("bar-wf")
        .dockerImage("busybox")
        .dockerArgs(ImmutableList.of("echo", "hello world"))
        .schedule(Schedule.DAYS)
        .build();
    final Workflow workflow = Workflow.create("foo-comp", config);
    when(client.send(any(Request.class))).thenReturn(CompletableFuture.completedFuture(
        Response.forStatus(Status.OK).withPayload(Json.serialize(workflow))));
    final StyxApolloClient styx = new StyxApolloClient(client, CLIENT_HOST);
    final CompletableFuture<Workflow> r = styx.createOrUpdateWorkflow("foo-comp", config).toCompletableFuture();
    verify(client, timeout(30_000)).send(requestCaptor.capture());
    assertThat(r.isDone(), is(true));
    final Request request = requestCaptor.getValue();
    assertThat(request.uri(), is(API_URL + "/workflows/foo-comp"));
    assertThat(Json.deserialize(request.payload().get(), WorkflowConfiguration.class), is(config));
    assertThat(request.method(), is("POST"));
  }
}
