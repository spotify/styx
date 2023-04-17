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

import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withCode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.common.collect.Iterables;
import com.google.common.net.HttpHeaders;
import com.spotify.apollo.Environment;
import com.spotify.apollo.Request;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import java.util.Optional;
import java.util.UUID;
import okio.ByteString;
import org.junit.Test;
import org.slf4j.MDC;

public class SchedulerProxyResourceTest extends VersionedApiTest {

  private static final String SCHEDULER_BASE = "http://localhost:12345";

  public SchedulerProxyResourceTest(Api.Version version) {
    super(SchedulerProxyResource.BASE, version);
  }

  @Override
  protected void init(Environment environment) {
    final SchedulerProxyResource schedulerProxyResource = new SchedulerProxyResource(SCHEDULER_BASE,
        environment.client());

    environment.routingEngine()
        .registerRoutes(schedulerProxyResource.routes());
  }

  @Test
  public void testEventInjectionProxy() throws Exception {
    sinceVersion(Api.Version.V3);

    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.ACCEPTED))
        .to(SCHEDULER_BASE + "/api/v0/events");

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path("/events")));

    assertThat(response, hasStatus(withCode(Status.ACCEPTED)));
  }

  @Test
  public void testTriggerWorkflowInstanceProxy() throws Exception {
    sinceVersion(Api.Version.V3);

    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.ACCEPTED))
        .to(SCHEDULER_BASE + "/api/v0/trigger");

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path("/trigger")));

    assertThat(response, hasStatus(withCode(Status.ACCEPTED)));
  }

  @Test
  public void testTriggerWorkflowWithQueryParameter() throws Exception {
    sinceVersion(Api.Version.V3);

    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.ACCEPTED))
        .to(SCHEDULER_BASE + "/api/v0/trigger?bar=foo&foo=foo&foo=bar");

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path("/trigger?foo=foo&foo=bar&bar=foo")));

    assertThat(response, hasStatus(withCode(Status.ACCEPTED)));
  }

  @Test
  public void verifyPassesHeaders() throws Exception {
    sinceVersion(Api.Version.V3);

    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.ACCEPTED))
        .to(SCHEDULER_BASE + "/api/v0/trigger");

    final String requestId = UUID.randomUUID().toString();

    awaitResponse(serviceHelper.request(Request
        .forUri(path("/trigger"), "POST")
        .withHeader("foo", "bar")
        .withHeader("X-Request-Id", requestId)
        .withHeader(HttpHeaders.AUTHORIZATION, "decafbad")));

    final Request schedulerRequest = Iterables.getOnlyElement(serviceHelper.stubClient().sentRequests());

    assertThat(schedulerRequest.header("foo"), is(Optional.of("bar")));
    assertThat(schedulerRequest.header(HttpHeaders.AUTHORIZATION), is(Optional.of("decafbad")));
    assertThat(schedulerRequest.header("X-Request-Id"), is(Optional.of(requestId)));
  }

  @Test
  public void verifyPropagatesContextRequestId() throws Exception {
    sinceVersion(Api.Version.V3);

    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.ACCEPTED))
        .to(SCHEDULER_BASE + "/api/v0/trigger");

    final String requestId = UUID.randomUUID().toString();

    MDC.put("request-id", requestId);

    awaitResponse(serviceHelper.request(Request
        .forUri(path("/trigger"), "POST")));

    final Request schedulerRequest = Iterables.getOnlyElement(serviceHelper.stubClient().sentRequests());

    assertThat(schedulerRequest.header("X-Request-Id"), is(Optional.of(requestId)));
  }

  @Test
  public void verifyReplaceHost() throws Exception {
    sinceVersion(Api.Version.V3);

    serviceHelper.stubClient()
            .respond(Response.forStatus(Status.ACCEPTED))
            .to(SCHEDULER_BASE + "/api/v0/trigger");

    awaitResponse(serviceHelper.request(Request
            .forUri(path("/trigger"), "POST")
            .withHeader("Host", "styx-api")));

    final Request schedulerRequest = Iterables.getOnlyElement(serviceHelper.stubClient().sentRequests());

    assertThat(schedulerRequest.header("Host"), is(Optional.of("localhost")));
  }
}
