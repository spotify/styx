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
import static org.junit.Assert.assertThat;

import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import okio.ByteString;
import org.junit.Test;

public class SchedulerProxyResourceTest extends VersionedApiTest {

  private static final String SCHEDULER_BASE = "http://localhost:8080";

  public SchedulerProxyResourceTest(Api.Version version) {
    super(SchedulerProxyResource.BASE, version);
  }

  @Override
  protected void init(Environment environment) {
    final SchedulerProxyResource schedulerProxyResource = new SchedulerProxyResource(SCHEDULER_BASE);

    environment.routingEngine().registerRoutes(schedulerProxyResource.routes());
  }

  @Test
  public void testEventInjectionProxy() throws Exception {
    sinceVersion(Api.Version.V2);

    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.ACCEPTED))
        .to(SCHEDULER_BASE + "/api/v0/events");

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path("/events")));

    assertThat(response, hasStatus(withCode(Status.ACCEPTED)));
  }

  @Test
  public void testTriggerWorkflowInstanceProxy() throws Exception {
    sinceVersion(Api.Version.V2);

    serviceHelper.stubClient()
        .respond(Response.forStatus(Status.ACCEPTED))
        .to(SCHEDULER_BASE + "/api/v0/trigger");

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path("/trigger")));

    assertThat(response, hasStatus(withCode(Status.ACCEPTED)));
  }
}
