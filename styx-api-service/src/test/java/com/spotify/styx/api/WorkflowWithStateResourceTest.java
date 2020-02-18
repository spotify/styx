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
import static com.spotify.styx.api.JsonMatchers.assertJson;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import com.google.cloud.datastore.Datastore;
import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.state.Trigger;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.BigtableMocker;
import com.spotify.styx.storage.BigtableStorage;
import com.spotify.styx.storage.DatastoreEmulator;
import java.io.IOException;
import java.time.Duration;
import okio.ByteString;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

public class WorkflowWithStateResourceTest extends VersionedApiTest {

  @ClassRule public static final DatastoreEmulator datastoreEmulator = new DatastoreEmulator();

  private Datastore datastore = datastoreEmulator.client();
  private Connection bigtable = setupBigTableMockTable();
  private AggregateStorage storage;
  private AggregateStorage rawStorage;

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

  public WorkflowWithStateResourceTest(Api.Version version) {
    super("/workflows", version, "workflow-test");
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void shouldReturnWorkflowsWithStates() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeWorkflow(Workflow.create("other_component", WORKFLOW_CONFIGURATION));

    Response<ByteString> response = awaitResponse(
        serviceHelper.request("GET", path("/")));

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertJson(response, "[*]", hasSize(2));
    assertJson(response, "[0].state.enabled", is(false));
    assertJson(response, "[1].state.enabled", is(false));
  }


  @Override
  protected void init(Environment environment) {
    rawStorage = new AggregateStorage(bigtable, datastore);
    storage = spy(rawStorage);
    WorkflowWithStateResource workflowResource =
        new WorkflowWithStateResource(storage);

    environment.routingEngine()
        .registerRoutes(workflowResource.routes());
  }

  @Before
  public void setUp() throws Exception {
    storage.storeWorkflow(WORKFLOW);
  }

  @After
  public void tearDown() {
    datastoreEmulator.reset();
    serviceHelper.stubClient().clear();
  }

  private Connection setupBigTableMockTable() {
    Connection bigtable = mock(Connection.class);
    try {
      new BigtableMocker(bigtable)
          .setNumFailures(0)
          .setupTable(BigtableStorage.EVENTS_TABLE_NAME)
          .finalizeMocking();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return bigtable;
  }

}
