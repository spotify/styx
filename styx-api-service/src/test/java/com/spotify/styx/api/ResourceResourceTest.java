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

import static com.github.npathai.hamcrestopt.OptionalMatchers.isEmpty;
import static com.github.npathai.hamcrestopt.OptionalMatchers.isPresentAndIs;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.belongsToFamily;
import static com.spotify.styx.api.JsonMatchers.assertJson;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.spotify.apollo.Environment;
import com.spotify.apollo.Response;
import com.spotify.apollo.StatusType;
import com.spotify.styx.model.Resource;
import com.spotify.styx.storage.AggregateStorage;
import com.spotify.styx.storage.DatastoreEmulator;
import java.io.IOException;
import java.util.Map;
import okio.ByteString;
import org.apache.hadoop.hbase.client.Connection;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

public class ResourceResourceTest extends VersionedApiTest {

  @ClassRule public static final DatastoreEmulator datastoreEmulator = new DatastoreEmulator();

  private AggregateStorage storage;

  private static final Resource RESOURCE_1 = Resource.create("resource1", 1);
  private static final Resource RESOURCE_2 = Resource.create("resource2", 2);

  public ResourceResourceTest(Api.Version version) {
    super(ResourceResource.BASE, version, "resource-test");
    MockitoAnnotations.initMocks(this);
  }

  @Override
  protected void init(Environment environment) {
    storage = spy(new AggregateStorage(
        mock(Connection.class),
        datastoreEmulator.client()
    ));

    ResourceResource resourceResource = new ResourceResource(storage);

    environment.routingEngine()
        .registerRoutes(resourceResource.routes());
  }

  @Before
  public void setUp() throws Exception {
    storage.storeResource(RESOURCE_1);
  }

  @After
  public void tearDown() {
    datastoreEmulator.reset();
  }

  @Test
  public void shouldListResources() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "resources[0].id", equalTo("resource1"));
    assertJson(response, "resources[0].concurrency", equalTo(1));
  }

  @Test
  public void shouldListMultipleResources() throws Exception {
    sinceVersion(Api.Version.V3);

    storage.storeResource(RESOURCE_2);
    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "resources", hasSize(2));
    assertJson(response, "resources", containsInAnyOrder(
        Map.of("id", "resource1", "concurrency", 1),
        Map.of("id", "resource2", "concurrency", 2)
    ));
  }

  @Test
  public void shouldFailToListResources() throws Exception {
    sinceVersion(Api.Version.V3);

    doThrow(new IOException()).when(storage).resources();

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SERVER_ERROR)));
  }

  @Test
  public void shouldGetResource() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/resource1")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "id", equalTo("resource1"));
    assertJson(response, "concurrency", equalTo(1));
  }

  @Test
  public void shouldFailToGetResource() throws Exception {
    sinceVersion(Api.Version.V3);

    doThrow(new IOException()).when(storage).resource(anyString());

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("GET", path("/resource1")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SERVER_ERROR)));
  }

  @Test
  public void shouldPostResource() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""),
            ByteString.encodeUtf8("{\"id\": \"resource2\", \"concurrency\": 2}")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "id", equalTo("resource2"));
    assertJson(response, "concurrency", equalTo(2));

    assertThat(storage.resource(RESOURCE_2.id()), isPresentAndIs(RESOURCE_2));
    verify(storage).storeResource(RESOURCE_2);
    assertThat(storage.getLimitForCounter(RESOURCE_2.id()), is(RESOURCE_2.concurrency()));
  }

  @Test
  public void shouldFailToPostResource() throws Exception {
    sinceVersion(Api.Version.V3);

    doThrow(new IOException()).when(storage).storeResource(any(Resource.class));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("POST", path(""),
            ByteString.encodeUtf8("{\"id\": \"resource2\", \"concurrency\": 2}")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SERVER_ERROR)));
  }

  @Test
  public void shouldUpdateResource() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PUT", path("/resource1"),
            ByteString.encodeUtf8("{\"id\": \"resource1\", \"concurrency\": 21}")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(response, "id", equalTo("resource1"));
    assertJson(response, "concurrency", equalTo(21));

    assertThat(storage.resource(RESOURCE_1.id()), isPresentAndIs(Resource.create(RESOURCE_1.id(), 21)));
    verify(storage).storeResource(Resource.create(RESOURCE_1.id(), 21L));
    assertThat(storage.getLimitForCounter(RESOURCE_1.id()), is(21L));
  }

  @Test
  public void shouldFailToUpdateResource() throws Exception {
    sinceVersion(Api.Version.V3);

    doThrow(new IOException()).when(storage).storeResource(any(Resource.class));

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("PUT", path("/resource1"),
            ByteString.encodeUtf8("{\"id\": \"resource1\", \"concurrency\": 21}")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SERVER_ERROR)));
  }

  @Test
  public void shouldDeleteResource() throws Exception {
    sinceVersion(Api.Version.V3);

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("DELETE", path("/resource1")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertThat(storage.resource(RESOURCE_1.id()).isPresent(), is(false));
    assertThat(storage.shardsForCounter(RESOURCE_1.id()), is(Map.of()));
  }

  @Test
  public void shouldFailToDeleteResource() throws Exception {
    sinceVersion(Api.Version.V3);

    doThrow(new IOException()).when(storage).deleteResource(anyString());

    Response<ByteString> response =
        awaitResponse(serviceHelper.request("DELETE", path("/resource1")));

    assertThat(response, hasStatus(belongsToFamily(StatusType.Family.SERVER_ERROR)));
  }

  @Test
  public void shouldHandleMultipleResourcesIndependently() throws Exception {
    sinceVersion(Api.Version.V3);

    // add resource2
    awaitResponse(serviceHelper.request("POST", path(""),
        ByteString.encodeUtf8("{\"id\": \"resource2\", \"concurrency\": 2}")));

    // make sure both are listed
    Response<ByteString> listResponse =
        awaitResponse(serviceHelper.request("GET", path("")));
    assertThat(listResponse, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(listResponse, "resources", hasSize(2));
    assertJson(listResponse, "resources", containsInAnyOrder(
        Map.of("id", "resource1", "concurrency", 1),
        Map.of("id", "resource2", "concurrency", 2)
    ));

    // change resource2
    Response<ByteString> putResponse =
        awaitResponse(serviceHelper.request("PUT", path("/resource2"),
            ByteString.encodeUtf8("{\"id\": \"resource2\", \"concurrency\": 3}")));
    assertThat(putResponse, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));

    // make sure resource2 is changed in list and that resource1 is unchanged
    Response<ByteString> listResponse2 =
        awaitResponse(serviceHelper.request("GET", path("")));
    assertThat(listResponse2, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(listResponse2, "resources", hasSize(2));
    assertJson(listResponse2, "resources", containsInAnyOrder(
        Map.of("id", "resource1", "concurrency", 1),
        Map.of("id", "resource2", "concurrency", 3)
    ));
    assertThat(storage.resource(RESOURCE_1.id()), isPresentAndIs(RESOURCE_1));
    assertThat(storage.resource("resource2"), isPresentAndIs(Resource.create("resource2", 3)));
    assertThat(storage.getLimitForCounter(RESOURCE_1.id()), is(RESOURCE_1.concurrency()));
    assertThat(storage.getLimitForCounter(RESOURCE_2.id()), is(3L));

    // delete resource2
    Response<ByteString> deleteResponse =
        awaitResponse(serviceHelper.request("DELETE", path("/resource2")));
    assertThat(deleteResponse, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));

    // make sure resource2 is not showing up in the listing
    Response<ByteString> listResponse3 =
        awaitResponse(serviceHelper.request("GET", path("")));
    assertThat(listResponse3, hasStatus(belongsToFamily(StatusType.Family.SUCCESSFUL)));
    assertJson(listResponse3, "resources", hasSize(1));
    assertJson(listResponse3, "resources", containsInAnyOrder(
        Map.of("id", "resource1", "concurrency", 1)
    ));
    assertThat(storage.resource(RESOURCE_1.id()), isPresentAndIs(RESOURCE_1));
    assertThat(storage.resource("resource2"), isEmpty());
    assertThat(storage.getLimitForCounter(RESOURCE_1.id()), is(RESOURCE_1.concurrency()));

    try {
      storage.getLimitForCounter(RESOURCE_2.id());
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("No limit found in Datastore for resource2"));
    }
  }
}
