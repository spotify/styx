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

package com.spotify.styx;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.googleapis.json.GoogleJsonError;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponseException;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.model.ServiceAccountKey;
import com.spotify.styx.monitoring.Stats;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ServiceAccountKeyManagerTest {

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private Iam iam;

  @Mock
  private Iam.Projects.ServiceAccounts serviceAccounts;

  @Mock
  private Iam.Projects.ServiceAccounts.Keys keys;

  @Mock
  private Iam.Projects.ServiceAccounts.Keys.Delete delete;

  @Mock
  private Iam.Projects.ServiceAccounts.Keys.Get get;

  @Mock
  private Iam.Projects.ServiceAccounts.Keys.Create create;

  @Mock
  private Stats stats;

  private ServiceAccountKeyManager sakm;

  private static final GoogleJsonResponseException INTERNAL_SERVER_ERROR =
      new GoogleJsonResponseException(
          new HttpResponseException.Builder(500, "Internal Server Error", new HttpHeaders()),
          new GoogleJsonError().set("status", "INTERNAL_SERVER_ERROR"));

  private static final GoogleJsonResponseException PERMISSION_DENIED =
      new GoogleJsonResponseException(
          new HttpResponseException.Builder(403, "Forbidden", new HttpHeaders()),
          new GoogleJsonError().set("status", "PERMISSION_DENIED"));

  private static final GoogleJsonResponseException NOT_FOUND =
      new GoogleJsonResponseException(
          new HttpResponseException.Builder(404, "Not found", new HttpHeaders()),
          new GoogleJsonError().set("status", "NOT_FOUND"));

  @Before
  public void setUp() throws Exception {
    when(iam.projects().serviceAccounts()).thenReturn(serviceAccounts);
    when(serviceAccounts.keys()).thenReturn(keys);
    when(keys.get(any())).thenReturn(get);
    when(keys.delete(any())).thenReturn(delete);
    when(keys.create(any(), any())).thenReturn(create);

    sakm = new ServiceAccountKeyManager(iam, stats);
  }

  @Test
  public void keyExistsTreatsPermissionDeniedAsNotFound() throws Exception {
    when(get.execute()).thenThrow(PERMISSION_DENIED);
    assertThat(sakm.keyExists("foo"), is(false));
    verify(iam.projects().serviceAccounts().keys()).get("foo");
  }

  @Test
  public void keyExistsReturnsFalseForNotFound() throws Exception {
    when(get.execute()).thenThrow(NOT_FOUND);
    assertThat(sakm.keyExists("foo"), is(false));
    verify(iam.projects().serviceAccounts().keys()).get("foo");
  }

  @Test(expected = GoogleJsonResponseException.class)
  public void keyExistsShouldThrowUnknownExceptions() throws Exception {
    when(get.execute()).thenThrow(INTERNAL_SERVER_ERROR);
    sakm.keyExists("foo");
    verify(iam.projects().serviceAccounts().keys()).get("foo");
  }

  @Test(expected = GoogleJsonResponseException.class)
  public void deleteKeyShouldThrowUnknownExceptions() throws Exception {
    when(delete.execute()).thenThrow(INTERNAL_SERVER_ERROR);
    sakm.deleteKey("foo");
    verify(iam.projects().serviceAccounts().keys()).delete("foo");
  }

  @Test
  public void tryDeleteKeyShouldIgnoreUnknownExceptions() throws Exception {
    when(delete.execute()).thenThrow(INTERNAL_SERVER_ERROR);
    sakm.tryDeleteKey("foo");
    verify(iam.projects().serviceAccounts().keys()).delete("foo");
  }

  @Test
  public void deleteKeyShouldIgnorePermissionDenied() throws Exception {
    when(delete.execute()).thenThrow(PERMISSION_DENIED);
    sakm.deleteKey("foo");
    verify(iam.projects().serviceAccounts().keys()).delete("foo");
  }

  @Test
  public void deleteKeyShouldIgnoreNotFound() throws Exception {
    when(delete.execute()).thenThrow(NOT_FOUND);
    sakm.deleteKey("foo");
    verify(iam.projects().serviceAccounts().keys()).delete("foo");
  }

  @Test
  public void createKeyShouldBeRecorded() throws Exception {
    ServiceAccountKey key = new ServiceAccountKey();
    when(create.execute()).thenReturn(key);
    sakm.createJsonKey("SA");
    verify(stats).recordKeyCreation();
  }

  @Test
  public void deleteKeyShouldBeRecorded() throws Exception {
    sakm.deleteKey("SA");
    verify(stats).recordKeyDeletion();
  }
}
