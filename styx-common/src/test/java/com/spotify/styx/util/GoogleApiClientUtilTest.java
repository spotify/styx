/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

package com.spotify.styx.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.github.rholder.retry.StopStrategies;
import com.github.rholder.retry.WaitStrategies;
import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.googleapis.testing.services.MockGoogleClient;
import com.google.api.client.googleapis.testing.services.MockGoogleClientRequest;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.EmptyContent;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.json.JsonObjectParser;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import java.io.IOException;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class GoogleApiClientUtilTest {

  private final MockLowLevelHttpResponse response = spy(new MockLowLevelHttpResponse())
      .setContent("");

  private final MockHttpTransport transport = new MockHttpTransport.Builder()
      .setLowLevelHttpResponse(response)
      .build();

  private final MockGoogleClient client = new MockGoogleClient.Builder(
      transport, "http://foo/", "/bar", new JsonObjectParser(
      Utils.getDefaultJsonFactory()),
      new MockGoogleCredential.Builder().build())
      .setApplicationName("test")
      .build();

  private MockGoogleClientRequest<Void> request = spy(new MockGoogleClientRequest<>(
      client, "GET", "http://foo/bar/baz", new EmptyContent(), Void.class));

  @Test
  public void shouldRetry429() throws IOException {
    when(response.getStatusCode()).thenReturn(429).thenReturn(200);
    GoogleApiClientUtil.executeWithRetries(request, WaitStrategies.noWait(), StopStrategies.stopAfterAttempt(3));
    verify(request, times(2)).execute();
  }

  @Test
  @Parameters({ "500", "503", "599" })
  public void shouldRetry5xx(int statusCode) throws IOException {
    when(response.getStatusCode()).thenReturn(statusCode).thenReturn(200);
    GoogleApiClientUtil.executeWithRetries(request, WaitStrategies.noWait(), StopStrategies.stopAfterAttempt(3));
    verify(request, times(2)).execute();
  }

  @Test
  public void shouldRetryIOException() throws IOException {
    response.setStatusCode(200);
    doThrow(new IOException()).doCallRealMethod().when(request).execute();
    GoogleApiClientUtil.executeWithRetries(request, WaitStrategies.noWait(), StopStrategies.stopAfterAttempt(3));
    verify(request, times(2)).execute();
  }

  @Test
  @Parameters({ "400", "401", "403", "404" })
  public void shouldNotRetry4xx(int statusCode) throws IOException {
    when(response.getStatusCode()).thenReturn(statusCode).thenReturn(200);
    try {
      GoogleApiClientUtil.executeWithRetries(request, WaitStrategies.noWait(), StopStrategies.stopAfterAttempt(3));
      fail();
    } catch (HttpResponseException e) {
      assertThat(e.getStatusCode(), is(statusCode));
    }
    verify(request, times(1)).execute();
  }

  @Test
  public void testRetriesByDefault() throws IOException {
    when(response.getStatusCode()).thenReturn(429).thenReturn(200);
    GoogleApiClientUtil.executeWithRetries(request);
    verify(request, times(2)).execute();
  }

  @Test
  public void testUtilityClass() throws ReflectiveOperationException {
    assertThat(ClassEnforcer.assertNotInstantiable(GoogleApiClientUtil.class), is(true));
  }
}
