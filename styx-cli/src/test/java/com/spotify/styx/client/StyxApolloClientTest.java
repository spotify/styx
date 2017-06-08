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

import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.spotify.apollo.Client;
import com.spotify.apollo.Request;
import com.spotify.apollo.Response;
import java.util.concurrent.CompletableFuture;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
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

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  @Parameters({
      "foo.bar, http://foo.bar",
      "foo.bar:80, http://foo.bar",
      "foo.bar:17, http://foo.bar:17",
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
}
