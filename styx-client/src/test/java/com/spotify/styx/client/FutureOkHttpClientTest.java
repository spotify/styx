/*-
 * -\-\-
 * Spotify Styx API Client
 * --
 * Copyright (C) 2016 - 2018 Spotify AB
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

import static com.spotify.styx.client.OkHttpTestUtil.bytesOfRequestBody;
import static com.spotify.styx.client.OkHttpTestUtil.responseBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import okhttp3.Call;
import okhttp3.Callback;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okio.ByteString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class FutureOkHttpClientTest {

  @Mock
  public Call call;
  @Mock
  public OkHttpClient okHttpClient;

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  private static final HttpUrl URI = Objects.requireNonNull(HttpUrl.parse("http://example.org/"));

  private static final Request REQUEST = FutureOkHttpClient.forUri(URI);

  @Test
  public void testMethod() {
    final Request request = FutureOkHttpClient.forUri(URI, "DELETE");

    assertThat(request.url(), is(URI));
    assertThat(request.method(), is("DELETE"));
  }

  @Test
  public void testSimpleGet() throws IOException {
    final FutureOkHttpClient client = FutureOkHttpClient.create(okHttpClient);
    when(okHttpClient.newCall(any())).thenReturn(call);
    final ArgumentCaptor<Callback> argumentCaptor = ArgumentCaptor.forClass(Callback.class);
    doNothing().when(call).enqueue(argumentCaptor.capture());
    final CompletableFuture<Response> future = client.send(REQUEST).toCompletableFuture();

    final Response response = responseBuilder(REQUEST, 200).build();

    argumentCaptor.getValue().onResponse(call, response);

    assertThat(future.isDone(), is(true));
    assertThat(future.join(), is(response));
  }

  @Test
  public void testUnserializablePayload() {
    class EmptyClass {}
    thrown.expectCause(instanceOf(JsonProcessingException.class));
    FutureOkHttpClient.forUri(URI, "POST", new EmptyClass());
  }

  @Test
  public void testSomethingGoesWrong() {
    final FutureOkHttpClient client = FutureOkHttpClient.create(okHttpClient);
    when(okHttpClient.newCall(any())).thenReturn(call);
    final ArgumentCaptor<Callback> argumentCaptor = ArgumentCaptor.forClass(Callback.class);
    doNothing().when(call).enqueue(argumentCaptor.capture());
    final CompletableFuture<Response> future = client.send(REQUEST).toCompletableFuture();

    final IOException exception = new IOException("oh noes!");
    argumentCaptor.getValue().onFailure(call, exception);

    assertThat(future.isDone(), is(true));

    thrown.expectCause(is(exception));
    future.join();
  }

  @Test
  public void testRequestWithBody() {
    final Request request = FutureOkHttpClient.forUri(URI, "POST", Arrays.asList(1, 2, 3));

    assertThat(request.url(), is(URI));
    assertThat(request.method(), is("POST"));
    assertThat(bytesOfRequestBody(request), is(ByteString.encodeUtf8("[1,2,3]")));
  }

}
