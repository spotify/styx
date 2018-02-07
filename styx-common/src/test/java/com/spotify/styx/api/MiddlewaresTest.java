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

import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.belongsToFamily;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withCode;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.auto.value.AutoValue;
import com.google.common.collect.ImmutableList;
import com.google.common.net.HttpHeaders;
import com.spotify.apollo.Client;
import com.spotify.apollo.Request;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.request.RequestContexts;
import com.spotify.apollo.request.RequestMetadataImpl;
import com.spotify.apollo.route.AsyncHandler;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import okio.ByteString;
import org.junit.Test;

/**
 * Tests Middlewares
 */
public class MiddlewaresTest {

  private static final TestStruct TEST_STRUCT = new AutoValue_MiddlewaresTest_TestStruct(
      "blah", new AutoValue_MiddlewaresTest_Inner("bloh", TestEnum.ENUM_VALUE));

  private RequestContext mockRequestContext(boolean hasHeader) {
    RequestContext requestContext = mock(RequestContext.class);
    Request request = Request.forUri("/", "GET");
    if (hasHeader) {
      request = request.withHeader("User-Agent", "Styx CLI 0.1.1");
    }
    when(requestContext.request()).thenReturn(request);
    return requestContext;
  }

  private <T> AsyncHandler<Response<T>> mockInnerHandler(RequestContext requestContext) {
    return mockInnerHandler(requestContext, completedFuture(Response.forStatus(Status.OK)));
  }

  private <T> AsyncHandler<Response<T>> mockInnerHandler(RequestContext requestContext,
                                                         CompletionStage completionStage) {
    // noinspection unchecked
    AsyncHandler<Response<T>> innerHandler = mock(AsyncHandler.class);
    // noinspection unchecked
    when(innerHandler.invoke(requestContext)).thenReturn(completionStage);
    return innerHandler;
  }

  private <T> AsyncHandler<Response<T>> mockInnerHandler(RequestContext requestContext,
                                                         Throwable throwable) {
    // noinspection unchecked
    AsyncHandler<Response<T>> innerHandler = mock(AsyncHandler.class);
    // noinspection unchecked
    when(innerHandler.invoke(requestContext)).thenThrow(throwable);
    return innerHandler;
  }

  @Test
  public void testJson() throws Exception {
    AsyncHandler<Response<ByteString>> outerHandler = Middlewares.json().apply(
        rc -> Response.forPayload(TEST_STRUCT)
    );

    CompletionStage<Response<ByteString>> completionStage = outerHandler.invoke(
        RequestContexts.create(mock(Request.class), mock(Client.class),
                               Collections.emptyMap(), System.nanoTime(),
                               RequestMetadataImpl.create(Instant.EPOCH, 
                                                          Optional.empty(), Optional.empty())));

    assertThat(completionStage.toCompletableFuture().get().payload().get().utf8(),
               is(
                   "{\"foo\":\"blah\"," +
                   "\"inner_object\":{" +
                   "\"field_name_convention\":\"bloh\"," +
                   "\"enum_field\":\"enum_value\"}}"
               ));
  }

  @Test
  public void testJsonAsync() throws Exception {
    AsyncHandler<Response<ByteString>> outerHandler = Middlewares.jsonAsync().apply(
        rc -> completedFuture(Response.forPayload(TEST_STRUCT))
    );

    CompletionStage<Response<ByteString>> completionStage = outerHandler.invoke(
        RequestContexts.create(mock(Request.class), mock(Client.class), Collections.emptyMap()));

    assertThat(completionStage.toCompletableFuture().get().payload().get().utf8(),
               is(
                   "{\"foo\":\"blah\"," +
                   "\"inner_object\":{" +
                   "\"field_name_convention\":\"bloh\"," +
                   "\"enum_field\":\"enum_value\"}}"
               ));
  }

  @Test
  public void testNoPayloadResponse() throws Exception {
    AsyncHandler<Response<ByteString>> outerHandler = Middlewares.json().apply(
        rc -> Response.forStatus(Status.INTERNAL_SERVER_ERROR)
    );

    CompletionStage<Response<ByteString>> completionStage = outerHandler.invoke(
        RequestContexts.create(mock(Request.class), mock(Client.class), Collections.emptyMap()));

    assertThat(completionStage.toCompletableFuture().get().payload().isPresent(), is(false));
  }

  private static class NotSerializable {
  }

  @Test
  public void testInvalidPayload() throws Exception {
    AsyncHandler<Response<ByteString>> outerHandler = Middlewares.json().apply(
        rc -> Response.forPayload(new NotSerializable())
    );

    CompletionStage<Response<ByteString>> completionStage = outerHandler.invoke(
        RequestContexts.create(mock(Request.class), mock(Client.class), Collections.emptyMap()));

    Response<ByteString> response = completionStage.toCompletableFuture().get();
    assertThat(response.payload().isPresent(), is(false));
    assertThat(response.status().family(), is(Status.Family.SERVER_ERROR));
  }

  @Test
  public void testValidClientNoBlacklist() throws Exception {
    Supplier<List<String>> supplier = Collections::emptyList;
    RequestContext requestContext = mockRequestContext(true);
    Response<Object> response = awaitResponse(Middlewares.clientValidator(supplier)
        .apply(mockInnerHandler(requestContext))
        .invoke(requestContext));
    assertThat(response, hasStatus(withCode(Status.OK)));
  }

  @Test
  public void testValidClientEmptyBlacklist() throws Exception {
    Supplier<List<String>> supplier = Collections::emptyList;
    RequestContext requestContext = mockRequestContext(true);
    Response<Object> response = awaitResponse(Middlewares.clientValidator(supplier)
        .apply(mockInnerHandler(requestContext))
        .invoke(requestContext));
    assertThat(response, hasStatus(withCode(Status.OK)));
  }

  @Test
  public void testValidClient() throws Exception {
    Supplier<List<String>> supplier = () -> ImmutableList.of("Styx CLI 0.1.0");
    RequestContext requestContext = mockRequestContext(true);
    Response<Object> response = awaitResponse(Middlewares.clientValidator(supplier)
        .apply(mockInnerHandler(requestContext))
        .invoke(requestContext));
    assertThat(response, hasStatus(withCode(Status.OK)));
  }

  @Test
  public void testValidClientNoHeader() throws Exception {
    Supplier<List<String>> supplier = () -> ImmutableList.of("Styx CLI 0.1.0");
    RequestContext requestContext = mockRequestContext(false);
    Response<Object> response = awaitResponse(Middlewares.clientValidator(supplier)
        .apply(mockInnerHandler(requestContext))
        .invoke(requestContext));
    assertThat(response, hasStatus(withCode(Status.OK)));
  }

  @Test
  public void testInvalidClient() throws Exception {
    Supplier<List<String>> supplier = () -> ImmutableList.of("Styx CLI 0.1.1");
    RequestContext requestContext = mockRequestContext(true);

    Response<Object> response = awaitResponse(Middlewares.clientValidator(supplier)
        .apply(mockInnerHandler(requestContext))
        .invoke(requestContext));
    assertThat(response, hasStatus(belongsToFamily(Status.Family.CLIENT_ERROR)));
  }

  @Test
  public void testHTTPLoggingForGet() throws Exception {
    RequestContext requestContext = mock(RequestContext.class);
    Request request = Request.forUri("/", "GET");
    when(requestContext.request()).thenReturn(request);

    Response<Object> response = awaitResponse(Middlewares.httpLogger()
        .apply(mockInnerHandler(requestContext))
        .invoke(requestContext));
    assertThat(response, hasStatus(withCode(Status.OK)));
  }

  @Test
  public void testAuditLoggingForPut() throws Exception {
    RequestContext requestContext = mock(RequestContext.class);
    Request request = Request.forUri("/", "PUT")
        .withPayload(ByteString.encodeUtf8("hello"));
    when(requestContext.request()).thenReturn(request);

    Response<Object> response = awaitResponse(Middlewares.httpLogger()
        .apply(mockInnerHandler(requestContext))
        .invoke(requestContext));
    assertThat(response, hasStatus(withCode(Status.OK)));
  }

  @Test
  public void testAuditLoggingForPutWithBrokenAuthorization()
      throws InterruptedException, ExecutionException, TimeoutException {
    RequestContext requestContext = mock(RequestContext.class);
    Request request = Request.forUri("/", "PUT")
        .withHeader(HttpHeaders.AUTHORIZATION, "Bearer broken")
        .withPayload(ByteString.encodeUtf8("hello"));
    when(requestContext.request()).thenReturn(request);

    Response<Object> response = Middlewares.httpLogger().and(Middlewares.exceptionHandler())
        .apply(mockInnerHandler(requestContext))
        .invoke(requestContext)
        .toCompletableFuture().get(5, SECONDS);

    assertThat(response, hasStatus(withCode(Status.BAD_REQUEST)));
  }

  @Test
  public void testExceptionHandler()
      throws InterruptedException, ExecutionException, TimeoutException {
    RequestContext requestContext = mock(RequestContext.class);
    Request request = Request.forUri("/", "GET");
    when(requestContext.request()).thenReturn(request);

    Response<Object> response = Middlewares.exceptionHandler()
        .apply(mockInnerHandler(requestContext, new ResponseException(Response.forStatus(Status.IM_A_TEAPOT))))
        .invoke(requestContext)
        .toCompletableFuture().get(5, SECONDS);

    assertThat(response, hasStatus(withCode(Status.IM_A_TEAPOT)));
  }

  @Test
  public void testExceptionHandlerForAsyncException()
      throws InterruptedException, ExecutionException, TimeoutException {
    RequestContext requestContext = mock(RequestContext.class);
    Request request = Request.forUri("/", "GET");
    when(requestContext.request()).thenReturn(request);

    CompletableFuture<?> failedFuture = new CompletableFuture();
    failedFuture.completeExceptionally(new ResponseException(Response.forStatus(Status.IM_A_TEAPOT)));

    Response<Object> response = Middlewares.exceptionHandler()
        .apply(mockInnerHandler(requestContext, failedFuture))
        .invoke(requestContext)
        .toCompletableFuture().get(5, SECONDS);

    assertThat(response, hasStatus(withCode(Status.IM_A_TEAPOT)));
  }

  @Test
  public void testAuthValidatorForGet() throws Exception {
    RequestContext requestContext = mock(RequestContext.class);
    Request request = Request.forUri("/", "GET");
    when(requestContext.request()).thenReturn(request);

    Response<Object> response = awaitResponse(Middlewares.authValidator()
                                                  .apply(mockInnerHandler(requestContext))
                                                  .invoke(requestContext));
    assertThat(response, hasStatus(withCode(Status.OK)));
  }

  @Test
  public void testAuthValidatorForPut() throws Exception {
    RequestContext requestContext = mock(RequestContext.class);
    Request request = Request.forUri("/", "PUT")
        .withPayload(ByteString.encodeUtf8("hello"));
    when(requestContext.request()).thenReturn(request);

    Response<Object> response = awaitResponse(Middlewares.authValidator()
                                                  .apply(mockInnerHandler(requestContext))
                                                  .invoke(requestContext));
    assertThat(response, hasStatus(withCode(Status.UNAUTHORIZED)));
  }

  public static <T> Response<T> awaitResponse(CompletionStage<Response<T>> completionStage)
      throws Exception {
    return completionStage.toCompletableFuture().get(5, SECONDS);
  }

  @AutoValue
  public static abstract class TestStruct {

    @JsonProperty
    public abstract String foo();

    @JsonProperty
    public abstract Inner innerObject();
  }

  @AutoValue
  public static abstract class Inner {

    @JsonProperty
    public abstract String fieldNameConvention();

    @JsonProperty
    public abstract TestEnum enumField();
  }

  public enum TestEnum {
    ENUM_VALUE;

    @JsonValue
    public String toJson() {
      return name().toLowerCase();
    }

    @JsonCreator
    public static TestEnum fromJson(String json) {
      return valueOf(json.toUpperCase());
    }
  }
}
