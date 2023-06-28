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

import static com.spotify.apollo.test.unit.ResponseMatchers.hasHeader;
import static com.spotify.apollo.test.unit.ResponseMatchers.hasStatus;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.belongsToFamily;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withCode;
import static com.spotify.apollo.test.unit.StatusTypeMatchers.withReasonPhrase;
import static com.spotify.styx.util.StringIsValidUuid.isValidUuid;
import static io.opencensus.trace.AttributeValue.stringAttributeValue;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.auto.value.AutoValue;
import com.google.common.net.HttpHeaders;
import com.spotify.apollo.Client;
import com.spotify.apollo.Request;
import com.spotify.apollo.RequestContext;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.entity.EntityMiddleware;
import com.spotify.apollo.entity.JacksonEntityCodec;
import com.spotify.apollo.request.RequestContexts;
import com.spotify.apollo.request.RequestMetadataImpl;
import com.spotify.apollo.route.AsyncHandler;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.util.ClassEnforcer;
import com.spotify.styx.util.MockSpan;
import io.opencensus.trace.SpanBuilder;
import io.opencensus.trace.Tracer;
import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import okio.ByteString;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

/**
 * Tests Middlewares
 */
@RunWith(MockitoJUnitRunner.class)
public class MiddlewaresTest {

  @Rule public ExpectedException exception = ExpectedException.none();

  @Mock private Logger log;
  @Mock private RequestAuthenticator authenticator;
  @Mock private GoogleIdToken idToken;
  @Mock private GoogleIdToken.Payload idTokenPayload;
  @Mock private Tracer tracer;
  @Mock private SpanBuilder spanBuilder;

  private static final TestStruct TEST_STRUCT = new AutoValue_MiddlewaresTest_TestStruct(
      "blah", new AutoValue_MiddlewaresTest_Inner("bloh", TestEnum.ENUM_VALUE));

  @Before
  public void setUp() throws Exception {
    when(authenticator.authenticate(any())).thenReturn(Optional::empty);
  }

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

  @Test
  public void shouldNotBeConstructable() throws ReflectiveOperationException {
    assertThat(ClassEnforcer.assertNotInstantiable(Middlewares.class), is(true));
  }

  @Test
  public void testJson() throws Exception {
    AsyncHandler<Response<ByteString>> outerHandler = Middlewares.json().apply(
        rc -> Response.forPayload(TEST_STRUCT)
    );

    CompletionStage<Response<ByteString>> completionStage = outerHandler.invoke(
        RequestContexts.create(mock(Request.class),
            mock(Client.class),
            Collections.emptyMap(),
            System.nanoTime(),
            RequestMetadataImpl.create(Instant.EPOCH,
                Optional.empty(), Optional.empty())));

    assertThat(completionStage.toCompletableFuture().get().payload().orElseThrow().utf8(),
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
        RequestContexts.create(mock(Request.class), mock(Client.class), Collections.emptyMap(), System.nanoTime(),
            RequestMetadataImpl.create(Instant.EPOCH, Optional.empty(), Optional.empty())));

    assertThat(completionStage.toCompletableFuture().get().payload().orElseThrow().utf8(),
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
        RequestContexts.create(mock(Request.class),
            mock(Client.class),
            Collections.emptyMap(),
            System.nanoTime(),
            RequestMetadataImpl.create(Instant.EPOCH,
                Optional.empty(), Optional.empty())));

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
        RequestContexts.create(mock(Request.class),
            mock(Client.class),
            Collections.emptyMap(),
            System.nanoTime(),
            RequestMetadataImpl.create(Instant.EPOCH,
                Optional.empty(), Optional.empty())));

    Response<ByteString> response = completionStage.toCompletableFuture().get();
    assertThat(response.payload().isPresent(), is(false));
    assertThat(response.status().family(), is(Status.Family.SERVER_ERROR));
  }

  @Test
  public void testValidClientNoBlacklist() throws Exception {
    final Supplier<Set<String>> supplier = Set::of;
    RequestContext requestContext = mockRequestContext(true);
    Response<Object> response = awaitResponse(Middlewares.clientValidator(supplier)
        .apply(mockInnerHandler(requestContext))
        .invoke(requestContext));
    assertThat(response, hasStatus(withCode(Status.OK)));
  }

  @Test
  public void testValidClientEmptyBlacklist() throws Exception {
    final Supplier<Set<String>> supplier = Set::of;
    RequestContext requestContext = mockRequestContext(true);
    Response<Object> response = awaitResponse(Middlewares.clientValidator(supplier)
        .apply(mockInnerHandler(requestContext))
        .invoke(requestContext));
    assertThat(response, hasStatus(withCode(Status.OK)));
  }

  @Test
  public void testValidClient() throws Exception {
    final Supplier<Set<String>> supplier = () -> Set.of("Styx CLI 0.1.0");
    RequestContext requestContext = mockRequestContext(true);
    Response<Object> response = awaitResponse(Middlewares.clientValidator(supplier)
        .apply(mockInnerHandler(requestContext))
        .invoke(requestContext));
    assertThat(response, hasStatus(withCode(Status.OK)));
  }

  @Test
  public void testValidClientNoHeader() throws Exception {
    final Supplier<Set<String>> supplier = () -> Set.of("Styx CLI 0.1.0");
    RequestContext requestContext = mockRequestContext(false);
    Response<Object> response = awaitResponse(Middlewares.clientValidator(supplier)
        .apply(mockInnerHandler(requestContext))
        .invoke(requestContext));
    assertThat(response, hasStatus(withCode(Status.OK)));
  }

  @Test
  public void testInvalidClient() throws Exception {
    final Supplier<Set<String>> supplier = () -> Set.of("Styx CLI 0.1.1");
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

    Response<Object> response = awaitResponse(Middlewares.httpLogger(authenticator)
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

    Response<Object> response = awaitResponse(Middlewares.httpLogger(authenticator)
        .apply(mockInnerHandler(requestContext))
        .invoke(requestContext));
    assertThat(response, hasStatus(withCode(Status.OK)));
  }

  @Test
  public void testExceptionAndRequestIdHandlerOnImmediateResponseException()
      throws InterruptedException, ExecutionException, TimeoutException {
    final RequestContext requestContext = mock(RequestContext.class);
    final Request request = Request.forUri("/", "GET");
    final AtomicReference<String> requestId = new AtomicReference<>();
    when(requestContext.request()).thenReturn(request);

    Response<Object> response = Middlewares.exceptionAndRequestIdHandler()
        .apply(rc -> {
          requestId.set(MDC.get("request-id"));
          LoggerFactory.getLogger(MiddlewaresTest.class).error("I'm a teapot!");
          throw new ResponseException(Response.forStatus(Status.IM_A_TEAPOT));
        })
        .invoke(requestContext)
        .toCompletableFuture().get(5, SECONDS);

    assertThat(response, hasStatus(is(Status.IM_A_TEAPOT)));
    assertThat(response, hasHeader("X-Styx-Request-Id", is(requestId.get())));
    assertThat(requestId.get(), isValidUuid());
  }

  @Test
  public void testExceptionAndRequestIdHandlerOnFutureResponseException()
      throws InterruptedException, ExecutionException, TimeoutException {
    final RequestContext requestContext = mock(RequestContext.class);
    final Request request = Request.forUri("/", "GET");
    final AtomicReference<String> requestId = new AtomicReference<>();
    when(requestContext.request()).thenReturn(request);

    Response<Object> response = Middlewares.exceptionAndRequestIdHandler()
        .apply(rc -> {
          requestId.set(MDC.get("request-id"));
          LoggerFactory.getLogger(MiddlewaresTest.class).error("I'm a teapot!");
          final CompletableFuture<Response<Object>> failure = new CompletableFuture<>();
          LoggerFactory.getLogger(MiddlewaresTest.class).error("deadbeef");
          failure.completeExceptionally(new ResponseException(Response.forStatus(Status.IM_A_TEAPOT)));
          return failure;
        })
        .invoke(requestContext)
        .toCompletableFuture().get(5, SECONDS);

    assertThat(response, hasStatus(is(Status.IM_A_TEAPOT)));
    assertThat(response, hasHeader("X-Styx-Request-Id", is(requestId.get())));
    assertThat(requestId.get(), isValidUuid());
  }

  @Test
  public void testExceptionAndRequestIdHandlerOnImmediateUnhandledException()
      throws InterruptedException, ExecutionException, TimeoutException {
    assertImmediateUnhandledExceptionIsHandled(
        () -> { throw new RuntimeException("fubar", new IOException("deadbeef")); },
        "RuntimeException: fubar: IOException: deadbeef");
  }

  @Test
  public void testExceptionAndRequestIdHandlerOnImmediateUnhandledError()
      throws InterruptedException, ExecutionException, TimeoutException {
    assertImmediateUnhandledExceptionIsHandled(
        () -> { throw new AssertionError("fubar", new IOException("deadbeef")); },
        "AssertionError: fubar: IOException: deadbeef");
  }

  private void assertImmediateUnhandledExceptionIsHandled(Runnable cause, String expectedMessage)
      throws InterruptedException, ExecutionException, TimeoutException {
    final RequestContext requestContext = mock(RequestContext.class);
    final Request request = Request.forUri("/", "GET");
    final AtomicReference<String> requestId = new AtomicReference<>();
    when(requestContext.request()).thenReturn(request);

    Response<Object> response = Middlewares.exceptionAndRequestIdHandler()
        .apply(rc -> {
          requestId.set(MDC.get("request-id"));
          LoggerFactory.getLogger(MiddlewaresTest.class).error("deadbeef");
          cause.run();
          return null;
        })
        .invoke(requestContext)
        .toCompletableFuture().get(5, SECONDS);

    assertThat(response, hasStatus(withCode(Status.INTERNAL_SERVER_ERROR)));
    assertThat(response, hasStatus(withReasonPhrase(is(
        "Internal Server Error (Request ID: " + requestId.get() + "): " + expectedMessage))));
    assertThat(response, hasHeader("X-Styx-Request-Id", is(requestId.get())));
    assertThat(requestId.get(), isValidUuid());
  }

  @Test
  public void testExceptionAndRequestIdHandlerOnFutureUnhandledException()
      throws InterruptedException, ExecutionException, TimeoutException {
    final RequestContext requestContext = mock(RequestContext.class);
    final Request request = Request.forUri("/", "GET");
    final AtomicReference<String> requestId = new AtomicReference<>();
    when(requestContext.request()).thenReturn(request);

    Response<Object> response = Middlewares.exceptionAndRequestIdHandler()
        .apply(rc -> {
          requestId.set(MDC.get("request-id"));
          final CompletableFuture<Response<Object>> failure = new CompletableFuture<>();
          LoggerFactory.getLogger(MiddlewaresTest.class).error("deadbeef");
          failure.completeExceptionally(new RuntimeException("fubar", new IOException("deadbeef")));
          return failure;
        })
        .invoke(requestContext)
        .toCompletableFuture().get(5, SECONDS);

    assertThat(response, hasStatus(withCode(Status.INTERNAL_SERVER_ERROR)));
    assertThat(response, hasStatus(withReasonPhrase(is(
        "Internal Server Error (Request ID: " + requestId.get() + "): RuntimeException: fubar: IOException: deadbeef"))));
    assertThat(response, hasHeader("X-Styx-Request-Id", is(requestId.get())));
    assertThat(requestId.get(), isValidUuid());
  }

  @Test
  public void testExceptionAndRequestIdHandlerOnResponse()
      throws InterruptedException, ExecutionException, TimeoutException {
    final RequestContext requestContext = mock(RequestContext.class);
    final Request request = Request.forUri("/", "GET");
    final AtomicReference<String> requestId = new AtomicReference<>();
    when(requestContext.request()).thenReturn(request);

    Response<Object> response = Middlewares.exceptionAndRequestIdHandler()
        .apply(rc -> {
          requestId.set(MDC.get("request-id"));
          LoggerFactory.getLogger(MiddlewaresTest.class).info("I'm OK!");
          return completedFuture(Response.forStatus(Status.OK));
        })
        .invoke(requestContext)
        .toCompletableFuture().get(5, SECONDS);

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertThat(response, hasHeader("X-Styx-Request-Id", is(requestId.get())));
    assertThat(requestId.get(), isValidUuid());
  }
  @Test
  public void testExceptionAndRequestIdHandlerAcceptsRequestIdHeader()
      throws InterruptedException, ExecutionException, TimeoutException {
    final RequestContext requestContext = mock(RequestContext.class);
    final String requestId = UUID.randomUUID().toString();
    final Request request = Request.forUri("/", "GET")
        .withHeader("X-Styx-Request-Id", requestId);
    final AtomicReference<String> propagatedRequestId = new AtomicReference<>();
    when(requestContext.request()).thenReturn(request);

    Response<Object> response = Middlewares.exceptionAndRequestIdHandler()
        .apply(rc -> {
          propagatedRequestId.set(MDC.get("request-id"));
          LoggerFactory.getLogger(MiddlewaresTest.class).info("I'm OK!");
          return completedFuture(Response.forStatus(Status.OK));
        })
        .invoke(requestContext)
        .toCompletableFuture().get(5, SECONDS);

    assertThat(response, hasStatus(withCode(Status.OK)));
    assertThat(response, hasHeader("X-Styx-Request-Id", is(requestId)));
    assertThat(propagatedRequestId.get(), is(requestId));
  }

  @Test
  public void testExceptionHandlerForAsyncException()
      throws InterruptedException, ExecutionException, TimeoutException {
    RequestContext requestContext = mock(RequestContext.class);
    Request request = Request.forUri("/", "GET");
    when(requestContext.request()).thenReturn(request);

    CompletableFuture<?> failedFuture = new CompletableFuture();
    failedFuture.completeExceptionally(new ResponseException(Response.forStatus(Status.IM_A_TEAPOT)));

    Response<Object> response = Middlewares.exceptionAndRequestIdHandler()
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

    Response<Object> response = awaitResponse(Middlewares.authenticator(authenticator)
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

    Response<Object> response = awaitResponse(Middlewares.authenticator(authenticator)
                                                  .apply(mockInnerHandler(requestContext))
                                                  .invoke(requestContext));
    assertThat(response, hasStatus(withCode(Status.UNAUTHORIZED)));
  }

  @Test
  public void testAuthedProvidesIdToken() {
    final RequestContext requestContext = mock(RequestContext.class);
    final Request request = Request.forUri("/", "PUT")
        .withPayload(ByteString.encodeUtf8("hello"))
        .withHeader(HttpHeaders.AUTHORIZATION, "Bearer s3cr3tp455w0rd");
    when(requestContext.request()).thenReturn(request);
    when(authenticator.authenticate(any())).thenReturn(() -> Optional.of(idToken));

    final Response<Boolean> response = Middlewares.<Boolean>authed(authenticator)
        .apply(rc -> ac -> Response.forPayload(ac.user().map(idToken::equals).orElse(false)))
        .invoke(requestContext);

    assertThat(response.payload(), is(Optional.of(true)));
  }

  @Test
  public void testAuthedEntityProvidesIdToken() throws Exception {

    final EntityMiddleware em = EntityMiddleware.forCodec(JacksonEntityCodec.forMapper(Json.OBJECT_MAPPER));

    final RequestContext requestContext = mock(RequestContext.class);
    final Request request = Request.forUri("/", "PUT")
        .withPayload(ByteString.encodeUtf8("\"hello \""))
        .withHeader(HttpHeaders.AUTHORIZATION, "Bearer s3cr3tp455w0rd");
    when(requestContext.request()).thenReturn(request);

    final String email = "foo@bar.net";

    when(authenticator.authenticate(any())).thenReturn(() -> Optional.of(idToken));
    when(idToken.getPayload()).thenReturn(idTokenPayload);
    when(idTokenPayload.getEmail()).thenReturn(email);

    final Response<ByteString> response = Middlewares.authedEntity(authenticator, em.response(String.class))
        .apply(ac -> rc -> payload -> Response.forPayload(payload + ac.user().orElseThrow().getPayload().getEmail()))
        .invoke(requestContext);

    assertThat(Json.deserialize(response.payload().orElseThrow(), String.class), is("hello " + email));
  }

  @Test
  public void testHttpLoggerHidesAuthHeader() throws Exception {
    RequestContext requestContext = mock(RequestContext.class);
    Request request = Request.forUri("/", "PUT")
        .withPayload(ByteString.encodeUtf8("hello"))
        .withHeader(HttpHeaders.AUTHORIZATION, "Bearer s3cr3tp455w0rd");
    when(requestContext.request()).thenReturn(request);

    String email = "foo@bar.net";

    when(authenticator.authenticate(any())).thenReturn(() -> Optional.of(idToken));
    when(idToken.getPayload()).thenReturn(idTokenPayload);
    when(idTokenPayload.getEmail()).thenReturn(email);

    awaitResponse(Middlewares.httpLogger(log, authenticator)
        .apply(mockInnerHandler(requestContext))
        .invoke(requestContext));

    verify(log).info("{}{} {} by {} with headers {} parameters {} and payload {}",
        "[AUDIT] ",
        request.method(),
        request.uri(),
        email,
        Map.of(HttpHeaders.AUTHORIZATION, "<hidden>"),
        Map.of(),
        request.payload().orElseThrow().utf8());
  }

  @Test
  public void testTracingSync() throws Exception {
    final RequestContext requestContext = mock(RequestContext.class);
    final Request request = Request.forUri("/bar", "GET");
    final MockSpan span = new MockSpan();

    when(requestContext.request()).thenReturn(request);
    when(tracer.spanBuilder("foo-service//bar")).thenReturn(spanBuilder);
    when(spanBuilder.startSpan()).thenReturn(span);

    awaitResponse(Middlewares.tracer(tracer, "foo-service")
        .apply(rc -> completedFuture(Response.ok()))
        .invoke(requestContext));

    verify(tracer).spanBuilder("foo-service//bar");
    verify(spanBuilder).startSpan();

    assertThat(span.attributes.get("method"), is(stringAttributeValue("GET")));
    assertThat(span.attributes.get("uri"), is(stringAttributeValue("/bar")));

    assertThat(span.ended, is(true));
    assertThat(span.status, is(nullValue()));
  }

  @Test
  public void testTracingSyncError() {
    final RequestContext requestContext = mock(RequestContext.class);
    final Request request = Request.forUri("/bar", "GET");
    final MockSpan span = new MockSpan();

    when(requestContext.request()).thenReturn(request);
    when(tracer.spanBuilder("foo-service//bar")).thenReturn(spanBuilder);
    when(spanBuilder.startSpan()).thenReturn(span);

    try {
      awaitResponse(Middlewares.tracer(tracer, "foo-service")
          .apply(rc -> {
            throw new RuntimeException();
          })
          .invoke(requestContext));
      fail();
    } catch (Exception ignore) {
    }

    assertThat(span.ended, is(true));
    assertThat(span.status, is(io.opencensus.trace.Status.UNKNOWN));
  }

  @Test
  public void testTracingAsync() throws ExecutionException, InterruptedException {
    final RequestContext requestContext = mock(RequestContext.class);
    final Request request = Request.forUri("/bar", "GET");
    final MockSpan span = new MockSpan();

    when(requestContext.request()).thenReturn(request);

    when(tracer.spanBuilder("foo-service//bar")).thenReturn(spanBuilder);
    when(spanBuilder.startSpan()).thenReturn(span);

    final CompletableFuture<Response<Object>> handlerFuture = new CompletableFuture<>();

    final CompletableFuture<Response<Object>> responseFuture = Middlewares.tracer(tracer, "foo-service")
        .apply(rc -> handlerFuture)
        .invoke(requestContext)
        .toCompletableFuture();

    // Request handling is not complete, span should not be ended
    assertThat(span.ended, is(false));

    // Complete request handling future and wait for response
    handlerFuture.complete(Response.ok());
    responseFuture.get();

    // Span should now be ended
    assertThat(span.ended, is(true));
    assertThat(span.status, is(nullValue()));
  }

  @Test
  public void testTracingAsyncError() throws InterruptedException {
    final RequestContext requestContext = mock(RequestContext.class);
    final Request request = Request.forUri("/bar", "GET");
    final MockSpan span = new MockSpan();

    when(requestContext.request()).thenReturn(request);

    when(tracer.spanBuilder("foo-service//bar")).thenReturn(spanBuilder);
    when(spanBuilder.startSpan()).thenReturn(span);

    final CompletableFuture<Response<Object>> handlerFuture = new CompletableFuture<>();

    final CompletableFuture<Response<Object>> responseFuture = Middlewares.tracer(tracer, "foo-service")
        .apply(rc -> handlerFuture)
        .invoke(requestContext)
        .toCompletableFuture();

    // Request handling is not complete, span should not be ended
    assertThat(span.ended, is(false));

    // Fail request handling future and wait for response
    handlerFuture.completeExceptionally(new RuntimeException());
    try {
      responseFuture.get();
    } catch (ExecutionException ignore) {
    }

    // Span should now be ended
    assertThat(span.ended, is(true));
    assertThat(span.status, is(io.opencensus.trace.Status.UNKNOWN));
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
