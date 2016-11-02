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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.auto.value.AutoValue;
import com.spotify.apollo.Client;
import com.spotify.apollo.Request;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.apollo.request.RequestContexts;
import com.spotify.apollo.route.AsyncHandler;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import okio.ByteString;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.Is;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Tests Middlewares
 */
public class MiddlewaresTest {

  private static final TestStruct TEST_STRUCT = new AutoValue_MiddlewaresTest_TestStruct(
      "blah", new AutoValue_MiddlewaresTest_Inner("bloh", TestEnum.ENUM_VALUE));

  @Test
  public void testJson() throws Exception {
    AsyncHandler<Response<ByteString>> outerHandler = Middlewares.json().apply(
        rc -> Response.forPayload(TEST_STRUCT)
    );

    CompletionStage<Response<ByteString>> completionStage = outerHandler.invoke(
        RequestContexts.create(Mockito.mock(Request.class), mock(Client.class), Collections.emptyMap()));

    MatcherAssert.assertThat(completionStage.toCompletableFuture().get().payload().get().utf8(),
                             Is.is(
            "{\"foo\":\"blah\"," +
            "\"inner_object\":{" +
              "\"field_name_convention\":\"bloh\"," +
              "\"enum_field\":\"enum_value\"}}"
        ));
  }

  @Test
  public void testJsonAsync() throws Exception {
    AsyncHandler<Response<ByteString>> outerHandler = Middlewares.jsonAsync().apply(
        rc -> CompletableFuture.completedFuture(Response.forPayload(TEST_STRUCT))
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
