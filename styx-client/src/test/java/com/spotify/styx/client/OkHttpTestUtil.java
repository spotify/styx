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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.spotify.styx.serialization.Json;
import java.io.IOException;
import java.util.Objects;
import okhttp3.MediaType;
import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okio.Buffer;
import okio.ByteString;

class OkHttpTestUtil {
  static final MediaType APPLICATION_JSON =
      Objects.requireNonNull(MediaType.parse("application/json"));

  private OkHttpTestUtil() {
  }

  static ByteString bytesOfRequestBody(Request request) {
    final Buffer sink = new Buffer();
    try {
      var body = request.body();
      assert body != null;
      body.writeTo(sink);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return ByteString.of(sink.readByteArray());
  }

  static Response.Builder responseBuilder(Request request, int code) {
    return new Response.Builder()
        .request(request)
        .protocol(Protocol.HTTP_1_1)
        .message("HTTP " + code)
        .code(code);
  }

  static Response.Builder responseBuilder(int code) {
    return responseBuilder(new Request.Builder().url("http://example.org").build(), code);
  }

  static Response.Builder responseBuilder(Request request, int code, Object body) throws JsonProcessingException {
    return responseBuilder(request, code)
        .body(ResponseBody.create(APPLICATION_JSON, Json.serialize(body).toByteArray()));
  }

  static Response.Builder responseBuilder(int code, Object body) throws JsonProcessingException {
    return responseBuilder(code)
        .body(ResponseBody.create(APPLICATION_JSON, Json.serialize(body).toByteArray()));
  }

  static Response response(int code) {
    return responseBuilder(code).build();
  }

  static Response response(int code, Object body) throws JsonProcessingException {
    return responseBuilder(code, body).build();
  }
}
