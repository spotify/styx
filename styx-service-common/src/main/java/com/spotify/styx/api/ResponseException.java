/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 - 2017 Spotify AB
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

import com.google.common.base.Preconditions;
import com.spotify.apollo.Response;

/**
 * An exception that can be thrown to signal exceptional execution with a response
 */
class ResponseException extends RuntimeException {

  private final Response<?> response;

  ResponseException(Response<?> response) {
    this(response, null);
  }

  ResponseException(Response<?> response, Throwable cause) {
    super(cause);
    Preconditions.checkArgument(response.payload().isEmpty());
    this.response = response;
  }

  public <T> Response<T> getResponse() {
    //noinspection unchecked
    return (Response<T>) response;
  }
}
