/*-
 * -\-\-
 * Spotify Styx API Client
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

package com.spotify.styx.client;

/**
 * Exception used in case of API errors, i.e. API requests whose response has status code
 * different than 2xx.
 */
public class ApiErrorException extends RuntimeException {

  private final int code;
  private final boolean authenticated;
  private final String requestId;

  public ApiErrorException(String message, int code, boolean authenticated, String requestId) {
    super(message);
    this.code = code;
    this.authenticated = authenticated;
    this.requestId = requestId;
  }

  public int getCode() {
    return code;
  }

  public boolean isAuthenticated() {
    return authenticated;
  }

  public String getRequestId() {
    return requestId;
  }

  @Override
  public String getMessage() {
    final String message = super.getMessage();
    return requestId == null || message.contains(requestId)
        ? message
        : message + " (Request ID: " + requestId + ")";
  }
}
