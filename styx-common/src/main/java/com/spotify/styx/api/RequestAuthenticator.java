/*-
 * -\-\-
 * Spotify Styx Common
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

package com.spotify.styx.api;

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.common.net.HttpHeaders;
import com.spotify.apollo.Request;
import com.spotify.apollo.Response;
import com.spotify.apollo.Status;
import com.spotify.styx.api.Middlewares.AuthContext;
import java.util.Optional;

/**
 * Authenticates incoming Styx API requests.
 */
public class RequestAuthenticator {

  private static final String BEARER_PREFIX = "Bearer ";

  private final Authenticator authenticator;

  public RequestAuthenticator(Authenticator authenticator) {
    this.authenticator = authenticator;
  }

  /**
   * Authentication an incoming Styx API request.
   * @param request The incoming request.
   * @return A {@link AuthContext} with the authentication result.
   * @throws ResponseException If the Authorization header does not have a Bearer prefix or if the token was invalid.
   */
  public AuthContext authenticate(Request request) {
    final boolean hasAuthHeader = request.header(HttpHeaders.AUTHORIZATION).isPresent();

    if (!hasAuthHeader) {
      return Optional::empty;
    }

    final String authHeader = request.header(HttpHeaders.AUTHORIZATION).get();
    if (!authHeader.startsWith(BEARER_PREFIX)) {
      throw new ResponseException(Response.forStatus(Status.BAD_REQUEST
          .withReasonPhrase("Authorization token must be of type Bearer")));
    }

    final GoogleIdToken googleIdToken;
    try {
      googleIdToken = authenticator.authenticate(authHeader.substring(BEARER_PREFIX.length()));
    } catch (IllegalArgumentException e) {
      throw new ResponseException(Response.forStatus(Status.BAD_REQUEST
          .withReasonPhrase("Failed to parse Authorization token")), e);
    }

    if (googleIdToken == null) {
      throw new ResponseException(Response.forStatus(Status.UNAUTHORIZED
          .withReasonPhrase("Authorization token is invalid")));
    }

    return () -> Optional.of(googleIdToken);
  }
}
