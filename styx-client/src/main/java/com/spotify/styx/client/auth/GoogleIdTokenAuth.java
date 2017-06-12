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

package com.spotify.styx.client.auth;

import com.google.api.client.auth.oauth2.RefreshTokenRequest;
import com.google.api.client.auth.oauth2.TokenRequest;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.api.client.json.webtoken.JsonWebSignature.Header;
import com.google.api.client.json.webtoken.JsonWebToken.Payload;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.security.GeneralSecurityException;

public class GoogleIdTokenAuth {
  private final HttpTransport httpTransport;
  private static final JsonFactory JSON_FACTORY = Utils.getDefaultJsonFactory();

  public GoogleIdTokenAuth(HttpTransport httpTransport) {
    this.httpTransport = httpTransport;
  }

  public GoogleIdTokenAuth() {
    this(Utils.getDefaultTransport());
  }

  public String getToken() throws IOException, GeneralSecurityException {
    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    if (credential.getServiceAccountId() != null) {
      // is a service account
      if (credential.createScopedRequired()) {
        credential = credential.createScoped(ImmutableList.of(
            "https://www.googleapis.com/auth/cloud-platform",
            "email"));
      }
      return getServiceAccountToken(credential, "https://styx.spotify.net");
    } else {
      // is a user
      return getUserToken(credential);
    }
  }

  private String getServiceAccountToken(GoogleCredential credential, String targetAudience)
      throws IOException, GeneralSecurityException {
    final TokenRequest request = new TokenRequest(
        this.httpTransport, JSON_FACTORY,
        new GenericUrl(credential.getTokenServerEncodedUrl()),
        "urn:ietf:params:oauth:grant-type:jwt-bearer");
    final Header header = jwtHeader();
    final Payload payload = jwtPayload(
        targetAudience, credential.getServiceAccountId(), credential.getTokenServerEncodedUrl());
    request.put("assertion", JsonWebSignature.signUsingRsaSha256(
        credential.getServiceAccountPrivateKey(), JSON_FACTORY, header, payload));
    final TokenResponse response = request.execute();
    return (String) response.get("id_token");
  }

  private static Payload jwtPayload(String targetAudience, String serviceAccountId, String tokenServerUrl) {
    final Payload payload = new Payload();
    final long currentTime = System.currentTimeMillis();
    payload.put("target_audience", targetAudience);
    payload.setIssuer(serviceAccountId);
    payload.setAudience(tokenServerUrl);
    payload.setIssuedAtTimeSeconds(currentTime / 1000);
    payload.setExpirationTimeSeconds(currentTime / 1000 + 3600);
    return payload;
  }

  private static Header jwtHeader() {
    final Header header = new Header();
    header.setAlgorithm("RS256");
    header.setType("JWT");
    return header;
  }

  private String getUserToken(GoogleCredential credential) throws IOException {
    final TokenRequest request = new RefreshTokenRequest(
        this.httpTransport, JSON_FACTORY,
        new GenericUrl(credential.getTokenServerEncodedUrl()),
        credential.getRefreshToken())
        .setClientAuthentication(credential.getClientAuthentication())
        .setRequestInitializer(credential);
    final TokenResponse response = request.execute();
    return (String) response.get("id_token");
  }
}
