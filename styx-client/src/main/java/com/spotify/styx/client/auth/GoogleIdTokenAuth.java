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
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.UriTemplate;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.api.client.json.webtoken.JsonWebSignature.Header;
import com.google.api.client.json.webtoken.JsonWebToken.Payload;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Objects;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GoogleIdTokenAuth {
  private static final Logger log = LoggerFactory.getLogger(GoogleIdTokenAuth.class);
  private static final JsonFactory JSON_FACTORY = Utils.getDefaultJsonFactory();

  protected final HttpTransport httpTransport;
  private final Optional<GoogleCredential> credential;

  private static class GCEDefaultIdTokenAuth extends GoogleIdTokenAuth {

    private static final String DEFAULT_GCE_METADATA_HOST = "http://169.254.169.254";
    private static final String GCE_METADATA_IDENTITY_PATH =
        "/computeMetadata/v1/instance/service-accounts/default/identity{?audience,format}";

    public GCEDefaultIdTokenAuth(HttpTransport httpTransport) {
      super(httpTransport, Optional.empty());
    }

    @Override
    public Optional<String> getToken(String targetAudience) throws IOException {
      // https://cloud.google.com/compute/docs/instances/verifying-instance-identity#request_signature
      final String metadataHost = System.getenv().getOrDefault("GCE_METADATA_HOST", DEFAULT_GCE_METADATA_HOST);
      final String uriTemplate = "http://" + metadataHost + GCE_METADATA_IDENTITY_PATH;
      final String identityUri = UriTemplate.expand(uriTemplate, ImmutableMap.of(
          "audience", targetAudience,
          "format", "full"),
          false);
      return Optional.of(httpTransport.createRequestFactory()
          .buildGetRequest(new GenericUrl(identityUri))
          .setHeaders(new HttpHeaders().set("Metadata-Flavor", "Google"))
          .execute()
          .parseAsString());
    }
  }

  GoogleIdTokenAuth(HttpTransport httpTransport, Optional<GoogleCredential> credential) {
    this.httpTransport = Objects.requireNonNull(httpTransport, "httpTransport");
    this.credential = Objects.requireNonNull(credential, "credential");
  }

  public Optional<String> getToken(String targetAudience)
      throws IOException, GeneralSecurityException {
    return credential.isPresent()
        ? Optional.of(getToken(targetAudience, credential.get()))
        : Optional.empty();
  }

  private String getToken(String targetAudience, GoogleCredential credential)
      throws IOException, GeneralSecurityException {
    if (credential.getServiceAccountId() != null) {
      // is a service account
      return getServiceAccountToken(credential, targetAudience);
    } else {
      // is a user
      return getUserToken(credential);
    }
  }

  private String getServiceAccountToken(GoogleCredential credential, String targetAudience)
      throws IOException, GeneralSecurityException {
    log.debug("Fetching service account id token for {}", credential.getServiceAccountId());
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
    log.debug("Fetching user id token");
    final TokenRequest request = new RefreshTokenRequest(
        this.httpTransport, JSON_FACTORY,
        new GenericUrl(credential.getTokenServerEncodedUrl()),
        credential.getRefreshToken())
        .setClientAuthentication(credential.getClientAuthentication())
        .setRequestInitializer(credential);
    final TokenResponse response = request.execute();
    return (String) response.get("id_token");
  }

  public static GoogleIdTokenAuth ofDefaultCredential() {
    try {
      if (isGCEDefault()) {
        return new GCEDefaultIdTokenAuth(Utils.getDefaultTransport());
      } else {
        return new GoogleIdTokenAuth(Utils.getDefaultTransport(),
            Optional.of(GoogleCredential.getApplicationDefault()));
      }
    } catch (IOException e) {
      return of(Optional.empty());
    }
  }

  public static GoogleIdTokenAuth of(Optional<GoogleCredential> credential) {
    return of(Utils.getDefaultTransport(), credential);
  }

  public static GoogleIdTokenAuth of(GoogleCredential credential) {
    return of(Utils.getDefaultTransport(), Optional.of(credential));
  }

  private static GoogleIdTokenAuth of(HttpTransport transport, Optional<GoogleCredential> credential) {
    return new GoogleIdTokenAuth(transport, credential);
  }

  private static boolean isGCEDefault() throws IOException {
    return GoogleCredentials.getApplicationDefault() instanceof ComputeEngineCredentials;
  }
}
