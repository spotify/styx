/*-
 * -\-\-
 * Spotify Styx Service Common
 * --
 * Copyright (C) 2016 - 2019 Spotify AB
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

import com.google.api.client.auth.oauth2.TokenRequest;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.json.webtoken.JsonWebToken;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.ServiceAccountName;
import java.io.IOException;
import java.security.PrivateKey;
import java.util.List;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A service account {@link GoogleCredential} that does not need a physical service account key.
 * <p>
 * Note that the service account must be granted the "Service Account Token Creator" role for itself.
 * <p>
 * The fundamental difference between this class and the regular {@link GoogleCredential} implementation is that
 * it calls the GCP IAM Service Account Credentials API {@code projects.serviceAccounts.signJwt} method in order to sign the access
 * token request assertion instead of using a local service account key to sign it.
 * <p>
 * As opposed to the {@link com.google.auth.oauth2.ImpersonatedCredentials}, this implementation allows specifying a
 * domain user to impersonate when accessing e.g. G Suite APIs.
 */
class ManagedServiceAccountKeyCredential extends GoogleCredential {

  private static final Logger log = LoggerFactory.getLogger(ManagedServiceAccountKeyCredential.class);

  private final IamCredentialsClient iamCredentialsClient;

  private ManagedServiceAccountKeyCredential(Builder builder) {
    super(builder);
    Objects.requireNonNull(getServiceAccountId(), "serviceAccountId");
    Objects.requireNonNull(getServiceAccountUser(), "serviceAccountUser");
    Objects.requireNonNull(getServiceAccountScopes(), "serviceAccountScopes");
    iamCredentialsClient = Objects.requireNonNull(builder.iamCredentialsClient,
        "iamCredentialsClient");
  }

  @Override
  protected TokenResponse executeRefreshToken() throws IOException {
    log.debug("Refreshing access token for {} using {} with scopes {}",
        getServiceAccountUser(), getServiceAccountId(), getServiceAccountScopes());

    var jwtPayload = jwtPayload();

    log.debug("Signing access token request jwt: {}", jwtPayload);

    var signedJwt = signJwt(getServiceAccountId(), jwtPayload);

    log.debug("Fetching access token using signed jwt for {}. ", getServiceAccountUser());

    var tokenResponse = requestToken(signedJwt);

    log.debug("Successfully fetched access token using signed jwt for {}", getServiceAccountUser());

    return tokenResponse;
  }

  private JsonWebToken.Payload jwtPayload() {
    var currentTime = System.currentTimeMillis();
    var payload = new JsonWebToken.Payload();
    payload.setIssuer(getServiceAccountId());
    payload.setAudience(getTokenServerEncodedUrl());
    payload.setIssuedAtTimeSeconds(currentTime / 1000);
    payload.setExpirationTimeSeconds(currentTime / 1000 + 3600);
    payload.setSubject(getServiceAccountUser());
    payload.put("scope", Joiner.on(' ').join(getServiceAccountScopes()));
    return payload;
  }

  private String signJwt(String serviceAccount, JsonWebToken.Payload payload) throws IOException {
    var serviceAccountName = ServiceAccountName.of("-", serviceAccount);
    var signJwtResponse = iamCredentialsClient.signJwt(serviceAccountName, List.of(),
        Utils.getDefaultJsonFactory().toString(payload));
    return signJwtResponse.getSignedJwt();
  }

  private TokenResponse requestToken(String signedJwt) throws IOException {
    var tokenRequest = new TokenRequest(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory(),
        new GenericUrl(getTokenServerEncodedUrl()), "urn:ietf:params:oauth:grant-type:jwt-bearer");
    tokenRequest.put("assertion", signedJwt);
    return tokenRequest.execute();
  }

  static class Builder extends GoogleCredential.Builder {

    private final IamCredentialsClient iamCredentialsClient;
    Builder(final IamCredentialsClient iamCredentialsClient) {
      this.iamCredentialsClient = Objects.requireNonNull(iamCredentialsClient, "iamCredentialsClient");
      setServiceAccountPrivateKey(DummyKey.INSTANCE);
    }

    @Override
    public GoogleCredential build() {
      return new ManagedServiceAccountKeyCredential(this);
    }
  }

  private static class DummyKey implements PrivateKey {

    private static final DummyKey INSTANCE = new DummyKey();

    @Override
    public String getAlgorithm() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getFormat() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] getEncoded() {
      throw new UnsupportedOperationException();
    }
  }
}
