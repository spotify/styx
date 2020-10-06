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

import static com.google.api.gax.rpc.StatusCode.Code.PERMISSION_DENIED;

import com.google.api.client.auth.oauth2.ClientParametersAuthentication;
import com.google.api.client.auth.oauth2.RefreshTokenRequest;
import com.google.api.client.auth.oauth2.TokenRequest;
import com.google.api.client.auth.oauth2.TokenResponse;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.UriTemplate;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.webtoken.JsonWebSignature;
import com.google.api.client.json.webtoken.JsonWebSignature.Header;
import com.google.api.client.json.webtoken.JsonWebToken.Payload;
import com.google.api.gax.rpc.ApiException;
import com.google.api.services.oauth2.Oauth2;
import com.google.api.services.oauth2.model.Tokeninfo;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.ComputeEngineCredentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.auth.oauth2.UserCredentials;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.IamCredentialsSettings;
import com.google.cloud.iam.credentials.v1.ServiceAccountName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class GoogleIdTokenAuth {

  private static final Logger log = LoggerFactory.getLogger(GoogleIdTokenAuth.class);
  private static final JsonFactory JSON_FACTORY = Utils.getDefaultJsonFactory();
  private static final String DEFAULT_GCE_METADATA_HOST = "169.254.169.254";
  private static final String GCE_METADATA_IDENTITY_PATH =
      "/computeMetadata/v1/instance/service-accounts/default/identity{?audience,format}";
  private static final Pattern SERVICE_ACCOUNT_PATTERN = Pattern.compile("^.+\\.gserviceaccount\\.com$");

  private final HttpTransport httpTransport;
  private final Optional<GoogleCredentials> credentials;

  private GoogleIdTokenAuth(HttpTransport httpTransport, Optional<GoogleCredentials> credentials) {
    this.httpTransport = Objects.requireNonNull(httpTransport, "httpTransport");
    this.credentials = Objects.requireNonNull(credentials, "credentials");
  }

  Optional<String> getToken(String targetAudience)
      throws IOException, GeneralSecurityException {
    return credentials.isPresent()
           ? Optional.of(getToken(targetAudience, credentials.get()))
           : Optional.empty();
  }

  private String getToken(String targetAudience, GoogleCredentials credentials)
      throws IOException, GeneralSecurityException {
    if (credentials instanceof ServiceAccountCredentials) {
      return getServiceAccountToken((ServiceAccountCredentials) credentials, targetAudience);
    } else if (credentials instanceof UserCredentials) {
      return getUserToken((UserCredentials) credentials);
    } else if (credentials instanceof ComputeEngineCredentials) {
      return getDefaultGCEIdToken(targetAudience);
    } else if (credentials instanceof ImpersonatedCredentials) {
      return getImpersonatedIdToken((ImpersonatedCredentials) credentials, targetAudience);
    } else {
      // Assume a type of service account credential
      return getServiceAccountIdTokenUsingAccessToken(credentials, targetAudience);
    }
  }

  private String getDefaultGCEIdToken(String targetAudience) throws IOException {
    // https://cloud.google.com/compute/docs/instances/verifying-instance-identity#request_signature
    final String metadataHost = System.getenv().getOrDefault("GCE_METADATA_HOST", DEFAULT_GCE_METADATA_HOST);
    final String uriTemplate = "http://" + metadataHost + GCE_METADATA_IDENTITY_PATH;
    final String identityUri = UriTemplate.expand(uriTemplate, ImmutableMap.of(
        "audience", targetAudience,
        "format", "full"),
        false);
    return httpTransport.createRequestFactory()
        .buildGetRequest(new GenericUrl(identityUri))
        .setHeaders(new HttpHeaders().set("Metadata-Flavor", "Google"))
        .execute()
        .parseAsString();
  }

  private String getServiceAccountToken(ServiceAccountCredentials credential, String targetAudience)
      throws IOException, GeneralSecurityException {
    log.debug("Fetching service account id token for {}", credential.getAccount());
    final TokenRequest request = new TokenRequest(
        this.httpTransport, JSON_FACTORY,
        new GenericUrl(credential.getTokenServerUri()),
        "urn:ietf:params:oauth:grant-type:jwt-bearer");
    final Header header = jwtHeader();
    final Payload payload = jwtPayload(
        targetAudience, credential.getAccount(), credential.getTokenServerUri().toString());
    request.put("assertion", JsonWebSignature.signUsingRsaSha256(
        credential.getPrivateKey(), JSON_FACTORY, header, payload));
    final TokenResponse response = request.execute();
    return (String) response.get("id_token");
  }

  private String getImpersonatedIdToken(ImpersonatedCredentials credentials, String targetAudience) throws IOException {
    final String serviceAccount = credentials.toBuilder().getTargetPrincipal();
    return getServiceAccountIdTokenUsingAccessToken(credentials, serviceAccount, targetAudience);
  }

  private String getServiceAccountIdTokenUsingAccessToken(GoogleCredentials credentials, String targetAudience)
      throws IOException {
    final Oauth2 oauth2 = new Oauth2.Builder(httpTransport, JSON_FACTORY, null)
        .build();
    final AccessToken accessToken = accessToken(withScopes(credentials,
        ImmutableList.of("https://www.googleapis.com/auth/userinfo.email")));
    final Tokeninfo info = oauth2.tokeninfo()
        .setAccessToken(accessToken.getTokenValue())
        .execute();
    final String principal = info.getEmail();
    if (principal == null) {
      throw new IOException("Unable to look up principal email, credentials missing email scope?");
    }
    if (!SERVICE_ACCOUNT_PATTERN.matcher(principal).matches()) {
      throw new IOException("Principal is not a service account, unable to acquire id token: " + principal);
    }
    return getServiceAccountIdTokenUsingAccessToken(credentials, principal, targetAudience);
  }

  private String getServiceAccountIdTokenUsingAccessToken(GoogleCredentials credentials,
                                                          String serviceAccount, String targetAudience)
      throws IOException {
    try (IamCredentialsClient iamCredentialsClient =
             IamCredentialsClient
                 .create(IamCredentialsSettings.newBuilder().setCredentialsProvider(() -> credentials).build())) {
      var serviceAccountName = ServiceAccountName.of("-", serviceAccount);
      var idTokenResponse = iamCredentialsClient.generateIdToken(serviceAccountName, List.of(),
          targetAudience, true);
      return idTokenResponse.getToken();
    } catch (ApiException e) {
      if (e.getStatusCode().getCode() == PERMISSION_DENIED) {
        throw new IOException(
            "Unable to get ID token, missing Service Account Token Creator role for self on "
            + serviceAccount + " or IAM Service Account Credentials API not enabled?", e);
      }
      throw e;
    }
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

  private String getUserToken(UserCredentials credentials) throws IOException {
    log.debug("Fetching user id token");
    final TokenRequest request = new RefreshTokenRequest(
        this.httpTransport, JSON_FACTORY,
        new GenericUrl(credentials.toBuilder().getTokenServerUri()),
        credentials.getRefreshToken())
        .setClientAuthentication(new ClientParametersAuthentication(
            credentials.getClientId(), credentials.getClientSecret()))
        .setRequestInitializer(new HttpCredentialsAdapter(credentials));
    final TokenResponse response = request.execute();
    return (String) response.get("id_token");
  }

  private static AccessToken accessToken(GoogleCredentials credentials) throws IOException {
    if (credentials.getAccessToken() == null) {
      credentials.refresh();
    }
    return credentials.getAccessToken();
  }

  private static GoogleCredentials withScopes(GoogleCredentials credentials, Collection<String> scopes) {
    if (!credentials.createScopedRequired()) {
      return credentials;
    }
    return credentials.createScoped(scopes);
  }

  static GoogleIdTokenAuth ofDefaultCredential() {
    try {
      return new GoogleIdTokenAuth(Utils.getDefaultTransport(),
          Optional.of(GoogleCredentials.getApplicationDefault()));
    } catch (IOException e) {
      return of(Optional.empty());
    }
  }

  public static GoogleIdTokenAuth of(Optional<GoogleCredentials> credentials) {
    return of(Utils.getDefaultTransport(), credentials);
  }

  public static GoogleIdTokenAuth of(GoogleCredentials credentials) {
    return of(Utils.getDefaultTransport(), Optional.of(credentials));
  }

  private static GoogleIdTokenAuth of(HttpTransport transport, Optional<GoogleCredentials> credentials) {
    return new GoogleIdTokenAuth(transport, credentials);
  }
}
