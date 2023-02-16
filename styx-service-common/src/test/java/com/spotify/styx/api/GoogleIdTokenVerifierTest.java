/*-
 * -\-\-
 * Spotify Styx Service Common
 * --
 * Copyright (C) 2019 Spotify AB
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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.api.client.googleapis.auth.oauth2.GoogleIdToken;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.api.client.googleapis.auth.oauth2.GooglePublicKeysManager;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.json.GenericJson;
import com.google.api.client.json.webtoken.JsonWebSignature;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.time.Instant;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class GoogleIdTokenVerifierTest {
  private PrivateKey privateKey;
  private GoogleIdTokenVerifier verifier;

  @Before
  public void setUp() throws Exception {
    final var keyGen = KeyPairGenerator.getInstance("RSA");
    keyGen.initialize(571);
    KeyPair pair = keyGen.generateKeyPair();
    privateKey = pair.getPrivate();

    final var keysManager = new GooglePublicKeysManager(Utils.getDefaultTransport(), Utils.getDefaultJsonFactory());
    stubPublicKey(keysManager, pair.getPublic());

    verifier = new GoogleIdTokenVerifier(keysManager);
  }

  @Test
  public void shouldVerifyTokensFromComputeEngine() throws GeneralSecurityException, IOException {
    var parsedToken = verifier.verify(createToken());

    assertThat(parsedToken, is(notNullValue()));
  }

  private String createToken() throws GeneralSecurityException, IOException {
    var issuedAt = Instant.now().getEpochSecond();
    var expiredAt = issuedAt + 3600; // One hour later
    var payload = new GoogleIdToken.Payload();
    payload.setAuthorizedParty("103411466401044735393");
    payload.setEmail("some.email@project.iam.gserviceaccount.com");
    payload.setEmailVerified(true);
    payload.setIssuedAtTimeSeconds(issuedAt);
    payload.setExpirationTimeSeconds(expiredAt);
    payload.setIssuer("https://accounts.google.com");
    payload.setSubject("103411466401044735393");
    GenericJson googleMetadata = new GenericJson()
        .set("compute_engine", new GenericJson()
                                   .set("instance_creation_timestamp", 1556025719L)
                                   .set("instance_id", "5850837338805153689")
                                   .set("instance_name", "gew1-metricscatalogbro-b-b7z2")
                                   .set("project_id", "metrics-catalog")
                                   .set("project_number", 283581591831L)
                                   .set("zone", "europe-west1-d")
        );
    payload.set("google", googleMetadata);

    var header = new JsonWebSignature.Header().setAlgorithm("RS256");
    return JsonWebSignature.signUsingRsaSha256(privateKey, Utils.getDefaultJsonFactory(), header, payload);
  }

  /*
   * Force use custom public key to avoid to request keys from Certificate Authorities.
   */
  private void stubPublicKey(GooglePublicKeysManager keysManager, PublicKey publicKey)
      throws NoSuchFieldException, IllegalAccessException {
    // Reflection is used because final methods make it impossible to mock
    setField(keysManager, "publicKeys", List.of(publicKey));
    setField(keysManager, "expirationTimeMilliseconds", Long.MAX_VALUE);
  }

  private void setField(GooglePublicKeysManager keysManager, String name, Object value)
      throws NoSuchFieldException, IllegalAccessException {
    final var field = GooglePublicKeysManager.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(keysManager, value);
  }
}
