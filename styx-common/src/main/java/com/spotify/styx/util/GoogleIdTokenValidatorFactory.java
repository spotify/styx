/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2018 Spotify AB
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

package com.spotify.styx.util;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.auth.oauth2.GoogleIdTokenVerifier;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.services.cloudresourcemanager.CloudResourceManager;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.IamScopes;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Set;
import java.util.function.BiFunction;

public interface GoogleIdTokenValidatorFactory
    extends BiFunction<Set<String>, String, GoogleIdTokenValidator> {

  GoogleIdTokenValidatorFactory DEFAULT = (domainWhitelist, service) -> {
    final HttpTransport httpTransport;
    try {
      httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    } catch (GeneralSecurityException | IOException e) {
      throw new RuntimeException(e);
    }

    final JsonFactory jsonFactory = Utils.getDefaultJsonFactory();

    final GoogleIdTokenVerifier
        idTokenVerifier = new GoogleIdTokenVerifier(httpTransport, jsonFactory);

    final GoogleCredential credential;
    try {
      credential = GoogleCredential.getApplicationDefault().createScoped(IamScopes.all());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final CloudResourceManager cloudResourceManager =
        new CloudResourceManager.Builder(httpTransport, jsonFactory, credential)
            .setApplicationName(service)
            .build();

    final Iam iam = new Iam.Builder(
        httpTransport, jsonFactory, credential)
        .setApplicationName(service)
        .build();

    final GoogleIdTokenValidator validator =
        new GoogleIdTokenValidator(idTokenVerifier, cloudResourceManager, iam, domainWhitelist);
    try {
      validator.cacheProjects();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return validator;
  };
}
