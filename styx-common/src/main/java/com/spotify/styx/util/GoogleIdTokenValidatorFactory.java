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
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Set;
import java.util.function.BiFunction;

public interface GoogleIdTokenValidatorFactory
    extends BiFunction<Set<String>, String, GoogleIdTokenValidator> {

  class DefaultGoogleIdTokenValidatorFactory implements GoogleIdTokenValidatorFactory {

    @VisibleForTesting
    GoogleIdTokenVerifier buildGoogleIdTokenVerifier(HttpTransport httpTransport,
                                                     JsonFactory jsonFactory) {
      return new GoogleIdTokenVerifier(httpTransport, jsonFactory);
    }

    @VisibleForTesting
    GoogleCredential loadCredential() {
      try {
        return GoogleCredential.getApplicationDefault().createScoped(IamScopes.all());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @VisibleForTesting
    Iam buildIam(HttpTransport httpTransport, JsonFactory jsonFactory, GoogleCredential credential,
                 String service) {
      return new Iam.Builder(
          httpTransport, jsonFactory, credential)
          .setApplicationName(service)
          .build();
    }

    @VisibleForTesting
    CloudResourceManager buildCloudResourceManager(HttpTransport httpTransport,
                                                   JsonFactory jsonFactory,
                                                   GoogleCredential credential,
                                                   String service) {
      return new CloudResourceManager.Builder(httpTransport, jsonFactory, credential)
          .setApplicationName(service)
          .build();
    }

    @Override
    public GoogleIdTokenValidator apply(Set<String> domainWhitelist, String service) {
      final HttpTransport httpTransport;
      try {
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();
      } catch (GeneralSecurityException | IOException e) {
        throw new RuntimeException(e);
      }

      final JsonFactory jsonFactory = Utils.getDefaultJsonFactory();

      final GoogleIdTokenVerifier googleIdTokenVerifier =
          buildGoogleIdTokenVerifier(httpTransport, jsonFactory);

      final GoogleCredential credential = loadCredential();

      final CloudResourceManager cloudResourceManager =
          buildCloudResourceManager(httpTransport, jsonFactory, credential, service);

      final Iam iam = buildIam(httpTransport, jsonFactory, credential, service);

      final GoogleIdTokenValidator validator =
          new GoogleIdTokenValidator(googleIdTokenVerifier, cloudResourceManager, iam,
              domainWhitelist);
      try {
        validator.cacheProjects();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return validator;
    }
  }

  GoogleIdTokenValidatorFactory DEFAULT = new DefaultGoogleIdTokenValidatorFactory();
}
