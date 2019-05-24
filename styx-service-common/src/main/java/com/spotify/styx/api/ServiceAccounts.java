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

import com.google.auth.ServiceAccountSigner;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;

class ServiceAccounts {

  private ServiceAccounts() {
    throw new UnsupportedOperationException();
  }

  static String serviceAccountEmail(GoogleCredentials credentials) {
    if (credentials instanceof ImpersonatedCredentials) {
      return ((ImpersonatedCredentials) credentials).toBuilder().getTargetPrincipal();
    } else if (credentials instanceof ServiceAccountSigner) {
      return ((ServiceAccountSigner) credentials).getAccount();
    } else {
      throw new IllegalArgumentException("Credential is not a service account");
    }
  }
}
