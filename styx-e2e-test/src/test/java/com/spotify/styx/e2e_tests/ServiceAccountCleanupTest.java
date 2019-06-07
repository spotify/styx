/*-
 * -\-\-
 * Spotify End-to-End Integration Tests
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

package com.spotify.styx.e2e_tests;

import static com.spotify.styx.util.GoogleApiClientUtil.executeWithRetries;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.IamScopes;
import com.google.api.services.iam.v1.model.ServiceAccount;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This removes expired service accounts that might be left behind by e2e tests
 * due to interrupted execution or failing teardown etc. It is not really a test.
 */
public class ServiceAccountCleanupTest {

  private static final Logger log = LoggerFactory.getLogger(ServiceAccountCleanupTest.class);

  private static final Instant NOW = Instant.now();

  @Test
  public void deleteExpiredTestServiceAccounts() throws IOException {
    var iam = new Iam.Builder(
        Utils.getDefaultTransport(), Utils.getDefaultJsonFactory(),
        GoogleCredential.getApplicationDefault().createScoped(IamScopes.all()))
        .setApplicationName(TestNamespaces.TEST_NAMESPACE_PREFIX)
        .build();

    var accounts = listServiceAccounts(iam);

    for (final ServiceAccount account : accounts) {
      var displayName = account.getDisplayName();
      if (displayName == null || !TestNamespaces.isExpiredTestNamespace(displayName, NOW)) {
        continue;
      }
      log.info("Deleting old test service account: {}", account.getEmail());
      try {
        var request = iam.projects().serviceAccounts()
            .delete("projects/styx-oss-test/serviceAccounts/" + account.getEmail());
        executeWithRetries(request);
      } catch (Throwable e) {
        log.error("Failed to delete old test service account: {}", account.getEmail(), e);
      }
    }
  }

  private List<ServiceAccount> listServiceAccounts(Iam iam) throws IOException {
    var accounts = new ArrayList<ServiceAccount>();
    String pageToken = null;
    do {
      var request = iam.projects().serviceAccounts().list("projects/styx-oss-test")
          .setPageToken(pageToken);
      var listResponse = executeWithRetries(request);
      accounts.addAll(listResponse.getAccounts());
      pageToken = listResponse.getNextPageToken();
    } while (pageToken != null);
    return accounts;
  }
}
