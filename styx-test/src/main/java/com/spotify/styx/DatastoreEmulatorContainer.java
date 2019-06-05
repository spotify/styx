/*-
 * -\-\-
 * Spotify Styx Testing Utilities
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

package com.spotify.styx;

import com.google.cloud.NoCredentials;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class DatastoreEmulatorContainer extends GoogleCloudContainer {

  public static final int EMULATOR_PORT = 8888;

  public DatastoreEmulatorContainer() {
    super();
    addEnv("JAVA", "java -Xmx256m");
    addExposedPort(EMULATOR_PORT);
    setCommand("/bin/sh", "-c", "gcloud beta emulators datastore start --no-legacy"
                                + " --project testing"
                                + " --host-port=0.0.0.0:8888"
                                + " --no-store-on-disk"
                                + " --consistency=1");
  }

  public void reset() {
    try {
      HttpClient.newHttpClient().send(HttpRequest.newBuilder()
          .POST(HttpRequest.BodyPublishers.noBody())
          .uri(URI.create("http://" + getContainerIpAddress() + ":" + getMappedPort(EMULATOR_PORT) + "/reset"))
          .build(), HttpResponse.BodyHandlers.discarding());
    } catch (IOException | InterruptedException e) {
      throw new RuntimeException("Failed to reset datastore", e);
    }
  }

  public DatastoreOptions datastoreOptions() {
    return DatastoreOptions.newBuilder().setProjectId("testing")
        .setHost(getContainerIpAddress() + ":" + getMappedPort(EMULATOR_PORT))
        .setCredentials(NoCredentials.getInstance())
        .build();
  }

  public Datastore datastoreClient() {
    return datastoreOptions().getService();
  }
}
