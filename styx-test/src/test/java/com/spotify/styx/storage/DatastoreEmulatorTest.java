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

package com.spotify.styx.storage;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.Entity;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;
import org.junit.runner.RunWith;

@RunWith(JUnitParamsRunner.class)
public class DatastoreEmulatorTest {

  /*
  @ClassRule public static final DatastoreEmulator datastoreEmulator = new DatastoreEmulator();

  @Rule public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  private Datastore[] clients() {
    return new Datastore[]{
        datastoreEmulator.client(),
        datastoreEmulator.options().getService()
    };
  }

  @Test
  @Parameters(method = "clients")
  public void testEmulator(Datastore client) {
    var key = client.newKeyFactory().setKind("foo").newKey("bar");
    var entity = Entity.newBuilder(key).set("baz", "quux").build();
    client.put(entity);
    var readEntity = client.get(key);
    assertThat(readEntity, is(entity));
    datastoreEmulator.reset();
    var readEntityAfterReset = client.get(key);
    assertThat(readEntityAfterReset, is(nullValue()));
  }

  @Test
  public void shouldFailIfNotGcloudEmulator() throws InterruptedException {
    environmentVariables.clear("PATH");
    var emulator = new DatastoreEmulator();

    var exception = Assert.assertThrows(AssertionError.class, emulator::before);

    assertThat(exception.getMessage(), Matchers.startsWith("Not using gcloud sdk datastore emulator"));
  }
}*/
