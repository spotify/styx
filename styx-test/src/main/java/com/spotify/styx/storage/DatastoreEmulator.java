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

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.google.cloud.datastore.testing.LocalDatastoreHelper;
import com.google.cloud.testing.BaseEmulatorHelper;
import java.util.logging.Level;
import javaslang.control.Try;
import org.junit.rules.ExternalResource;

public class DatastoreEmulator extends ExternalResource {

  static {
    java.util.logging.Logger.getLogger(LocalDatastoreHelper.class.getName())
        .setLevel(Level.OFF);
  }

  // TODO: the datastore emulator behavior wrt conflicts etc differs from the real datastore
  private final LocalDatastoreHelper helper;

  public DatastoreEmulator(double consistency) {
    helper = LocalDatastoreHelper.create(consistency);
  }

  public DatastoreEmulator() {
    this(1.0);
  }

  @Override
  protected void before() {
    Try.run(helper::start).get();
    assertGcloudDatastoreEmulator();
  }

  @Override
  protected void after() {
    Try.run(helper::stop).get();
  }

  public DatastoreOptions options() {
    return helper.getOptions().toBuilder()
        // Enable retries
        .setRetrySettings(DatastoreOptions.getDefaultRetrySettings())
        .build();
  }

  public Datastore client() {
    return options().getService();
  }

  public void reset() {
    Try.run(helper::reset).get();
  }

  private void assertGcloudDatastoreEmulator() {
    var runnerClass = Try.of(() -> {
      var activeRunnerField = BaseEmulatorHelper.class.getDeclaredField("activeRunner");
      activeRunnerField.setAccessible(true);
      var activeRunner = activeRunnerField.get(helper);
      return activeRunner.getClass();
    }).get();
//    if (!runnerClass.getName().equals("com.google.cloud.testing.BaseEmulatorHelper$GcloudEmulatorRunner")) {
//      throw new AssertionError("Not using gcloud sdk datastore emulator, please run: "
//                               + "gcloud components install beta cloud-datastore-emulator");
//    }
  }
}
