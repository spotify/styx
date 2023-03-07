/*
 * -\-\-
 * Spotify Styx Scheduler Service
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.when;

import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.storage.InstrumentedDatastore;
import com.typesafe.config.Config;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ConnectionsTest {

  private static final String TEST_PROJECT = "test-project";
  private static final String TEST_NAMESPACE = "test-namespace";

  @Mock Config config;
  @Mock Stats stats;

  @Test
  public void createDatastore() {
    when(config.getString(Connections.DATASTORE_PROJECT)).thenReturn(TEST_PROJECT);
    when(config.getString(Connections.DATASTORE_NAMESPACE)).thenReturn(TEST_NAMESPACE);
    final InstrumentedDatastore instrumentedDatastore = Connections.createDatastore(config, stats);
    final Datastore datastore = instrumentedDatastore.delegate();
    final DatastoreOptions options = datastore.getOptions();
    assertThat(options.getProjectId(), is(TEST_PROJECT));
    assertThat(options.getNamespace(), is(TEST_NAMESPACE));
  }
}
