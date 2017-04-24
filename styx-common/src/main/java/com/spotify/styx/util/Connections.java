/*-
 * -\-\-
 * Spotify Styx Common
 * --
 * Copyright (C) 2016 Spotify AB
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

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.typesafe.config.Config;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for setting up connections to external services
 */
public final class Connections {

  private static final Logger LOG = LoggerFactory.getLogger(Connections.class);

  public static final String DATASTORE_PROJECT = "styx.datastore.project-id";
  public static final String DATASTORE_NAMESPACE = "styx.datastore.namespace";
  public static final String BIGTABLE_PROJECT_ID = "styx.bigtable.project-id";
  public static final String BIGTABLE_INSTANCE_ID = "styx.bigtable.instance-id";
  public static final String BIGTABLE_FALLBACK_PROJECT_ID = "styx.bigtable.fallback-project-id";
  public static final String BIGTABLE_FALLBACK_INSTANCE_ID = "styx.bigtable.fallback-instance-id";

  private Connections() {
  }

  public static Datastore createDatastore(Config config) {
    final String projectId = config.getString(DATASTORE_PROJECT);
    final String namespace = config.getString(DATASTORE_NAMESPACE);

    LOG.info("Creating Datastore connection for project:{}, namespace:{}",
             projectId, namespace);

    return DatastoreOptions.builder()
        .namespace(namespace)
        .projectId(projectId)
        .build()
        .service();
  }

  public static Connection createBigTableConnection(Config config) {
    final String projectId = config.getString(BIGTABLE_PROJECT_ID);
    final String instanceId = config.getString(BIGTABLE_INSTANCE_ID);
    return createBigTableConnection(projectId, instanceId);
  }

  public static Optional<Connection> createFallbackBigTableConnection(Config config) {
    if (!config.hasPath(BIGTABLE_FALLBACK_PROJECT_ID)) {
      return Optional.empty();
    }

    final String projectId = config.getString(BIGTABLE_FALLBACK_PROJECT_ID);
    final String instanceId = config.getString(BIGTABLE_FALLBACK_INSTANCE_ID);

    return Optional.ofNullable(createBigTableConnection(projectId, instanceId));
  }

  private static Connection createBigTableConnection(String projectId, String instanceId) {
    LOG.info("Creating Bigtable connection for project:{}, instance:{}",
             projectId, instanceId);

    final Configuration bigtableConfiguration = new Configuration();
    bigtableConfiguration.set("google.bigtable.project.id", projectId);
    bigtableConfiguration.set("google.bigtable.instance.id", instanceId);
    bigtableConfiguration.setBoolean("google.bigtable.rpc.use.timeouts", true);

    return BigtableConfiguration.connect(bigtableConfiguration);
  }
}
