package com.spotify.styx.util;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.cloud.datastore.Datastore;
import com.google.cloud.datastore.DatastoreOptions;
import com.typesafe.config.Config;
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

    LOG.info("Creating Bigtable connection for project:{}, instance:{}",
             projectId, instanceId);

    final Configuration bigtableConfiguration = new Configuration();
    bigtableConfiguration.set("google.bigtable.project.id", projectId);
    bigtableConfiguration.set("google.bigtable.instance.id", instanceId);
    bigtableConfiguration.setBoolean("google.bigtable.rpc.use.timeouts", true);

    return BigtableConfiguration.connect(bigtableConfiguration);
  }

}
