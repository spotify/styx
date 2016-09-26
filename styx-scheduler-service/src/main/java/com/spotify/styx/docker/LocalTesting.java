/*
 * -\-\-
 * Spotify Styx Scheduler Service
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
package com.spotify.styx.docker;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.container.Container;
import com.google.api.services.container.ContainerScopes;
import com.google.api.services.container.model.Cluster;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.CountDownLatch;

import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodBuilder;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;

import static java.lang.System.getenv;

/**
 * Small program for local execution of the local docker runner.
 */
final class LocalTesting {

  private static final Logger LOG = LoggerFactory.getLogger(LocalTesting.class);

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  private static final String STYX_RUN = "styx-run";

  // service account ID (email) and p12 file from:
  // https://console.developers.google.com/permissions/serviceaccounts?project=steel-ridge-91615
  private static final String SERVICE_ACCOUNT_ID = getenv("SERVICE_ACCOUNT_ID");
  private static final String SERVICE_ACCOUNT_PEM_FILE = getenv("SERVICE_ACCOUNT_PEM_FILE");

  private static final String GKE_PROJECT_ID = getenv("GKE_PROJECT_ID");
  private static final String GKE_ZONE = getenv("GKE_ZONE");
  private static final String GKE_CLUSTER_ID = getenv("GKE_CLUSTER_ID");

  private LocalTesting() {
  }

  private static void testKube() throws Exception {
    final NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    final GoogleCredential credential = new GoogleCredential.Builder()
        .setTransport(httpTransport)
        .setJsonFactory(JSON_FACTORY)
        .setServiceAccountId(SERVICE_ACCOUNT_ID)
        .setServiceAccountPrivateKeyFromPemFile(new File(SERVICE_ACCOUNT_PEM_FILE))
        .setServiceAccountScopes(ContainerScopes.all())
        .build();

    final Container gke = new Container.Builder(httpTransport, JSON_FACTORY, credential)
        .setApplicationName(STYX_RUN)
        .build();
    final Cluster cluster = gke.projects().zones().clusters()
        .get(GKE_PROJECT_ID, GKE_ZONE, GKE_CLUSTER_ID).execute();

    final Config config = new ConfigBuilder()
        .withMasterUrl("https://" + cluster.getEndpoint())
        .withCaCertData(cluster.getMasterAuth().getClusterCaCertificate())
        .withClientCertData(cluster.getMasterAuth().getClientCertificate())
        .withClientKeyData(cluster.getMasterAuth().getClientKey())
        .build();
    try (final KubernetesClient client =
             new DefaultKubernetesClient(config).inNamespace("default")) {
      final CountDownLatch closeLatch = new CountDownLatch(1);

      final PodBuilder podBuilder = new PodBuilder()
          .withNewMetadata()
              .withGenerateName(STYX_RUN + "-")
          .endMetadata()

          .withNewSpec()
          .withRestartPolicy("Never")
          .addNewContainer()
              .withName(STYX_RUN)
              .withImage("gcr.io/steel-ridge-91615/luigid-monitoring:a9617a6-1450186176760")
              .withCommand("luigi", "--module", "canary_job", "CanaryJob", "--local-scheduler")
          .endContainer()
          .endSpec();

      final Pod pod = client.pods().create(podBuilder.build());

      try (Watch watch = client.pods().watch(new PodWatcher(pod.getMetadata().getName(), closeLatch))) {
        closeLatch.await();
      }
    }

    System.out.println("k bye");
  }

  public static void main(String... args) throws Exception {
    testKube();
  }

  private static class PodWatcher implements Watcher<Pod> {

    private final String podName;
    private final CountDownLatch latch;

    PodWatcher(final String podName, final CountDownLatch latch) {
      this.podName = podName;
      this.latch = latch;
    }

    @Override
    public void eventReceived(final Action action, final Pod pod) {
      if (action == Action.DELETED) {
        latch.countDown();
        return;
      } else if (pod == null) {
        return;
      } else if (!podName.equals(pod.getMetadata().getName())) {
        return;
      }

      final String phase = pod.getStatus().getPhase();
      if (!phase.equals("Pending") && !phase.equals("Running")) {
        System.out.println("end phase = " + phase);
        latch.countDown();
      }
    }

    @Override
    public void onClose(final KubernetesClientException e) {
      latch.countDown();
    }
  }
}
