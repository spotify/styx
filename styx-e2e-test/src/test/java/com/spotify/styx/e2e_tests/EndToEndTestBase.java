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

import static com.spotify.styx.e2e_tests.DatastoreUtil.deleteDatastoreNamespace;
import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.google.cloud.datastore.Datastore;
import com.spotify.apollo.core.Service;
import com.spotify.apollo.httpservice.HttpService;
import com.spotify.spawn.Subprocesses;
import com.spotify.styx.StyxApi;
import com.spotify.styx.StyxScheduler;
import com.spotify.styx.cli.CliMain;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.monitoring.Stats;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.util.Connections;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.norberg.automatter.AutoMatter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javaslang.control.Try;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndToEndTestBase {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  static final Logger log = LoggerFactory.getLogger(EndToEndTestBase.class);

  static final String SCHEDULER_SERVICE_NAME = "styx-e2e-test-scheduler";
  static final String API_SERVICE_NAME = "styx-e2e-test-api";

  final ExecutorService executor = Executors.newCachedThreadPool();

  static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter
      .ofPattern("yyyyMMdd-HHmmss", Locale.ROOT)
      .withZone(ZoneOffset.UTC);

  final String namespace = TestNamespaces.createTestNamespace(Instant.now());

  final String component1 = namespace + "-c-1";
  final String workflowId1 = namespace + "-wf-1";

  private CompletableFuture<Service.Instance> styxSchedulerInstance = new CompletableFuture<>();
  private CompletableFuture<Service.Instance> styxApiInstance = new CompletableFuture<>();

  private Future<Object> styxSchedulerThread;
  private Future<Object> styxApiThread;

  private Datastore datastore;
  private NamespacedKubernetesClient k8s;

  private Config schedulerConfig;

  private static final int BASE_PORT = 18080;

  private final List<? extends Class<? extends EndToEndTestBase>> testClasses =
      ServiceLoader.load(EndToEndTestBase.class)
          .stream()
          .map(ServiceLoader.Provider::type)
          .collect(toList());

  private final int testIndex = testClasses.indexOf(getClass());
  private final int styxApiPort = BASE_PORT + testIndex * 2;
  private final int styxSchedulerPort = BASE_PORT + testIndex * 2 + 1;

  @Before
  public void setUp() throws Exception {
    // Setup namespace
    log.info("Setting up styx e2e test: {}", namespace);
    System.setProperty("styx.test.namespace", namespace);

    System.setProperty("styx.api.port", String.valueOf(styxApiPort));
    System.setProperty("styx.scheduler.port", String.valueOf(styxSchedulerPort));

    schedulerConfig = ConfigFactory.load(SCHEDULER_SERVICE_NAME);
    datastore = Connections.createDatastore(schedulerConfig, Stats.NOOP);

    System.setProperty("kubernetes.auth.tryKubeConfig", "false");
    log.info("Creating k8s namespace: {}", namespace);
    k8s = StyxScheduler.getKubernetesClient(schedulerConfig, "default");
    k8s.namespaces().createNew()
        .withNewMetadata().withName(namespace).endMetadata()
        .done();

    // Start scheduler
    styxSchedulerThread = executor.submit(() -> {
      HttpService.boot(env -> StyxScheduler.newBuilder()
          .setServiceName(SCHEDULER_SERVICE_NAME)
          .build()
          .create(env), SCHEDULER_SERVICE_NAME, styxSchedulerInstance::complete);
      return null;
    });

    // Start api
    styxApiThread = executor.submit(() -> {
      HttpService.boot(env -> StyxApi.newBuilder()
          .setServiceName(API_SERVICE_NAME)
          .build()
          .create(env), API_SERVICE_NAME, styxApiInstance::complete);
      return null;
    });

    await().atMost(300, SECONDS)
        .until(() -> {
          if (styxSchedulerThread.isDone()) {
            styxSchedulerThread.get();
          }
          if (styxApiThread.isDone()) {
            styxApiThread.get();
          }
          return styxSchedulerInstance.isDone() && styxApiInstance.isDone();
        });
  }

  @After
  public void tearDown() throws Exception {
    log.info("Tearing down styx e2e test: {}", namespace);

    try {
      styxApiInstance.thenAccept(instance -> instance.getSignaller().signalShutdown());
      styxSchedulerInstance.thenAccept(instance -> instance.getSignaller().signalShutdown());
      if (styxApiThread != null) {
        Try.run(() -> styxApiThread.get(30, SECONDS));
        styxApiThread.cancel(true);
      }
      if (styxSchedulerThread != null) {
        Try.run(() -> styxSchedulerThread.get(30, SECONDS));
        styxSchedulerThread.cancel(true);
      }
      executor.shutdownNow();
      executor.awaitTermination(30, SECONDS);
    } catch (Throwable t) {
      log.error("styx teardown failed", t);
    }

    try {
      if (datastore != null) {
        Try.run(() -> deleteDatastoreNamespace(datastore, namespace));
      }
    } catch (Throwable t) {
      log.error("datastore teardown failed", t);
    }

    try {
      if (k8s != null) {
        log.info("Deleting k8s namespace: {}", namespace);
        k8s.inNamespace(namespace).pods().withGracePeriod(0).delete();
        k8s.namespaces().withName(namespace).delete();
      }
    } catch (Throwable t) {
      log.error("k8s teardown failed", t);
    }
  }

  <T> T cliJson(Class<T> outputClass, String... args) throws IOException, InterruptedException, CliException {
    return cliJson(Json.OBJECT_MAPPER.getTypeFactory().constructType(outputClass), List.of(args));
  }

  <T> T cliJson(TypeReference<T> outputType, String... args)
      throws IOException, InterruptedException, CliException {
    return cliJson(Json.OBJECT_MAPPER.getTypeFactory().constructType(outputType), List.of(args));
  }

  <T> T cliJson(JavaType outputType, List<String> args)
      throws IOException, InterruptedException, CliException {
    var jsonArgs = new ArrayList<String>();
    jsonArgs.add("--json");
    jsonArgs.addAll(args);
    var output = cli(jsonArgs);
    try {
      return Json.OBJECT_MAPPER.readValue(output, outputType);
    } catch (IOException e) {
      log.error("Failed to json deserialize cli output: {}", new String(output, StandardCharsets.UTF_8), e);
      throw e;
    }
  }

  byte[] cli(List<String> args) throws IOException, InterruptedException, CliException {

    var stdout = new ByteArrayOutputStream();

    var spawner = Subprocesses.process().main(CliMain.class)
        .jvmArgs(
            "-Xmx128m", "-Xms128m",
            "-Xverify:none", "-XX:+TieredCompilation", "-XX:TieredStopAtLevel=1")
        .args(args)
        .redirectStderr(INHERIT)
        .pipeStdout(stdout);

    spawner.processBuilder().environment().put("STYX_CLI_HOST", "http://127.0.0.1:" + styxApiPort);

    var process = spawner.spawn();

    var exited = process.process().waitFor(120, SECONDS);
    if (!exited) {
      process.kill();
      throw new AssertionError("cli timeout");
    }

    var exitCode = process.process().exitValue();
    if (exitCode != 0) {
      throw new CliException("cli failed", exitCode);
    }

    return stdout.toByteArray();
  }

  @AutoMatter
  interface WorkflowWithState {

    Workflow workflow();

    WorkflowState state();
  }

  class CliException extends Exception {

    final int code;

    private CliException(String message, int code) {
      super(message + ": code=" + code);
      this.code = code;
    }
  }
}
