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
import static com.spotify.styx.testdata.TestData.TEST_DEPLOYMENT_TIME;
import static java.lang.ProcessBuilder.Redirect.INHERIT;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.util.Utils;
import com.google.api.services.iam.v1.Iam;
import com.google.api.services.iam.v1.IamScopes;
import com.google.api.services.iam.v1.model.Binding;
import com.google.api.services.iam.v1.model.CreateServiceAccountRequest;
import com.google.api.services.iam.v1.model.ServiceAccount;
import com.google.api.services.iam.v1.model.SetIamPolicyRequest;
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
import com.spotify.styx.util.Time;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.fabric8.kubernetes.client.NamespacedKubernetesClient;
import io.norberg.automatter.AutoMatter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import javaslang.control.Try;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndToEndTestBase {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  protected static final Logger log = LoggerFactory.getLogger(EndToEndTestBase.class);

  static final String SCHEDULER_SERVICE_NAME = "styx-e2e-test-scheduler";
  private static final String API_SERVICE_NAME = "styx-e2e-test-api";

  private final ExecutorService executor = Executors.newCachedThreadPool();

  static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter
      .ofPattern("yyyyMMdd-HHmmss", Locale.ROOT)
      .withZone(ZoneOffset.UTC);

  private final String testNamespace = TestNamespaces.createTestNamespace(Instant.now());

  // Service account IDs cannot be longer than 30 chars
  private final String workflowServiceAccountId = String.join("-", "e2e",
      TestNamespaces.testNamespaceTimeTimestamp(testNamespace),
      TestNamespaces.testNamespaceRandom(testNamespace));

  protected final String component1 = testNamespace + "-c-1";
  protected final String workflowId1 = testNamespace + "-wf-1";

  private static final String FLYTE_PROJECT = "flytesnacks";

  private static final String FLYTE_DOMAIN = "development";
  private static final String FLYTE_LAUNCH_PLAN_NAME = "morning_greeting";
  private static final String FLYTE_LAUNCH_PLAN_VERSION = "04e346ca5f43fc2778259d77a2d9f64ce42b2a27";
  private static final String RESOURCE_TYPE = "LAUNCH_PLAN";
  private static final Map<String, String> REFERENCE_ID =
      Map.of(
          "project", FLYTE_PROJECT,
          "domain", FLYTE_DOMAIN,
          "name", FLYTE_LAUNCH_PLAN_NAME,
          "version", FLYTE_LAUNCH_PLAN_VERSION,
          "resource_type", RESOURCE_TYPE
      );

  protected static final Map<String, Object> FLYTE_EXEC_CONF_MAP = Map.of(
      "reference_id", REFERENCE_ID,
      "input_fields", Map.of()
  );

  private final CompletableFuture<Service.Instance> styxSchedulerInstance = new CompletableFuture<>();
  private final CompletableFuture<Service.Instance> styxApiInstance = new CompletableFuture<>();

  private Future<Object> styxSchedulerThread;
  private Future<Object> styxApiThread;

  private Iam iam;
  private Datastore datastore;
  private NamespacedKubernetesClient k8s;

  private Config schedulerConfig;
  private Config apiConfig;
  private final Time time = ()-> TEST_DEPLOYMENT_TIME;;

  private static final int BASE_PORT = 18080;

  private final List<? extends Class<? extends EndToEndTestBase>> testClasses =
      ServiceLoader.load(EndToEndTestBase.class)
          .stream()
          .map(ServiceLoader.Provider::type)
          .collect(toList());

  private final int testIndex = testClasses.indexOf(getClass());
  private final int styxApiPort = BASE_PORT + testIndex * 2;
  private final int styxSchedulerPort = BASE_PORT + testIndex * 2 + 1;

  ServiceAccount workflowServiceAccount;

  @Before
  public void setUp() throws Exception {
    log.info("Setting up styx e2e test: {}", testNamespace);

    Awaitility.setDefaultPollInterval(Duration.ofSeconds(5));

    setUpServiceAccounts();
    setUpConfig();
    setUpDatastore();
    setUpKubernetes();
    startStyx();
  }

  @After
  public void tearDown() {
    log.info("Tearing down styx e2e test: {}", testNamespace);

    Try.run(this::stopStyx).onFailure(e -> log.error("Styx teardown failed", e));
    Try.run(this::tearDownDatastore).onFailure(e -> log.error("Datastore teardown failed", e));
    Try.run(this::tearDownKubernetes).onFailure(e -> log.error("Kubernetes teardown failed", e));
    Try.run(this::tearDownServiceAccounts).onFailure(e -> log.error("Service account teardown failed", e));
  }

  private void startStyx() {
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
          .setTime(time)
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

  private void setUpKubernetes() {
    System.setProperty("kubernetes.auth.tryKubeConfig", "false");
    log.info("Creating k8s namespace: {}", testNamespace);
    k8s = StyxScheduler.getKubernetesClient(schedulerConfig, "default");
    k8s.namespaces().createNew()
        .withNewMetadata().withName(testNamespace).endMetadata()
        .done();
  }

  private void setUpDatastore() {
    // Create datastore client
    datastore = Connections.createDatastore(schedulerConfig, Stats.NOOP);
  }

  private void setUpConfig() {
    System.setProperty("styx.test.namespace", testNamespace);
    System.setProperty("styx.api.port", String.valueOf(styxApiPort));
    System.setProperty("styx.scheduler.port", String.valueOf(styxSchedulerPort));
    ConfigFactory.invalidateCaches();
    schedulerConfig = ConfigFactory.load(SCHEDULER_SERVICE_NAME);
    apiConfig = ConfigFactory.load(API_SERVICE_NAME);
  }

  private void setUpServiceAccounts() throws IOException {
    // Create workflow service account
    iam = new Iam.Builder(
        Utils.getDefaultTransport(), Utils.getDefaultJsonFactory(),
        GoogleCredential.getApplicationDefault().createScoped(IamScopes.all()))
        .setApplicationName(testNamespace)
        .build();
    workflowServiceAccount = iam.projects().serviceAccounts()
        .create("projects/styx-oss-test",
            new CreateServiceAccountRequest().setAccountId(workflowServiceAccountId)
                .setServiceAccount(new ServiceAccount().setDisplayName(testNamespace)))
        .execute();
    log.info("Created workflow test service account: {}", workflowServiceAccount.getEmail());

    // Set up workflow service account permissions
    var workflowServiceAccountFqn = "projects/styx-oss-test/serviceAccounts/" + workflowServiceAccount.getEmail();
    var workflowServiceAccountPolicy = iam.projects().serviceAccounts()
        .getIamPolicy(workflowServiceAccountFqn)
        .execute();
    if (workflowServiceAccountPolicy.getBindings() == null) {
      workflowServiceAccountPolicy.setBindings(new ArrayList<>());
    }
    workflowServiceAccountPolicy.getBindings()
        .add(new Binding().setRole("projects/styx-oss-test/roles/StyxWorkflowServiceAccountUser")
            .setMembers(List.of("serviceAccount:styx-circle-ci@styx-oss-test.iam.gserviceaccount.com")));
    // TODO: set up a styx service account instead of using styx-circle-ci@
    workflowServiceAccountPolicy.getBindings()
        .add(new Binding().setRole("roles/iam.serviceAccountKeyAdmin")
            .setMembers(List.of("serviceAccount:styx-circle-ci@styx-oss-test.iam.gserviceaccount.com")));
    iam.projects().serviceAccounts().setIamPolicy(workflowServiceAccountFqn,
        new SetIamPolicyRequest().setPolicy(workflowServiceAccountPolicy))
        .execute();
  }

  private void tearDownServiceAccounts() throws IOException {
    if (iam != null && workflowServiceAccount != null) {
      log.info("Deleting workflow service account: {}", workflowServiceAccount.getEmail());
      iam.projects().serviceAccounts()
          .delete("projects/styx-oss-test/serviceAccounts/" + workflowServiceAccount.getEmail())
          .execute();
    }
  }

  private void tearDownKubernetes() {
    if (k8s != null) {
      log.info("Deleting k8s namespace: {}", testNamespace);
      k8s.inNamespace(testNamespace).pods().withGracePeriod(0).delete();
      k8s.namespaces().withName(testNamespace).delete();
    }
  }

  private void tearDownDatastore() {
    if (datastore != null) {
      deleteDatastoreNamespace(datastore, testNamespace);
    }
  }

  private void stopStyx() throws InterruptedException {
    styxApiInstance.thenAccept(instance -> instance.getSignaller().signalShutdown()).getNow(null);
    styxSchedulerInstance.thenAccept(instance -> instance.getSignaller().signalShutdown()).getNow(null);
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
  }

  <T> T cliJson(Class<T> outputClass, String... args) throws IOException, InterruptedException, CliException {
    return cliJson(Json.OBJECT_MAPPER.getTypeFactory().constructType(outputClass), List.of(args));
  }

  <T> T cliJson(TypeReference<T> outputType, String... args)
      throws IOException, InterruptedException, CliException {
    return cliJson(Json.OBJECT_MAPPER.getTypeFactory().constructType(outputType), List.of(args));
  }

  private <T> T cliJson(JavaType outputType, List<String> args)
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

  private byte[] cli(List<String> args) throws IOException, InterruptedException, CliException {

    var stdout = new ByteArrayOutputStream();

    var spawner = Subprocesses.process().main(CliMain.class)
        .jvmArgs(
            "-Xmx192m", "-Xms192m",
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

  static class CliException extends Exception {

    final int code;

    private CliException(String message, int code) {
      super(message + ": code=" + code);
      this.code = code;
    }
  }
}
