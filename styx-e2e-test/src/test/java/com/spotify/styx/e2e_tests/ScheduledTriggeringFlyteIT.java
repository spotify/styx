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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.auto.service.AutoService;
import com.spotify.styx.model.data.EventInfo;
import com.spotify.styx.serialization.Json;
import java.nio.file.Files;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import org.junit.Test;

@AutoService(EndToEndTestBase.class)
public class ScheduledTriggeringFlyteIT extends EndToEndTestBase {
  private static final String FLYTE_PROJECT = "flytesnacks";

  public static final String FLYTE_DOMAIN = "development";
  public static final String FLYTE_LAUNCH_PLAN_NAME = "workflows.hello_world_workflow.lp";
  public static final String FLYTE_LAUNCH_PLAN_VERSION = "b495f8671b8fc2da9e51acd5803a26d52e18795a";
  public static final String RESOURCE_TYPE = "LAUNCH_PLAN";
  private static Map<String, String> REFERENCE_ID =
    Map.of(
      "project", FLYTE_PROJECT,
      "domain", FLYTE_DOMAIN,
      "name", FLYTE_LAUNCH_PLAN_NAME,
      "version", FLYTE_LAUNCH_PLAN_VERSION,
      "resource_type", RESOURCE_TYPE
    );

  private static Map<String, Object> FLYTE_EXEC_CONF_MAP =
      Map.of(
      "reference_id", REFERENCE_ID,
      "input_fields", Map.of()
    );

  @Test
  public void testScheduledTriggering() throws Exception {
    // Generate workflow configuration
    var workflowJson = Json.OBJECT_MAPPER.writeValueAsString(Map.of(
        "id", workflowId1,
        "schedule", "* * * * *",
        "flyte_exec_conf", FLYTE_EXEC_CONF_MAP));
    var workflowJsonFile = temporaryFolder.newFile().toPath();

    Files.writeString(workflowJsonFile, workflowJson);

    // Create workflow
    log.info("Creating workflow: {} {}", component1, workflowId1);
    var workflowCreateResult = cliJson(String.class,
        "workflow", "create", "-f", workflowJsonFile.toString(), component1);
    assertThat(workflowCreateResult, is("Workflow " + workflowId1 + " in component " + component1 + " created."));

    // Enable workflow scheduled execution
    log.info("Enabling workflow: {} {}", component1, workflowId1);
    var enableResult = cliJson(String.class, "workflow", "enable", component1, workflowId1);
    assertThat(enableResult, is("Workflow " + workflowId1 + " in component " + component1 + " enabled."));

    // Get expected scheduled instance
    var workflowWithState = cliJson(WorkflowWithState.class, "workflow", "show", component1, workflowId1);
    var nextNaturalTrigger = workflowWithState.state().nextNaturalTrigger().orElseThrow();
    var instance = nextNaturalTrigger.truncatedTo(ChronoUnit.SECONDS).toString();
    log.info("Expected instance: {}", instance);

    // Wait for expected instance to successfully complete
    await().atMost(5, MINUTES).until(() -> {
      var events = cliJson(new TypeReference<List<EventInfo>>() {}, "e", component1, workflowId1, instance);
      return events.stream().anyMatch(event -> event.name().equals("success"));
    });
  }
}
