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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.auto.service.AutoService;
import com.spotify.styx.model.data.EventInfo;
import com.spotify.styx.serialization.Json;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import org.junit.Test;

@AutoService(EndToEndTestBase.class)
public class AdHocTriggeringIT extends EndToEndTestBase {

  @Test
  public void testAdhocTriggeringFlyteWorkflow() throws Exception {
    // Generate workflow configuration
    var workflowJson = Json.OBJECT_MAPPER.writeValueAsString(Map.of(
        "id", workflowId1,
        "schedule", "daily",
        "flyte_exec_conf", FLYTE_EXEC_CONF_MAP));

    doTestAdhocTriggering(workflowJson);
  }

  @Test
  public void testAdHocTriggering() throws Exception {

    // Generate workflow configuration
    var workflowJson = Json.OBJECT_MAPPER.writeValueAsString(Map.of(
        "id", workflowId1,
        "schedule", "daily",
        "service_account", workflowServiceAccount.getEmail(),
        "docker_image", "busybox",
        "docker_args", List.of("echo", "hello world")));

    doTestAdhocTriggering(workflowJson);
  }

  private void doTestAdhocTriggering(String workflowJson) throws Exception {
    var workflowJsonFile = temporaryFolder.newFile().toPath();
    Files.writeString(workflowJsonFile, workflowJson);

    // Create workflow
    log.info("Creating workflow: {}", workflowId1);
    var workflowCreateResult = cliJson(String.class,
        "workflow", "create", "-f", workflowJsonFile.toString(), component1);
    assertThat(workflowCreateResult, is("Workflow " + workflowId1 + " in component " + component1 + " created."));

    // Trigger workflow instance
    log.info("Triggering workflow");
    var instance = "2019-05-13";
    var triggerResult = cliJson(String.class, "t", component1, workflowId1, instance);
    assertThat(triggerResult, is("Triggered! Use `styx ls -c " + component1 + "` to check active workflow instances."));

    // Wait for instance to successfully complete
    await().atMost(10, MINUTES).until(() -> {
      var events = cliJson(new TypeReference<List<EventInfo>>() {}, "e", component1, workflowId1, instance);
      return events.stream().anyMatch(event -> event.name().equals("success"));
    });
  }
}
