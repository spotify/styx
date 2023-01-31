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
import static java.util.stream.Collectors.toList;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.auto.service.AutoService;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.data.EventInfo;
import com.spotify.styx.serialization.Json;
import java.nio.file.Files;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.Test;

@AutoService(EndToEndTestBase.class)
public class BackfillIT extends EndToEndTestBase {
  @Test
  public void testBackfill() throws Exception {

    // Generate workflow configuration
    var workflowJson = Json.OBJECT_MAPPER.writeValueAsString(Map.of(
        "id", workflowId1,
        "schedule", "daily",
        "service_account", workflowServiceAccount.getEmail(),
        "docker_image", "busybox",
        "docker_args", List.of("echo", "{}")));
    doTestBackfill(workflowJson);
  }

  @Test
  public void testBackfillFlyteWorkflow() throws Exception {
    // Generate workflow configuration
    var workflowJson = Json.OBJECT_MAPPER.writeValueAsString(Map.of(
        "id", workflowId1,
        "schedule", "daily",
        "flyte_exec_conf", FLYTE_EXEC_CONF_MAP));
    doTestBackfill(workflowJson);
  }

  private void doTestBackfill(String workflowJson) throws Exception {
    var workflowJsonFile = temporaryFolder.newFile().toPath();
    Files.writeString(workflowJsonFile, workflowJson);

    // Create workflow
    log.info("Creating workflow: {} {}", component1, workflowId1);
    var workflowCreateResult = cliJson(String.class,
        "workflow", "create", "-f", workflowJsonFile.toString(), component1);
    assertThat(workflowCreateResult, is("Workflow " + workflowId1 + " in component " + component1 + " created."));

    var start = LocalDate.parse("2019-05-01");
    var end = LocalDate.parse("2019-05-04");
    var expectedInstances = Stream.iterate(start, i -> i.isBefore(end), i -> i.plusDays(1))
        .collect(toList());

    // Create backfill
    var backfill = cliJson(Backfill.class,
        "backfill", "create", component1, workflowId1, start.toString(), end.toString(), "2");

    // Wait for backfill to successfully complete
    await().atMost(10, MINUTES).until(() -> {
      var backfillPayload = cliJson(BackfillPayload.class, "backfill", "show", backfill.id());
      if (!backfillPayload.backfill().allTriggered()) {
        return false;
      }
      for (final LocalDate instance : expectedInstances) {
        var events = cliJson(new TypeReference<List<EventInfo>>() {}, "e", component1, workflowId1,
            instance.toString());
        var success = events.stream().anyMatch(event -> event.name().equals("success"));
        if (!success) {
          return false;
        }
      }
      return true;
    });
  }
}
