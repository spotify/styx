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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.auto.service.AutoService;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.serialization.Json;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

@AutoService(EndToEndTestBase.class)
public class CreateDeleteWorkflowIT extends EndToEndTestBase {

  @Test
  public void testCreateDeleteWorkflow() throws Exception {

    var workflowConfiguration = WorkflowConfiguration.builder()
        .id(workflowId1)
        .schedule(Schedule.DAYS)
        .dockerImage("busybox")
        .dockerArgs(List.of("echo", "hello world"))
        .serviceAccount(workflowServiceAccount.getEmail())
        .build();
    var workflow = Workflow.create(component1, workflowConfiguration);
    var workflowJson = Json.OBJECT_MAPPER.writeValueAsString(workflowConfiguration);
    var workflowJsonFile = temporaryFolder.newFile().toPath();
    Files.writeString(workflowJsonFile, workflowJson);

    // Create workflow
    log.info("Creating workflow: {}", workflowId1);
    var workflowCreateResult = cliJson(String.class,
        "workflow", "create", "-f", workflowJsonFile.toString(), component1);
    assertThat(workflowCreateResult, is("Workflow " + workflowId1 + " in component " + component1 + " created."));

    // Check workflow
    var workflowWithState = cliJson(WorkflowWithState.class, "workflow", "show", component1, workflowId1);
    assertThat(workflowWithState.workflow(), is(workflow));
    assertThat(workflowWithState.state().enabled(), is(Optional.of(false)));
    assertThat(workflowWithState.state().nextNaturalTrigger(), is(not(Optional.empty())));
    assertThat(workflowWithState.state().nextNaturalOffsetTrigger(), is(not(Optional.empty())));

    // Delete workflow
    var deleteResult = cliJson(String.class, "workflow", "delete", "--force", component1, workflowId1);
    assertThat(deleteResult, is("Workflow " + workflowId1 + " in component " + component1 + " deleted."));

    // Check that the workflow is gone
    try {
      cliJson(String.class, "workflow", "show", component1, workflowId1);
      fail();
    } catch (CliException e) {
      // workflow 404 not found -> api error -> exit code 4
      assertThat(e.code, is(4));
    }
  }
}
