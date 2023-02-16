/*
 * -\-\-
 * Spotify Styx Scheduler Service
 * --
 * Copyright (C) 2017 Spotify AB
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

package com.spotify.styx.cli;

import static com.spotify.futures.CompletableFutures.exceptionallyCompletedFuture;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import com.google.common.base.Throwables;
import com.google.common.io.Resources;
import com.spotify.styx.api.BackfillPayload;
import com.spotify.styx.api.BackfillsPayload;
import com.spotify.styx.api.RunStateDataPayload;
import com.spotify.styx.api.TestServiceAccountUsageAuthorizationResponse;
import com.spotify.styx.cli.CliExitException.ExitStatus;
import com.spotify.styx.cli.CliMain.CliContext;
import com.spotify.styx.cli.CliMain.CliContext.Output;
import com.spotify.styx.client.ApiErrorException;
import com.spotify.styx.client.ClientErrorException;
import com.spotify.styx.client.StyxClient;
import com.spotify.styx.model.Backfill;
import com.spotify.styx.model.BackfillInput;
import com.spotify.styx.model.Schedule;
import com.spotify.styx.model.TriggerParameters;
import com.spotify.styx.model.Workflow;
import com.spotify.styx.model.WorkflowConfiguration;
import com.spotify.styx.model.WorkflowId;
import com.spotify.styx.model.WorkflowState;
import com.spotify.styx.model.WorkflowWithState;
import com.spotify.styx.serialization.Json;
import com.spotify.styx.util.WorkflowValidator;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javaslang.control.Try;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(JUnitParamsRunner.class)
public class CliMainTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Mock CliContext cliContext;
  @Mock StyxClient client;
  @Mock CliOutput cliOutput;
  @Mock WorkflowValidator validator;

  private String requestId = UUID.randomUUID().toString();
  private static final Instant currentTime = Instant.parse("2019-01-01T00:00:00Z");

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    when(cliContext.workflowValidator()).thenReturn(validator);
    when(validator.validateWorkflow(any())).thenReturn(Collections.emptyList());
    when(cliContext.createClient(any())).thenReturn(client);
    when(cliContext.output(any())).thenReturn(cliOutput);
    when(cliContext.env()).thenReturn(
        Map.of("STYX_CLI_HOST", "https://styx.foo.bar:4711"));
  }

  @Test
  public void testList() {
    final RunStateDataPayload payload = mock(RunStateDataPayload.class);
    when(client.activeStates(Optional.empty()))
        .thenReturn(CompletableFuture.completedFuture(payload));
    CliMain.run(cliContext, "ls");
    verify(client).activeStates(Optional.empty());
    verify(cliOutput).printStates(payload);
  }

  @Test
  public void testWorkflowList() {
    @SuppressWarnings("unchecked")
    final List<Workflow> payload = mock(List.class);
    when(client.workflows())
        .thenReturn(CompletableFuture.completedFuture(payload));
    CliMain.run(cliContext, "workflow", "ls");
    verify(client).workflows();
    verify(cliOutput).printWorkflows(payload);
  }

  @Test
  public void testWorkflowShow() {
    var payload = mock(WorkflowWithState.class);
    when(client.workflowWithState("foo", "bar"))
        .thenReturn(CompletableFuture.completedFuture(payload));
    CliMain.run(cliContext, "workflow", "show", "foo", "bar");
    verify(client).workflowWithState("foo", "bar");
    verify(cliOutput).printWorkflow(payload.workflow(), payload.state());
  }

  @Test
  public void testWorkflowCreate() throws Exception {
    final String component = "quux";

    final Path workflowsFile = fileFromResource("workflows.yaml");
    final List<WorkflowConfiguration> expected = Json.YAML_MAPPER
        .reader().forType(WorkflowConfiguration.class)
        .<WorkflowConfiguration>readValues(workflowsFile.toFile())
        .readAll();
    assertThat(expected, is(not(Matchers.empty())));

    when(client.createOrUpdateWorkflow(any(), any())).thenAnswer(a -> {
      final String comp = a.getArgument(0);
      final WorkflowConfiguration wfConfig = a.getArgument(1);
      return CompletableFuture.completedFuture(Workflow.create(comp, wfConfig));
    });

    CliMain.run(cliContext, "workflow", "create", component, "-f", workflowsFile.toString());

    for (WorkflowConfiguration workflowConfiguration : expected) {
      verify(client).createOrUpdateWorkflow(component, workflowConfiguration);
      verify(cliOutput).printMessage("Workflow " + workflowConfiguration.id() + " in component "
          + component + " created.");
    }
  }

  @Test
  public void testWorkflowCreateInvalidStructure() throws Exception {
    final Path workflowsFile = fileFromResource("wf-missing-schedule.yaml");
    try {
      CliMain.run(cliContext, "workflow", "create", "-f", workflowsFile.toString(), "foo");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.InputError));
    }
    verify(cliOutput).printError(
        "Workflow configuration doesn't conform to the expected structure, problem: "
            + "schedule\n at [Source: (File); line: 4, column: 1]\n"
            + "Minimal valid example: \n"
            + "========================\n"
            + "id: bar\n"
            + "schedule: DAYS\n"
            + "docker_image: busybox\n"
            + "docker_args: [echo, bar]\n"
            + "========================\n");
  }

  @Test
  public void testWorkflowCreateBadInput() throws Exception {
    final Path workflowsFile = fileFromResource("bad-content.yaml");
    try {
      CliMain.run(cliContext, "workflow", "create", "-f", workflowsFile.toString(), "foo");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.InputError));
    }
    verify(cliOutput).printError(contains(
        "Workflow configuration doesn't conform to the expected structure, "));
  }

  @Test
  public void testWorkflowCreateInvalid() throws Exception {
    final String component = "quux";

    final Path workflowsFile = fileFromResource("workflows.yaml");
    final List<WorkflowConfiguration> expected = Json.YAML_MAPPER
        .reader().forType(WorkflowConfiguration.class)
        .<WorkflowConfiguration>readValues(workflowsFile.toFile())
        .readAll();
    assertThat(expected, is(not(Matchers.empty())));

    for (WorkflowConfiguration configuration : expected) {
      var workflow = Workflow.create(component, configuration);
      when(validator.validateWorkflow(workflow)).thenReturn(
          List.of("bad-" + configuration.id(), "cfg-" + configuration.id()));
    }

    try {
      CliMain.run(cliContext, "workflow", "create", component, "-f", workflowsFile.toString());
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.ArgumentError));
    }

    for (WorkflowConfiguration configuration : expected) {
      verify(cliOutput).printError("Invalid workflow configuration: " + configuration.id());
      verify(cliOutput).printError("  error: bad-" + configuration.id());
      verify(cliOutput).printError("  error: cfg-" + configuration.id());
    }

    verify(client, never()).createOrUpdateWorkflow(any(), any());
  }

  @Test
  public void testWorkflowDelete() {
    final String component = "quux";

    when(client.deleteWorkflow(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    CliMain.run(cliContext, "workflow", "delete", component, "foo", "bar");

    verify(client).deleteWorkflow(component, "foo");
    verify(client).deleteWorkflow(component, "bar");

    verify(cliOutput).printMessage("Workflow foo in component " + component + " deleted.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " deleted.");
  }

  @Test
  public void testBackfillCreate() {
    final String component = "quux";
    final String start = "2017-01-01T00:00:00Z";
    final String end = "2017-01-30T00:00:00Z";

    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse(start))
        .end(Instant.parse(end))
        .workflowId(WorkflowId.create(component, "foo"))
        .concurrency(1)
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .created(currentTime)
        .lastModified(currentTime)
        .build();

    final BackfillInput expectedInput = BackfillInput.newBuilder()
        .component(backfill.workflowId().componentId())
        .workflow(backfill.workflowId().id())
        .start(backfill.start())
        .end(backfill.end())
        .reverse(backfill.reverse())
        .concurrency(backfill.concurrency())
        .triggerParameters(TriggerParameters.zero())
        .build();

    when(client.backfillCreate(expectedInput, false))
        .thenReturn(CompletableFuture.completedFuture(backfill));

    CliMain.run(cliContext, "backfill", "create", component, "foo", "2017-01-01", "2017-01-30",
        "1");

    verify(client).backfillCreate(expectedInput, false);
    verify(cliOutput).printBackfill(backfill, true);
  }

  @Test
  public void testBackfillCreateWithEnv() {
    final String component = "quux";
    final String start = "2017-01-01T00:00:00Z";
    final String end = "2017-01-30T00:00:00Z";

    final TriggerParameters expectedTriggerParameters = TriggerParameters.builder()
        .env("FOO", "foo",
            "BAR", "bar",
            "BAZ", "baz",
            "FOOBAR", "")
        .build();
    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse(start))
        .end(Instant.parse(end))
        .workflowId(WorkflowId.create(component, "foo"))
        .concurrency(1)
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .triggerParameters(expectedTriggerParameters)
        .created(currentTime)
        .lastModified(currentTime)
        .build();

    final BackfillInput expectedInput = BackfillInput.newBuilder()
        .component(backfill.workflowId().componentId())
        .workflow(backfill.workflowId().id())
        .start(backfill.start())
        .end(backfill.end())
        .reverse(backfill.reverse())
        .concurrency(backfill.concurrency())
        .triggerParameters(expectedTriggerParameters)
        .build();

    when(client.backfillCreate(expectedInput, false))
        .thenReturn(CompletableFuture.completedFuture(backfill));

    CliMain.run(cliContext, "backfill", "create", "-e", "FOO=foo", component, "foo", "2017-01-01", "2017-01-30",
        "1", "-e", "BAR=bar", "--env", "BAZ=baz", "-e", "FOOBAR=");

    verify(client).backfillCreate(expectedInput, false);
    verify(cliOutput).printBackfill(backfill, true);
  }

  @Test
  public void testBackfillCreateWithInvalidEnv() {
    final String component = "quux";

    try {
      CliMain.run(cliContext, "backfill", "create", "-e", "FOO=foo", component, "foo", "2017-01-01", "2017-01-30",
          "1", "-e", "BAR=bar", "--env", "BAZ=baz", "-e", "FOOBAR");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.UnknownError));
    }
  }

  @Test
  public void testBackfillCreateReverse() {
    final String component = "quux";
    final Instant start = Instant.parse("2017-01-01T00:00:00Z");
    final Instant end = Instant.parse("2017-01-30T00:00:00Z");

    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(start)
        .end(end)
        .reverse(true)
        .workflowId(WorkflowId.create(component, "foo"))
        .concurrency(1)
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .created(currentTime)
        .lastModified(currentTime)
        .build();

    final BackfillInput expectedInput = BackfillInput.newBuilder()
        .component(backfill.workflowId().componentId())
        .workflow(backfill.workflowId().id())
        .start(backfill.start())
        .end(backfill.end())
        .reverse(backfill.reverse())
        .concurrency(backfill.concurrency())
        .triggerParameters(TriggerParameters.zero())
        .build();

    when(client.backfillCreate(expectedInput, false))
        .thenReturn(CompletableFuture.completedFuture(backfill));

    CliMain.run(cliContext, "backfill", "create", component, "foo", "2017-01-01", "2017-01-30", "1", "--reverse");

    verify(client).backfillCreate(expectedInput, false);
    verify(cliOutput).printBackfill(backfill, true);
  }

  @Test
  public void testBackfillCreateWithDescription() {
    final String component = "quux";
    final String start = "2017-01-01T00:00:00Z";
    final String end = "2017-01-30T00:00:00Z";

    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse(start))
        .end(Instant.parse(end))
        .workflowId(WorkflowId.create(component, "foo"))
        .concurrency(1)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .created(currentTime)
        .lastModified(currentTime)
        .build();

    final BackfillInput expectedInput = BackfillInput.newBuilder()
        .component(backfill.workflowId().componentId())
        .workflow(backfill.workflowId().id())
        .start(backfill.start())
        .end(backfill.end())
        .concurrency(backfill.concurrency())
        .description("Description")
        .triggerParameters(TriggerParameters.zero())
        .build();

    when(client.backfillCreate(expectedInput, false))
        .thenReturn(CompletableFuture.completedFuture(backfill));

    CliMain.run(cliContext, "backfill", "create", component, "foo", "2017-01-01", "2017-01-30", "1",
        "-d", "Description");

    verify(client).backfillCreate(expectedInput, false);
    verify(cliOutput).printBackfill(backfill, true);
  }

  @Test
  public void testBackfillCreateAllowFuture() {
    final String component = "quux";
    final String start = "2017-01-01T00:00:00Z";
    final String end = "2017-01-30T00:00:00Z";

    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse(start))
        .end(Instant.parse(end))
        .workflowId(WorkflowId.create(component, "foo"))
        .concurrency(1)
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .created(currentTime)
        .lastModified(currentTime)
        .build();

    final BackfillInput expectedInput = BackfillInput.newBuilder()
        .component(backfill.workflowId().componentId())
        .workflow(backfill.workflowId().id())
        .start(backfill.start())
        .end(backfill.end())
        .reverse(backfill.reverse())
        .concurrency(backfill.concurrency())
        .triggerParameters(TriggerParameters.zero())
        .build();

    when(client.backfillCreate(expectedInput, true))
        .thenReturn(CompletableFuture.completedFuture(backfill));

    CliMain.run(cliContext, "backfill", "create", component, "foo", "2017-01-01", "2017-01-30",
        "1", "--allow-future");

    verify(client).backfillCreate(expectedInput, true);
    verify(cliOutput).printBackfill(backfill, true);
  }

  @Test
  public void testBackfillShow() {
    final String backfillId = "backfill-2";

    final Backfill backfill = Backfill.newBuilder()
        .id(backfillId)
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-30T00:00:00Z"))
        .workflowId(WorkflowId.create("quux", backfillId))
        .concurrency(1)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .created(currentTime)
        .lastModified(currentTime)
        .build();
    
    final BackfillPayload backfillPayload = BackfillPayload.create(backfill,
        Optional.empty());

    when(client.backfill(backfillId, true))
        .thenReturn(CompletableFuture.completedFuture(backfillPayload));

    CliMain.run(cliContext, "backfill", "show", backfillId, "--no-trunc");
    
    verify(client).backfill(backfillId, true);
    verify(cliOutput).printBackfillPayload(backfillPayload, true);
  }

  @Test
  public void testBackfillShowTruncating() {
    final String backfillId = "backfill-2";

    final Backfill backfill = Backfill.newBuilder()
        .id(backfillId)
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-30T00:00:00Z"))
        .workflowId(WorkflowId.create("quux", backfillId))
        .concurrency(1)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .created(currentTime)
        .lastModified(currentTime)
        .build();
    
    final BackfillPayload backfillPayload = BackfillPayload.create(backfill,
        Optional.empty());

    when(client.backfill(backfillId, true))
        .thenReturn(CompletableFuture.completedFuture(backfillPayload));

    CliMain.run(cliContext, "backfill", "show", backfillId);
    
    verify(client).backfill(backfillId, true);
    verify(cliOutput).printBackfillPayload(backfillPayload, false);
  }

  @Test
  public void testBackfillEdit() {
    final String backfillId = "backfill-2";

    final Backfill backfill = Backfill.newBuilder()
        .id(backfillId)
        .start(Instant.parse("2017-01-01T00:00:00Z"))
        .end(Instant.parse("2017-01-30T00:00:00Z"))
        .workflowId(WorkflowId.create("quux", backfillId))
        .concurrency(1)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .created(currentTime)
        .lastModified(currentTime)
        .build();

    when(client.backfillEditConcurrency(backfillId, 1))
        .thenReturn(CompletableFuture.completedFuture(backfill));

    CliMain.run(cliContext, "backfill", "edit", backfillId, "--concurrency", "1");

    verify(client).backfillEditConcurrency(backfillId, 1);
    verify(cliOutput).printBackfill(backfill, true);
  }

  @Test
  public void testBackfillList() {
    final String component = "quux";
    final String workflow = "foo";
    final String start = "2017-01-01T00:00:00Z";
    final String end = "2017-01-30T00:00:00Z";

    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse(start))
        .end(Instant.parse(end))
        .workflowId(WorkflowId.create(component, workflow))
        .concurrency(1)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .created(currentTime)
        .lastModified(currentTime)
        .build();
    
    final BackfillsPayload backfillsPayload = BackfillsPayload.create(
        List.of(BackfillPayload.create(backfill, Optional.empty())));

    when(client.backfillList(Optional.of(component), Optional.of(workflow), false, false))
        .thenReturn(CompletableFuture.completedFuture(backfillsPayload));

    CliMain.run(cliContext, "backfill", "list", "-c", component, "-w", workflow, "--no-trunc");

    verify(client).backfillList(Optional.of(component), Optional.of(workflow), false, false);
    verify(cliOutput).printBackfills(backfillsPayload.backfills(), true);
  }

  @Test
  public void testBackfillListWithoutCreatedTS() {
    final String component = "quux";
    final String workflow = "foo";
    final String start = "2017-01-01T00:00:00Z";
    final String end = "2017-01-30T00:00:00Z";

    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse(start))
        .end(Instant.parse(end))
        .workflowId(WorkflowId.create(component, workflow))
        .concurrency(1)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .build();

    final BackfillsPayload backfillsPayload = BackfillsPayload.create(
        List.of(BackfillPayload.create(backfill, Optional.empty())));

    when(client.backfillList(Optional.of(component), Optional.of(workflow), false, false))
        .thenReturn(CompletableFuture.completedFuture(backfillsPayload));

    CliMain.run(cliContext, "backfill", "list", "-c", component, "-w", workflow, "--no-trunc");

    verify(client).backfillList(Optional.of(component), Optional.of(workflow), false, false);
    verify(cliOutput).printBackfills(backfillsPayload.backfills(), true);
  }

  @Test
  public void testBackfillListTruncating() {
    final String component = "quux";
    final String workflow = "foo";
    final String start = "2017-01-01T00:00:00Z";
    final String end = "2017-01-30T00:00:00Z";

    final Backfill backfill = Backfill.newBuilder()
        .id("backfill-2")
        .start(Instant.parse(start))
        .end(Instant.parse(end))
        .workflowId(WorkflowId.create(component, workflow))
        .concurrency(1)
        .description("Description")
        .nextTrigger(Instant.parse("2017-01-01T00:00:00Z"))
        .schedule(Schedule.DAYS)
        .created(currentTime)
        .lastModified(currentTime)
        .build();
    
    final BackfillsPayload backfillsPayload = BackfillsPayload.create(
        List.of(BackfillPayload.create(backfill, Optional.empty())));

    when(client.backfillList(Optional.of(component), Optional.of(workflow), false, false))
        .thenReturn(CompletableFuture.completedFuture(backfillsPayload));

    CliMain.run(cliContext, "backfill", "list", "-c", component, "-w", workflow);

    verify(client).backfillList(Optional.of(component), Optional.of(workflow), false, false);
    verify(cliOutput).printBackfills(backfillsPayload.backfills(), false);
  }

  @Test
  public void testBackfillHalt() {
    var backfillId = "backfill-2";

    when(client.backfillHalt(backfillId, false))
        .thenReturn(CompletableFuture.completedFuture(null));

    CliMain.run(cliContext, "backfill", "halt", backfillId);

    verify(client).backfillHalt(backfillId, false);
  }

  @Test
  public void testBackfillHaltGracefully() {
    var backfillId = "backfill-2";

    when(client.backfillHalt(backfillId, true))
        .thenReturn(CompletableFuture.completedFuture(null));

    CliMain.run(cliContext, "backfill", "halt", backfillId, "--graceful");

    verify(client).backfillHalt(backfillId, true);
  }

  @Test
  @Parameters({
      "n",
      "N",
      "Y",
      "",
      "  ",
      "dfgdfgd",
  })
  public void testWorkflowDeleteInteractiveNo(String reply) {
    when(cliContext.hasConsole()).thenReturn(true);
    when(cliContext.consoleReadLine(any())).thenReturn(reply);

    try {
      CliMain.run(cliContext, "workflow", "delete", "quux", "foo", "bar");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.UnknownError));
    }

    verify(cliContext).consoleReadLine(
        "Sure you want to delete the workflows foo, bar in component quux? [y/N] ");
  }

  @Test
  @Parameters({
      "y",
      " y",
      "y ",
      " y ",
  })
  public void testWorkflowDeleteInteractiveYes(String reply) {
    final String component = "quux";

    when(cliContext.hasConsole()).thenReturn(true);
    when(cliContext.consoleReadLine(any())).thenReturn(reply);

    when(client.deleteWorkflow(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    CliMain.run(cliContext, "workflow", "delete", component, "foo", "bar");

    verify(cliContext).consoleReadLine(
        "Sure you want to delete the workflows foo, bar in component quux? [y/N] ");

    verify(client).deleteWorkflow(component, "foo");
    verify(client).deleteWorkflow(component, "bar");

    verify(cliOutput).printMessage("Workflow foo in component " + component + " deleted.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " deleted.");
  }

  @Test
  public void testWorkflowDeleteInteractiveForce() {
    final String component = "quux";

    when(cliContext.hasConsole()).thenReturn(true);

    when(client.deleteWorkflow(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    CliMain.run(cliContext, "workflow", "delete", component, "foo", "bar", "--force");

    verify(cliContext, never()).consoleReadLine(any());

    verify(client).deleteWorkflow(component, "foo");
    verify(client).deleteWorkflow(component, "bar");

    verify(cliOutput).printMessage("Workflow foo in component " + component + " deleted.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " deleted.");
  }

  @Test
  public void testWorkflowEnable() {
    final String component = "quux";
    final WorkflowState workflowState = WorkflowState.builder()
        .enabled(true)
        .build();

    when(client.updateWorkflowState(any(), any(), eq(workflowState)))
        .thenReturn(CompletableFuture.completedFuture(workflowState));

    CliMain.run(cliContext, "workflow", "enable", component, "foo", "bar");

    verify(client).updateWorkflowState(component, "foo", workflowState);
    verify(client).updateWorkflowState(component, "bar", workflowState);
    verify(cliOutput).printMessage("Workflow foo in component " + component + " enabled.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " enabled.");
  }

  @Test
  public void testWorkflowDisable() {
    final String component = "quux";
    final WorkflowState workflowState = WorkflowState.builder()
        .enabled(false)
        .build();

    when(client.updateWorkflowState(any(), any(), eq(workflowState)))
        .thenReturn(CompletableFuture.completedFuture(workflowState));

    CliMain.run(cliContext, "workflow", "disable", component, "foo", "bar");

    verify(client).updateWorkflowState(component, "foo", workflowState);
    verify(client).updateWorkflowState(component, "bar", workflowState);
    verify(cliOutput).printMessage("Workflow foo in component " + component + " disabled.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " disabled.");
  }

  @Test
  public void shouldHandleWorkflowNotFoundWhenEnabling() {
    final String component = "quux";
    final WorkflowState workflowState = WorkflowState.builder()
        .enabled(true)
        .build();

    final ApiErrorException exception = new ApiErrorException("not found", 404, true, requestId);
    when(client.updateWorkflowState(any(), any(), eq(workflowState)))
        .thenReturn(exceptionallyCompletedFuture(exception));

    CliMain.run(cliContext, "workflow", "enable", component, "foo", "bar");
    verify(cliOutput).printMessage("Workflow foo in component " + component + " not found.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " not found.");
  }

  @Test
  public void shouldHandleWorkflowNotFoundWhenDisabling() {
    final String component = "quux";
    final WorkflowState workflowState = WorkflowState.builder()
        .enabled(false)
        .build();

    final ApiErrorException exception = new ApiErrorException("not found", 404, true, requestId);
    when(client.updateWorkflowState(any(), any(), eq(workflowState)))
        .thenReturn(exceptionallyCompletedFuture(exception));

    CliMain.run(cliContext, "workflow", "disable", component, "foo", "bar");
    verify(cliOutput).printMessage("Workflow foo in component " + component + " not found.");
    verify(cliOutput).printMessage("Workflow bar in component " + component + " not found.");
  }

  @Test
  public void testClientError() {
    when(client.triggerWorkflowInstance(any(), any(), any(), any(), eq(false)))
        .thenReturn(exceptionallyCompletedFuture(
            new ClientErrorException("foo failure",
                new IOException("bar failure",
                    new ConnectException("failed to connect to baz")))));

    try {
      CliMain.run(cliContext, "t", "foo", "bar", "2017-01-02");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.ClientError));
    }

    verify(cliOutput).printError("Client error: foo failure: ConnectException: failed to connect to baz");
  }

  @Test
  public void testClientErrorDebug() {
    final Throwable cause = new ClientErrorException("foo failure",
        new IOException("bar failure",
            new ConnectException("failed to connect to baz")));
    when(client.triggerWorkflowInstance(any(), any(), any(), any(), eq(false)))
        .thenReturn(exceptionallyCompletedFuture(cause));

    try {
      CliMain.run(cliContext, "--debug", "t", "foo", "bar", "2017-01-02");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.ClientError));
    }

    verify(cliOutput).printError(Throwables.getStackTraceAsString(cause));
    verify(cliOutput).printError("Client error: foo failure: ConnectException: failed to connect to baz");
  }

  @Test
  public void testApiError() {
    final ApiErrorException exception = new ApiErrorException("bar failure", 500, true, requestId);
    when(client.triggerWorkflowInstance(any(), any(), any(), any(), eq(false)))
        .thenReturn(exceptionallyCompletedFuture(exception));

    try {
      CliMain.run(cliContext, "t", "foo", "bar", "2017-01-02");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.ApiError));
    }

    verify(cliOutput).printError("API error: " + exception.getMessage());
  }

  @Test
  public void testClientUnknownError() {
    final Exception exception = new UnsupportedOperationException();
    when(client.triggerWorkflowInstance(any(), any(), any(), any(), eq(false)))
        .thenReturn(exceptionallyCompletedFuture(exception));

    try {
      CliMain.run(cliContext, "t", "foo", "bar", "2017-01-02");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.ClientError));
    }

    verify(cliOutput).printError(Throwables.getStackTraceAsString(exception));
  }


  @Test
  public void testUnknownError() {
    final Exception exception = new UnsupportedOperationException();
    when(client.triggerWorkflowInstance(any(), any(), any(), any(), eq(false)))
        .thenThrow(exception);

    try {
      CliMain.run(cliContext, "t", "foo", "bar", "2017-01-02");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.UnknownError));
    }

    verify(cliOutput).printError(Throwables.getStackTraceAsString(exception));
  }

  @Test
  public void testMissingCredentialsHelpMessage() {
    final ApiErrorException apiError = new ApiErrorException("foo", 401, false, requestId);
    when(client.triggerWorkflowInstance(any(), any(), any(), any(), eq(false)))
        .thenReturn(exceptionallyCompletedFuture(apiError));

    try {
      CliMain.run(cliContext, "t", "foo", "bar", "2017-01-02");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.AuthError));
    }

    verify(cliOutput).printError(contains("gcloud auth application-default login"));
    verify(cliOutput).printError(contains(apiError.getMessage()));
  }

  @Test
  public void testUnauthorizedMessage() {
    final ApiErrorException apiError = new ApiErrorException("foo", 401, true, requestId);
    when(client.triggerWorkflowInstance(any(), any(), any(), any(), eq(false)))
        .thenReturn(exceptionallyCompletedFuture(apiError));

    try {
      CliMain.run(cliContext, "t", "foo", "bar", "2017-01-02");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.AuthError));
    }

    verify(cliOutput).printError("API error: Unauthorized: " + apiError.getMessage());
  }

  @Test
  public void testHelp() {
    try {
      CliMain.run(cliContext, "--help");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.Success));
    }

    try {
      CliMain.run(cliContext);
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.Success));
    }

    verifyZeroInteractions(client);
  }

  @Test
  public void testArgumentError() {
    try {
      CliMain.run(cliContext, "foozbarz");
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.ArgumentError));
    }

    verifyZeroInteractions(client);
  }

  @Test
  public void testTrigger() {
    final TriggerParameters expectedParameters = TriggerParameters.zero();
    when(client.triggerWorkflowInstance("foo", "bar", "2017-01-02", expectedParameters, false))
        .thenReturn(CompletableFuture.completedFuture(null));

    CliMain.run(cliContext, "t", "foo", "bar", "2017-01-02");

    verify(client).triggerWorkflowInstance("foo", "bar", "2017-01-02", expectedParameters, false);
  }

  @Test
  public void testTriggerWithEnv() {
    final TriggerParameters expectedParameters = TriggerParameters.builder()
        .env("FOO", "foo",
            "BAR", "bar",
            "BAZ", "baz")
        .build();
    when(client.triggerWorkflowInstance("foo", "bar", "2017-01-02", expectedParameters, false))
        .thenReturn(CompletableFuture.completedFuture(null));

    CliMain.run(cliContext, "t", "-e", "FOO=foo", "foo", "bar", "2017-01-02", "-e", "BAR=bar", "--env", "BAZ=baz");

    verify(client).triggerWorkflowInstance("foo", "bar", "2017-01-02", expectedParameters, false);
  }

  @Test
  public void testTriggerAllowFuture() {
    final TriggerParameters expectedParameters = TriggerParameters.zero();
    when(client.triggerWorkflowInstance("foo", "bar", "2017-01-02", expectedParameters, true))
        .thenReturn(CompletableFuture.completedFuture(null));

    CliMain.run(cliContext, "t", "foo", "bar", "2017-01-02", "--allow-future");

    verify(client).triggerWorkflowInstance("foo", "bar", "2017-01-02", expectedParameters, true);
  }

  @Test
  @Parameters({
      "--json workflow ls",
      "workflow --json ls",
      "workflow ls --json",
  })
  public void testJsonOptionIsGlobal(final String argLine) {
    when(client.workflows()).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
    CliMain.run(cliContext, argLine.split(" "));
    verify(cliContext).output(Output.JSON);
  }

  @Test
  @Parameters({
      "--plain workflow ls",
      "workflow --plain ls",
      "workflow ls --plain",
  })
  public void testPlainOptionIsGlobal(final String argLine) {
    when(client.workflows()).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
    CliMain.run(cliContext, argLine.split(" "));
    verify(cliContext).output(Output.PLAIN);
  }

  @Test
  @Parameters({
      "--debug workflow ls",
      "workflow --debug ls",
      "workflow ls --debug",
  })
  public void testDebugOptionIsGlobal(final String argLine) {
    when(client.workflows()).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
    assertThat(Try.run(() -> CliMain.run(cliContext, argLine.split(" "))).isSuccess(), is(true));
  }

  @Test
  @Parameters({
      "--host https://foo.bar workflow ls",
      "workflow --host https://foo.bar ls",
      "workflow ls --host https://foo.bar",
  })
  public void testHostOptionIsGlobal(final String argLine) {
    when(client.workflows()).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
    CliMain.run(cliContext, argLine.split(" "));
    verify(cliContext).createClient("https://foo.bar");
  }

  @Test
  public void testAuthTestServiceAccountUsageSuccess() {
    final String serviceAccount = "foo@bar.iam.gserviceaccount.com";
    final String principal = "baz@example.com";

    final TestServiceAccountUsageAuthorizationResponse response = TestServiceAccountUsageAuthorizationResponse.builder()
        .authorized(true)
        .serviceAccount(serviceAccount)
        .principal(principal)
        .message("foobar")
        .build();

    when(client.testServiceAccountUsageAuthorization(serviceAccount, principal))
        .thenReturn(CompletableFuture.completedFuture(response));

    CliMain.run(cliContext, "auth", "test", "--service-account", serviceAccount, "--principal", principal);

    verify(client).testServiceAccountUsageAuthorization(serviceAccount, principal);
    verify(cliOutput).printMessage("The principal " + principal + " is authorized to use the service account "
                                   + serviceAccount + ". " + response.message().get());
  }

  @Test
  public void testAuthTestServiceAccountUsageFailure() {
    final String serviceAccount = "foo@bar.iam.gserviceaccount.com";
    final String principal = "baz@example.com";

    final TestServiceAccountUsageAuthorizationResponse response = TestServiceAccountUsageAuthorizationResponse.builder()
        .authorized(false)
        .serviceAccount(serviceAccount)
        .principal(principal)
        .message("foobar")
        .build();

    when(client.testServiceAccountUsageAuthorization(serviceAccount, principal))
        .thenReturn(CompletableFuture.completedFuture(response));

    try {
      CliMain.run(cliContext, "auth", "test", "--service-account", serviceAccount, "--principal", principal);
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.UnknownError));
    }

    verify(client).testServiceAccountUsageAuthorization(serviceAccount, principal);
    verify(cliOutput).printMessage("The principal " + principal + " is not authorized to use the service account "
                                   + serviceAccount + ". " + response.message().orElse(""));
  }

  @Test
  @Parameters({
      "auth test --service-account foo@bar.iam.gserviceaccount.com",
      "auth test --principal baz@example.com",
      "auth test"
  })
  public void testAuthTestServiceAccountUsageMissingRequiredArgument(final String argLine) {
    when(client.workflows()).thenReturn(CompletableFuture.completedFuture(Collections.emptyList()));
    try {
      CliMain.run(cliContext, argLine.split(" "));
      fail();
    } catch (CliExitException e) {
      assertThat(e.status(), is(ExitStatus.ArgumentError));
    }
  }

  private Path fileFromResource(String name) throws IOException {
    final File workflowsFile = temporaryFolder.newFile();
    try (OutputStream os = Files.newOutputStream(workflowsFile.toPath())) {
      Resources.copy(Resources.getResource(this.getClass(), name), os);
    }
    return workflowsFile.toPath();
  }
}
